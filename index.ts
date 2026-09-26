/**
 * Pi Agent Team — 让多台机器上的 Pi 互相通信,像一个集群。
 *
 * 传输:跑在私有网络(tailnet)上的一个 broker,所有节点主动 dial out。
 * 注入:pi.sendUserMessage() —— 对端消息以"用户输入"形式进入本会话,
 *       模型真的会处理它,和真人手打没区别。TUI 模式下同样生效。
 *
 * 用法:
 *   TEAM_URL=ws://<tailscale-ip>:8787 \
 *   TEAM_NAME=laptop \
 *   TEAM_TAGS=web,frontend \
 *   TEAM_TOKEN=xxx \
 *     pi --extension /path/to/pi-agent-team/index.ts
 *
 * ── 身份是"节点名",不是 IP ──
 *   同一台机器可以跑多个 Pi,它们各自是独立节点。host 只用于分组显示,
 *   不参与路由。所以 tailscale 的成员表(只到机器这一层)当不了成员表。
 *
 * ── 两个 API 分属不同对象(踩过的坑)──
 *   pi.sendUserMessage() / pi.appendEntry()  在 ExtensionAPI 上
 *   ctx.isIdle() / ctx.ui.select()           在 ExtensionContext 上
 * 写成 ctx.sendUserMessage() 会抛 "is not a function"。
 *
 * ── 可视化 ──
 *   用 appendEntry 在聊天流插卡片(📥/📤/🔁/⚠️)。
 *   不能用 sendMessage({display:true}) —— 那个会进 LLM 上下文,
 *   会让模型把自己发出的卡片当成收到的消息。详见 showCard 注释。
 *
 * ── 生命周期契约 ──
 *   不在 factory 里开 socket:有些调用会加载扩展但不启动会话。
 *   socket 从 session_start 起,由幂等的 session_shutdown 关。
 */

import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Box, Text } from "@earendil-works/pi-tui";
import { hostname } from "node:os";
import { Type } from "typebox";
import { MAX_HOPS, buildPayload, classifyInbound, excerpt, rememberBounded } from "./src/policy.js";

// ---------------------------------------------------------------- 类型

/** broker 推来的成员元数据 */
type Member = {
  name: string;
  host: string | null;
  addr: string | null;
  tags: string[];
  since: number;
};

type Envelope = {
  from: string;
  to: string | string[];
  id: string;
  re: string | null;
  body: Record<string, unknown>;
};

type Status = "idle" | "connecting" | "online" | "offline";

/**
 * 自动推送模式。
 *
 *   off    只在 /team say 或 team_send 时发送
 *   auto   只在"本轮由 team 消息触发"时回推给发信方(默认)
 *   always 每轮结束都推给所有在线节点
 */
type AnnounceMode = "off" | "auto" | "always";

type CardKind = "receive" | "send" | "reply" | "failed";

type CardDetails = {
  kind: CardKind;
  /** 收件人/发件人展示名 */
  peer: string;
  text: string;
  at: number;
  reason?: string;
};

const CARD_TYPE = "team-message";

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30_000;
const DEDUPE_CAP = 1000;

/** broker 用这个关闭码表示"你被同名的新连接顶掉了"。收到它不要重连。 */
const CLOSE_REPLACED = 4001;

/** /team say 一次最多发给多少人,防止手滑把 20 个节点全叫醒。 */
const BULK_WARN_THRESHOLD = 5;

// ---------------------------------------------------------------- 状态

let socket: WebSocket | null = null;
let status: Status = "offline";
let self = "";
let brokerUrl = "";
let token = "";
let selfTags: string[] = [];

/** 当前在线的其他成员(含元数据) */
let members: Member[] = [];

let reconnectDelay = RECONNECT_BASE_MS;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
let shuttingDown = false;

let announceMode: AnnounceMode = "auto";

/**
 * 本轮该不该自动回传、回给谁。
 *
 * 由 classifyInbound 决定(在 src/policy.js,有单测):
 *   - 纯粹的"新请求" → 回传
 *   - 对方对我们的回复  → 【不】回传,否则会请求→回复→回复…
 *     一路打转到跳数上限。真实故障,已用回归测试钉住。
 */
let pendingReply: { to: string; hops: number; re: string } | null = null;

/**
 * 我们自己发出过的消息,按 id 索引。
 *
 * 用途:收到一条 re 指向某 id 的回复时,判断那条原消息是
 * 模型发出的(那模型知道上下文)还是用户 /team send 发出的
 * (那模型完全不知道,不该被叫醒去疑惑"正文是空的")。
 */
const outbound = new Map<string, { text: string; origin: "user" | "model"; to: string | string[] }>();

const seen = new Set<string>();
const injected = new Set<string>();

let lastAssistantText = "";

let ctxRef: ExtensionContext | null = null;
let apiRef: ExtensionAPI | null = null;

const newId = () => `m-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

function remember(set: Set<string>, value: string) {
  set.add(value);
  if (set.size > DEDUPE_CAP) {
    const oldest = set.values().next().value;
    if (oldest !== undefined) set.delete(oldest);
  }
}

/** 其他成员(排除自己) */
const others = () => members.filter((m) => m.name !== self);

/** 所有已知 tag(来自他人),用于补全和分组显示 */
function knownTags(): string[] {
  const s = new Set<string>();
  for (const m of others()) for (const t of m.tags) s.add(t);
  return [...s].sort();
}

function render() {
  const ui = ctxRef?.ui;
  if (!ui) return;
  const size = members.length > 0 ? members.length : 1;
  const icon = status === "online" ? "🟢" : status === "connecting" ? "🟡" : "🔴";
  ui.setStatus("team", `${icon} team:${self} (${size})`);
}

// ---------------------------------------------------------------- 卡片

/**
 * 在聊天流里插一张 team 卡片。
 *
 * 用 appendEntry 而不是 sendMessage —— 这是实测得出的结论:
 * sendMessage({display:true}) 虽然看起来是纯展示 API,但它
 * 【会进 LLM 上下文】(用 context 事件验证过,消息序列里出现
 * "CUSTOM:team-message")。后果是模型看到自己发出的卡片,会误认为
 * 是收到的消息 —— 实测 Opus 5.5 明确说"看起来是我发出去的消息
 * 又被回送到了我这边",然后拒绝继续协作。
 *
 * appendEntry 的文档注释是 "not sent to LLM",配 registerEntryRenderer
 * 渲染。纯展示,零上下文污染。
 */
function showCard(details: Omit<CardDetails, "at">) {
  apiRef?.appendEntry<CardDetails>(CARD_TYPE, { ...details, at: Date.now() });
}

// ---------------------------------------------------------------- 注入

/**
 * 把对端消息作为"用户输入"送进本会话。
 *
 * 措辞由 policy.buildPayload 构造 —— 请求和回复的说法必须不同,
 * 否则模型无法判断这一轮输出会不会回传。实测教训:
 *   1) 只说"这是另一个 Pi 节点的消息" → 模型回 "Ready for the task"
 *      / "请告诉我需要处理什么任务",完全不动手。
 *   2) 改成"这是一项已下达的任务,立即执行" → 治好了被动,但把
 *      多轮对话压成了任务下发。
 *   3) 现在按 classifyInbound 的结果分两种措辞。
 */
function injectAsUserMessage(
  from: string,
  payload: string,
  reply: { hops: number; re: string } | null,
): boolean {
  const ctx = ctxRef;
  if (!ctx) return false;

  // reply 为 null 表示"这是一条回复,不要自动回传"。
  // 注意这里不能在 reply 为 null 时清空 pendingReply:模型可能正在处理
  // 一条请求,这时插进来的回复(followUp 排队)不该取消那条请求的回传。
  if (reply) pendingReply = { to: from, hops: reply.hops, re: reply.re };

  try {
    if (ctx.isIdle()) {
      apiRef?.sendUserMessage(payload);
    } else {
      apiRef?.sendUserMessage(payload, { deliverAs: "followUp" });
    }
    return true;
  } catch (err) {
    ctx.ui.notify(`team:注入失败 ${(err as Error).message}`, "error");
    return false;
  }
}

// ---------------------------------------------------------------- 连接

function scheduleReconnect() {
  if (shuttingDown || reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, reconnectDelay);
  reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_MS);
}

function connect() {
  if (shuttingDown) return;
  if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
    return;
  }

  status = "connecting";
  render();

  const qs = new URLSearchParams({ name: self, token });
  if (selfTags.length) qs.set("tags", selfTags.join(","));
  // host 只用于分组显示。取不到就算了,不阻塞连接。
  try {
    const h = hostname();
    if (h) qs.set("host", h);
  } catch {}

  let ws: WebSocket;
  try {
    ws = new WebSocket(`${brokerUrl}?${qs}`);
  } catch (err) {
    ctxRef?.ui.notify(`team 连接失败:${(err as Error).message}`, "error");
    status = "offline";
    render();
    scheduleReconnect();
    return;
  }
  socket = ws;

  ws.addEventListener("open", () => {
    reconnectDelay = RECONNECT_BASE_MS;
    status = "online";
    render();
  });

  ws.addEventListener("message", (ev) => {
    let env: Envelope;
    try {
      env = JSON.parse(String(ev.data)) as Envelope;
    } catch {
      return;
    }
    handleEnvelope(env);
  });

  ws.addEventListener("close", (ev) => {
    socket = null;

    if (ev.code === CLOSE_REPLACED) {
      // 另一个同名节点接管了。继续重连只会和它互相顶来顶去,
      // 两边都连不上。停下来并说清楚原因。
      status = "offline";
      members = [];
      render();
      shuttingDown = true;
      ctxRef?.ui.notify(
        `team:节点名 "${self}" 已被另一个实例接管,本实例停止重连。` +
          `换一个 TEAM_NAME,或关掉那个实例。`,
        "error",
      );
      return;
    }

    status = "offline";
    members = [];
    render();
    scheduleReconnect();
  });

  ws.addEventListener("error", () => {});
}

/**
 * 发送,并记下"这条是谁发起的"。
 *
 * 记录 origin 是为了处理对方回复时能分辨:模型发起的消息它有上下文,
 * 用户的 /team send 它完全不知道 —— 后者不该叫醒模型。
 *
 * @param to 收件人:节点名、"*"、或 ["a","b","#tag"] 混合
 * @param origin "user"(用户手动)或 "model"(模型调用 team_send)
 */
function send(
  to: string | string[],
  body: Record<string, unknown>,
  origin: "user" | "model",
  re: string | null = null,
): boolean {
  if (!socket || socket.readyState !== WebSocket.OPEN) return false;
  const id = newId();
  socket.send(JSON.stringify({ from: self, id, re, to, body }));
  rememberBounded(outbound, id, {
    text: typeof body.text === "string" ? body.text : "",
    origin,
    to,
  });
  return true;
}

// ---------------------------------------------------------------- 收

function handleEnvelope(env: Envelope) {
  const body = env.body ?? {};
  const kind = typeof body.kind === "string" ? body.kind : undefined;

  // ---- broker 系统消息
  if (env.from === "broker") {
    // 兼容:老 broker 只给 peers(名字数组),新的给 members。
    const list: Member[] = Array.isArray(body.members)
      ? (body.members as Member[])
      : (Array.isArray(body.peers) ? (body.peers as string[]).map((n) => ({ name: n, host: null, addr: null, tags: [], since: 0 })) : []);

    const mine = list.filter((m) => m.name !== self);

    switch (kind) {
      case "welcome":
      case "peer_joined":
      case "peer_left":
        members = mine;
        render();
        if (kind === "peer_joined" && typeof body.peer === "string" && body.peer !== self) {
          const joined = mine.find((m) => m.name === body.peer);
          const where = joined?.host ? ` (${joined.host})` : "";
          ctxRef?.ui.notify(`team:${body.peer} 上线${where}`, "info");
        }
        break;

      case "undeliverable": {
        const unknown = Array.isArray(body.unknown) ? (body.unknown as string[]) : [];
        const to = String(body.to ?? "?");
        let why: string;
        if (unknown.length && unknown.every((u) => u.startsWith("#"))) {
          why = `分组 ${unknown.join(",")} 里没有在线节点`;
        } else if (unknown.length) {
          why = `找不到 ${unknown.join(",")}(不在线或名字写错)`;
        } else {
          why = "没有可投递的对象";
        }
        ctxRef?.ui.notify(`team:发给 ${to} 失败 —— ${why}`, "warning");
        break;
      }

      case "delivered": {
        const failed = Array.isArray(body.failed) ? (body.failed as string[]) : [];
        const unknown = Array.isArray(body.unknown) ? (body.unknown as string[]) : [];
        // 部分成功也要说,否则用户以为"发出去了"就全到了
        if (failed.length || unknown.length) {
          const parts: string[] = [`投递 ${body.delivered}/${body.total}`];
          if (failed.length) parts.push(`写入失败:${failed.join(",")}`);
          if (unknown.length) parts.push(`未知:${unknown.join(",")}`);
          ctxRef?.ui.notify(`team:${parts.join(" · ")}`, "warning");
        }
        break;
      }

      case "ping":
        break;
    }
    return;
  }

  // ---- 团队消息
  if (seen.has(env.id)) return;
  remember(seen, env.id);

  if (injected.has(env.id)) return;
  remember(injected, env.id);

  // 对话形状的判断集中在 policy 模块(有单测):
  // 新请求 → 注入 + 自动回传;对方对我们的回复 → 注入但不回传;
  // 回复用户手动发出的消息 → 只显示卡片;fyi 广播 → 只显示卡片。
  const cls = classifyInbound(env, outbound);

  if (cls.action === "drop") return;

  const text = typeof body.text === "string" ? body.text : "";

  if (cls.action === "card") {
    showCard({
      kind: cls.kind === "fyi" ? "send" : "receive",
      peer: env.from,
      text,
      reason: cls.original ? `回复:${excerpt(cls.original, 60)}` : undefined,
    });
    return;
  }

  showCard({ kind: "receive", peer: env.from, text });
  injectAsUserMessage(
    env.from,
    buildPayload(env.from, text, cls),
    cls.autoReply ? { hops: typeof body.hops === "number" ? body.hops : 0, re: env.id } : null,
  );
}

// ---------------------------------------------------------------- 出站

/**
 * 轮次真正结束后决定要不要推、推给谁。
 *
 * 用 agent_settled 而不是 agent_end:后者之后还可能有重试、
 * 溢出恢复、compaction、queued continuation,拿它当"结束"会推中间态。
 */
function maybeAnnounce() {
  const text = lastAssistantText;
  if (!text.trim()) return;

  if (announceMode === "always") {
    if (status !== "online" || others().length === 0) return;
    lastAssistantText = "";
    for (const m of others()) {
      // fyi:true 让收件人只显示卡片,不叫醒它的模型。
      // 否则 N 个节点都开 always 时,每轮都会触发 N-1 轮新的思考。
      if (send(m.name, { text, hops: 1, relayedFrom: self, fyi: true }, "model")) {
        showCard({ kind: "send", peer: m.name, text });
      }
    }
    return;
  }

  if (announceMode === "auto") {
    const reply = pendingReply;
    pendingReply = null;
    if (!reply) return;

    // 对方可能在回信之前就下线了。这一轮仍然值得留住文本,
    // 但不能假装发送成功。
    lastAssistantText = "";
    const hops = reply.hops + 1;

    if (!others().some((m) => m.name === reply.to)) {
      showCard({
        kind: "failed",
        peer: reply.to,
        text,
        reason: `${reply.to} 已离线,回复没有送出`,
      });
      return;
    }

    if (send(reply.to, { text, hops, relayedFrom: self }, "model", reply.re)) {
      if (hops >= MAX_HOPS) {
        showCard({
          kind: "send",
          peer: reply.to,
          text,
          reason: `已达跳数上限 ${MAX_HOPS},对端不会再回传`,
        });
      } else {
        showCard({ kind: "reply", peer: reply.to, text });
      }
    }
    return;
  }

  pendingReply = null;
}

// ---------------------------------------------------------------- 展示辅助

/** 按机器分组,同一台机器上的多个 agent 归到一起 */
function groupByHost(list: Member[]): Map<string, Member[]> {
  const g = new Map<string, Member[]>();
  for (const m of list) {
    const key = m.host ?? "(未知主机)";
    if (!g.has(key)) g.set(key, []);
    g.get(key)!.push(m);
  }
  for (const arr of g.values()) arr.sort((a, b) => a.name.localeCompare(b.name));
  return g;
}

function describeMember(m: Member): string {
  const tags = m.tags.length ? `  [${m.tags.join(" ")}]` : "";
  return `${m.name}${tags}`;
}

/** 人类可读的成员树:按机器分组 */
function rosterLines(): string[] {
  const list = others();
  if (!list.length) return [`只有你自己在线(${self})`];

  const lines: string[] = [];
  for (const [host, arr] of [...groupByHost(list)].sort((a, b) => a[0].localeCompare(b[0]))) {
    lines.push(`${host}  (${arr.length})`);
    for (const m of arr) lines.push(`  ${describeMember(m)}`);
  }
  const tags = knownTags();
  if (tags.length) lines.push("", `可用分组:${tags.map((t) => `#${t}`).join(" ")}`);
  return lines;
}

/**
 * 把用户输入解析成收件人。
 *
 *   "laptop"           → "laptop"
 *   "all" / "*"        → "*"
 *   "#web"             → "#web"
 *   "a,b,#web"         → ["a","b","#web"]
 */
function parseRecipients(raw: string): string | string[] {
  if (raw === "all" || raw === "*") return "*";
  const parts = raw.split(",").map((s) => s.trim()).filter(Boolean);
  if (parts.length === 0) return raw;
  if (parts.length === 1) return parts[0];
  return parts;
}

// ---------------------------------------------------------------- 导出

export default function (pi: ExtensionAPI) {
  apiRef = pi;

  // ---- 卡片渲染器
  pi.registerEntryRenderer<CardDetails>(CARD_TYPE, (entry, { expanded }, theme) => {
    const d = entry.data;
    const kind: CardKind = d?.kind ?? "receive";
    const peer = d?.peer ?? "?";
    const body = d?.text ?? "";

    const meta: Record<CardKind, { icon: string; label: string; color: string; arrow: string }> = {
      receive: { icon: "📥", label: "RECV", color: "accent", arrow: "←" },
      send: { icon: "📤", label: "SEND", color: "success", arrow: "→" },
      reply: { icon: "🔁", label: "REPLY", color: "success", arrow: "→" },
      failed: { icon: "⚠️", label: "FAIL", color: "error", arrow: "→" },
    };
    const m = meta[kind] ?? meta.receive;

    const head =
      theme.fg(m.color, `${m.icon} ${m.label}`) +
      " " +
      theme.fg("dim", `${m.arrow} ${peer}`) +
      (d?.at ? theme.fg("dim", `  ${new Date(d.at).toLocaleTimeString()}`) : "");

    const lines = body.split("\n");
    const shown = expanded ? lines : lines.slice(0, 6);
    let text = head + "\n" + shown.map((l) => `  ${l}`).join("\n");
    if (!expanded && lines.length > 6) {
      text += "\n" + theme.fg("dim", `  …还有 ${lines.length - 6} 行(展开查看)`);
    }
    if (d?.reason) text += "\n" + theme.fg(m.color, `  ${d.reason}`);

    const box = new Box(0, 1, (t) => theme.bg("customMessageBg", t));
    box.addChild(new Text(text, 0, 0));
    return box;
  });

  // ---- CLI flags
  pi.registerFlag("team-url", { description: "pi-agent-team broker URL", type: "string" });
  pi.registerFlag("team-name", { description: "本节点在团队里的名字", type: "string" });
  pi.registerFlag("team-tags", { description: "本节点的分组标签,逗号分隔", type: "string" });
  pi.registerFlag("team-announce", { description: "自动推送模式:off | auto | always", type: "string" });
  pi.registerFlag("team-quiet", { description: "静默:不把自己的产出推送给任何人", type: "boolean" });

  // ---- 生命周期
  pi.on("session_start", async (_event, ctx) => {
    ctxRef = ctx;

    brokerUrl = (pi.getFlag("team-url") as string) ?? process.env.TEAM_URL ?? "";
    token = process.env.TEAM_TOKEN ?? "";
    self =
      (pi.getFlag("team-name") as string) ??
      process.env.TEAM_NAME ??
      process.cwd().split("/").filter(Boolean).pop() ??
      "pi";

    const tagStr = (pi.getFlag("team-tags") as string) ?? process.env.TEAM_TAGS ?? "";
    selfTags = tagStr.split(",").map((s) => s.trim()).filter(Boolean);

    if (pi.getFlag("team-quiet") === true) {
      announceMode = "off";
    } else {
      const flag = (pi.getFlag("team-announce") as string) ?? process.env.TEAM_ANNOUNCE;
      if (flag === "off" || flag === "auto" || flag === "always") announceMode = flag;
      else if (flag === "1" || flag === "true") announceMode = "always";
    }

    if (!brokerUrl) {
      ctx.ui.notify("team:未配置 TEAM_URL —— 扩展休眠", "warning");
      return;
    }
    if (!token) {
      ctx.ui.notify("team:未配置 TEAM_TOKEN —— 拒绝连接", "error");
      return;
    }

    connect();
  });

  pi.on("session_shutdown", async () => {
    shuttingDown = true;
    if (reconnectTimer) clearTimeout(reconnectTimer);
    reconnectTimer = null;
    try {
      socket?.close();
    } catch {}
    socket = null;
    ctxRef = null;
  });

  pi.on("turn_start", async (_event, ctx) => {
    ctxRef = ctx;
  });

  // ---- 累积 assistant 文本
  //
  // 注意:agent_settled 的事件对象只有 { type },没有 messages。
  // 所以文本必须在这里攒,不能到 settled 里从 event 上读。
  pi.on("message_end", async (event) => {
    const m = event.message as { role?: string; content?: unknown };
    if (m?.role !== "assistant") return;

    if (typeof m.content === "string") {
      lastAssistantText = m.content;
    } else if (Array.isArray(m.content)) {
      lastAssistantText = m.content
        .filter((c: { type?: string }) => c?.type === "text")
        .map((c: { text?: string }) => c.text ?? "")
        .join("");
    }
  });

  pi.on("agent_settled", async (_event, ctx) => {
    ctxRef = ctx;
    maybeAnnounce();
  });

  // ---- 把团队写进系统提示
  pi.on("before_agent_start", async (event) => {
    const list = others();
    const tags = knownTags();

    const rosterText = list.length
      ? [...groupByHost(list)]
          .sort((a, b) => a[0].localeCompare(b[0]))
          .map(([host, arr]) => `  ${host}: ${arr.map((m) => m.name).join(", ")}`)
          .join("\n")
      : "  (无其他节点在线)";

    const note = [
      "",
      "## Team",
      `你是多机 Pi 集群的一员,本节点名 \`${self}\`${selfTags.length ? `,tags: ${selfTags.join(", ")}` : ""}。`,
      "",
      "在线节点(按机器分组):",
      rosterText,
      tags.length ? `可用分组:${tags.map((t) => `#${t}`).join(" ")}` : "",
      "",
      "**发送**:调用 `team_send({ to, text })`。`to` 可以是节点名、`\"#tag\"` 分组、`\"*\"`(全员),或数组(如 `[\"web1\",\"#db\"]`)。",
      "节点名从上面的列表里取,不要自己拼。",
      "",
      "**接收**:输入里出现 `[来自 <名字> 的 team 消息]` 前缀时,那是另一个 agent 发来的消息,不是真人打字。",
      "按消息本身的意思回应:它可能是任务、回复、提问,或协作中的一步。是任务就动手,是对话就接着走。",
      "不要反问「需要我做什么」,也不要复述确认。你这一轮的最终输出会自动回传给发信方。",
      "**不要**在收到 team 消息后额外调用 `team_send` 回复 —— 那会造成重复投递。",
      "",
      "**克制**:不要主动给其他节点发消息,除非任务需要。每次发送都会占用对方一轮思考;",
      "用 `\"*\"` 或 `#tag` 群发时,会同时占用所有收件人的一轮思考,尤其要克制。",
      "",
    ]
      .filter((l) => l !== "")
      .join("\n");

    return { systemPrompt: event.systemPrompt + note };
  });

  // ---- 模型可调用的发送工具
  pi.registerTool({
    name: "team_send",
    label: "Team Send",
    description:
      "给同一集群里的其他 Pi 节点发消息。to 可以是节点名、'#tag' 分组、'*' 全员,或名字数组。节点名从系统提示的 Team 段落获取。",
    promptSnippet: "team_send(to, text) — 给一个或一组 Pi 节点发消息",
    promptGuidelines: [
      "Use team_send only when the task genuinely spans another machine; every send costs each recipient a full model turn.",
      "Never use team_send to reply to an incoming team message — the reply is automatic.",
      "Broadcasting with '*' or a '#tag' wakes every matching node; prefer naming recipients.",
    ],
    parameters: Type.Object({
      to: Type.String({
        description: "节点名、'#tag' 分组、'*' 全员,或逗号分隔的多收件人(如 'web1,#db')",
      }),
      text: Type.String({ description: "消息内容:说清背景、期望产出和验收标准,一次说全" }),
    }),

    renderCall(args, theme) {
      const raw = String(args?.to ?? "?");
      const isBroadcast = raw === "all" || raw === "*" || raw.includes(",") || raw.includes("#");
      const head =
        theme.fg("toolTitle", theme.bold("team_send ")) +
        theme.fg(isBroadcast ? "warning" : "accent", isBroadcast ? `📢 ${raw}` : `📤 ${raw}`);
      const body = String(args?.text ?? "");
      const lines = body.split("\n");
      let text = head + "\n" + lines.slice(0, 4).map((l) => theme.fg("muted", `  ${l}`)).join("\n");
      if (lines.length > 4) text += "\n" + theme.fg("dim", `  …还有 ${lines.length - 4} 行`);
      return new Text(text, 0, 0);
    },

    renderResult(result, _options, theme) {
      const d = result.details as { delivered?: boolean; to?: string } | undefined;
      if (d?.delivered === false) {
        return new Text(theme.fg("error", "⚠️ 未连接 broker,消息没发出去"), 0, 0);
      }
      return new Text(theme.fg("success", `✓ 已投递给 ${d?.to ?? "?"}`), 0, 0);
    },

    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      ctxRef = ctx;
      const to = parseRecipients(params.to);

      if (!send(to, { text: params.text, hops: 0, relayedFrom: self }, "model")) {
        return {
          content: [{ type: "text", text: "发送失败:未连接 broker。" }],
          details: { delivered: false, to: params.to },
        };
      }

      showCard({ kind: "send", peer: params.to, text: params.text });

      return {
        content: [
          {
            type: "text",
            text: `已发给 ${params.to}。回执只表示对方 socket 收到了,不表示对方已处理完。`,
          },
        ],
        details: { delivered: true, to: params.to },
      };
    },
  });

  // ---- /team 命令
  pi.registerCommand("team", {
    description: "Pi Agent Team:成员 / 发送 / 群发 / 模式 / 状态",
    getArgumentCompletions(prefix) {
      // 第一段:子命令
      if (!prefix.includes(" ")) {
        const subs = ["send", "say", "peers", "status", "announce", "on", "off"];
        return subs
          .filter((s) => s.startsWith(prefix))
          .map((s) => ({ value: s, label: s, description: `/team ${s}` }));
      }

      const [sub, ...rest] = prefix.split(" ");
      const partial = rest.join(" ");

      // 第二段(收件人):节点名 + 分组 + 全员
      if (["send", "say"].includes(sub) && rest.length <= 1) {
        const items = [
          { value: "*", label: "*", description: "全员(除自己)" },
          ...knownTags().map((t) => ({ value: `#${t}`, label: `#${t}`, description: "分组" })),
          ...others().map((m) => ({
            value: m.name,
            label: m.name,
            description: m.host ? `${m.host}${m.tags.length ? ` · ${m.tags.join(" ")}` : ""}` : undefined,
          })),
        ];
        return items.filter((i) => i.value.startsWith(partial));
      }

      if (sub === "announce") {
        return ["off", "auto", "always"]
          .filter((m) => m.startsWith(partial))
          .map((m) => ({
            value: m,
            label: m,
            description: m === "auto" ? "收到 team 消息后自动回给对方" : m === "always" ? "每轮都推给所有节点" : "只手动发送",
          }));
      }

      return null;
    },

    handler: async (args, ctx) => {
      ctxRef = ctx;
      const trimmed = args.trim();

      // 无参数 → 交互菜单
      if (!trimmed) {
        await menu(ctx);
        return;
      }

      const parts = trimmed.split(/\s+/);
      const sub = parts[0];

      switch (sub) {
        case "status": {
          ctx.ui.notify(
            [
              `节点名    ${self}`,
              `本机标签  ${selfTags.length ? selfTags.join(", ") : "(无)"}`,
              `状态      ${status}`,
              `broker    ${brokerUrl || "(未配置)"}`,
              `在线      ${members.length} 个节点`,
              `announce  ${announceMode}`,
            ].join("\n"),
            "info",
          );
          break;
        }

        case "peers": {
          ctx.ui.notify(rosterLines().join("\n"), others().length ? "info" : "warning");
          break;
        }

        case "say":
        case "send": {
          const rawTo = parts[1];
          const text = parts.slice(2).join(" ");
          if (!rawTo || !text) {
            ctx.ui.notify("用法:/team send <名字|#分组|*|a,b> <内容>", "warning");
            break;
          }
          const to = parseRecipients(rawTo);

          // 群发前确认 —— 每多一个收件人就多一份模型开销,不该手滑就发生
          if (to === "*" || Array.isArray(to) || (typeof to === "string" && to.startsWith("#"))) {
            const n = to === "*" ? others().length : Array.isArray(to) ? to.length : others().filter((m) => m.tags.includes(to.slice(1))).length;
            if (n === 0) {
              ctx.ui.notify(`没有匹配的收件人(${rawTo})`, "warning");
              break;
            }
            if (n > BULK_WARN_THRESHOLD) {
              const ok = await ctx.ui.confirm(
                `群发给 ${n} 个节点?`,
                `每个收件人都会跑一轮完整思考,消耗各自的 token。`,
              );
              if (!ok) {
                ctx.ui.notify("已取消", "info");
                break;
              }
            }
          }

          if (!send(to, { text, hops: 0, relayedFrom: self }, "user")) {
            ctx.ui.notify("未连接 broker,消息没发出去", "error");
            break;
          }
          showCard({ kind: "send", peer: rawTo, text });
          break;
        }

        case "announce": {
          const mode = parts[1];
          if (mode !== "off" && mode !== "auto" && mode !== "always") {
            ctx.ui.notify(`当前 announce=${announceMode}。用法:/team announce <off|auto|always>`, "warning");
            break;
          }
          announceMode = mode;
          ctx.ui.notify(`announce=${mode}`, "info");
          break;
        }

        case "on":
          announceMode = "auto";
          ctx.ui.notify("announce=auto(收到 team 消息后自动回给对方)", "info");
          break;

        case "off":
          announceMode = "off";
          ctx.ui.notify("announce=off(只在 /team send 或 team_send 时发送)", "info");
          break;

        default:
          ctx.ui.notify(`未知子命令 "${sub}"。直接运行 /team 打开菜单。`, "warning");
      }
    },
  });

  // ---------------------------------------------------------------- 菜单

  /**
   * /team 的交互菜单。
   *
   * select() 只返回字符串,所以用编号前缀把选择映射回动作。
   * 这不是花哨 —— 是为了让"发消息给谁"这件事不需要记名字。
   */
  async function menu(ctx: ExtensionContext) {
    const list = others();

    const choices: { label: string; run: () => Promise<void> }[] = [
      {
        label: "📋 查看成员",
        run: async () => {
          ctx.ui.notify(rosterLines().join("\n"), list.length ? "info" : "warning");
        },
      },
      {
        label: "✉️  发消息给某个节点",
        run: async () => {
          if (!list.length) {
            ctx.ui.notify("没有其他节点在线", "warning");
            return;
          }
          const pick = await ctx.ui.select(
            "发给谁?",
            list.map((m) => `${m.name}${m.host ? `  —  ${m.host}` : ""}${m.tags.length ? `  [${m.tags.join(" ")}]` : ""}`),
          );
          if (!pick) return;
          const target = list.find((m) => pick.startsWith(m.name));
          if (!target) return;
          const text = await ctx.ui.input(`发给 ${target.name}`, "消息内容");
          if (!text?.trim()) return;
          if (!send(target.name, { text, hops: 0, relayedFrom: self }, "user")) {
            ctx.ui.notify("未连接 broker,消息没发出去", "error");
            return;
          }
          showCard({ kind: "send", peer: target.name, text });
        },
      },
      {
        label: "📢 群发",
        run: async () => {
          if (!list.length) {
            ctx.ui.notify("没有其他节点在线", "warning");
            return;
          }
          const tags = knownTags();
          const options = [
            `全员  (${list.length} 个节点)`,
            ...tags.map((t) => `#${t}  (${list.filter((m) => m.tags.includes(t)).length} 个节点)`),
          ];
          const pick = await ctx.ui.select("群发给哪一组?", options);
          if (!pick) return;

          const isAll = pick.startsWith("全员");
          const to = isAll ? "*" : `#${pick.slice(1).split("  ")[0]}`;
          const n = isAll ? list.length : list.filter((m) => m.tags.includes(to.slice(1))).length;

          if (n > BULK_WARN_THRESHOLD) {
            const ok = await ctx.ui.confirm(`群发给 ${n} 个节点?`, "每个收件人都会跑一轮完整思考。");
            if (!ok) return;
          }

          const text = await ctx.ui.input(`群发给 ${pick}`, "消息内容");
          if (!text?.trim()) return;
          if (!send(to, { text, hops: 0, relayedFrom: self }, "user")) {
            ctx.ui.notify("未连接 broker,消息没发出去", "error");
            return;
          }
          showCard({ kind: "send", peer: isAll ? "全员" : to, text });
        },
      },
      {
        label: `🔔 自动回信模式  (当前:${announceMode})`,
        run: async () => {
          const pick = await ctx.ui.select("announce 模式", [
            "off  —  只手动发送",
            "auto  —  收到 team 消息后自动回给对方",
            "always  —  每轮都推给所有在线节点(两边都开会互相刷屏)",
          ]);
          if (!pick) return;
          const mode = pick.split(" ")[0] as AnnounceMode;
          if (mode === "off" || mode === "auto" || mode === "always") {
            announceMode = mode;
            ctx.ui.notify(`announce=${mode}`, "info");
          }
        },
      },
      {
        label: "📊 状态",
        run: async () => {
          ctx.ui.notify(
            [
              `节点名    ${self}`,
              `本机标签  ${selfTags.length ? selfTags.join(", ") : "(无)"}`,
              `状态      ${status}`,
              `broker    ${brokerUrl || "(未配置)"}`,
              `在线      ${members.length} 个节点`,
              `announce  ${announceMode}`,
            ].join("\n"),
            "info",
          );
        },
      },
    ];

    const pick = await ctx.ui.select(
      "Pi Agent Team",
      choices.map((c) => c.label),
    );
    if (!pick) return;

    const idx = choices.findIndex((c) => c.label === pick);
    if (idx >= 0) await choices[idx].run();
  }
}
