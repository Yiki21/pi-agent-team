/**
 * Pi Agent Team — 让多台机器上的 Pi 互相通信,像一个集群。
 *
 * 传输:跑在 tailnet 上的一个 broker,所有节点主动 dial out。
 * 注入:pi.sendUserMessage() —— 对端消息以"用户输入"形式进入本会话,
 *       模型真的会处理它,和真人手打没区别。TUI 模式下同样生效。
 *
 * 用法:
 *   TEAM_URL=ws://<tailscale-ip>:8787 \
 *   TEAM_NAME=laptop \
 *   TEAM_TOKEN=xxx \
 *     pi --extension /path/to/pi-agent-team/index.ts
 *
 * ── 两个 API 分属不同对象(踩过的坑)──
 *   pi.sendUserMessage() / pi.sendMessage()  在 ExtensionAPI 上
 *   ctx.isIdle()                             在 ExtensionContext 上
 * 写成 ctx.sendUserMessage() 会抛 "is not a function"。
 *
 * ── 可视化(为什么需要)──
 *   对端消息如果不加标识,在 TUI 里和真人输入长得一模一样,分不出来。
 *   所以用 custom message 在聊天流里插卡片:
 *     📥 receive  ← 收到对端消息
 *     📤 send     → team_send 发出的
 *     🔁 reply    → 自动回传的回复
 *     ⚠️  failed   → 投递失败
 *
 *   关键事实(实测):sendMessage({display:true}) 只在 TUI 显示,
 *   【不进 LLM 上下文】。所以卡片是纯展示,模型看到的内容由
 *   sendUserMessage 那条独立提供,不会重复。
 *
 * ── 生命周期契约(来自 Pi 扩展文档)──
 *   不在 factory 里开 socket:有些调用会加载扩展但不启动会话。
 *   socket 从 session_start 起,由幂等的 session_shutdown 关。
 */

import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Box, Text } from "@earendil-works/pi-tui";
import { Type } from "typebox";

// ---------------------------------------------------------------- 类型

type Envelope = {
  from: string;
  to: string;
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
 *
 * 默认 auto:让"收到就回"能工作,又不会两个节点无端对推烧 token。
 */
type AnnounceMode = "off" | "auto" | "always";

/** 聊天流卡片种类 */
type CardKind = "receive" | "send" | "reply" | "failed";

type CardDetails = {
  kind: CardKind;
  peer: string;
  text: string;
  at: number;
  reason?: string;
};

const CARD_TYPE = "team-message";

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30_000;
const DEDUPE_CAP = 1000;

/** 跳数上限:A→B→A→B 的回声必须靠它断掉。 */
const MAX_HOPS = 4;

// ---------------------------------------------------------------- 状态

let socket: WebSocket | null = null;
let status: Status = "offline";
let self = "";
let brokerUrl = "";
let token = "";
let peers: string[] = [];
let reconnectDelay = RECONNECT_BASE_MS;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
let shuttingDown = false;

let announceMode: AnnounceMode = "auto";

/** 本轮由哪条 team 消息触发、几跳 —— auto 模式靠它决定回给谁。 */
let pendingReply: { to: string; hops: number } | null = null;

const seen = new Set<string>();
const injected = new Set<string>();

/** 最近一次 assistant 文本,由 message_end 累积。 */
let lastAssistantText = "";

let ctxRef: ExtensionContext | null = null;

/** ExtensionAPI 引用 —— sendUserMessage/sendMessage 在这里,不在 ctx 上。 */
let apiRef: ExtensionAPI | null = null;

const newId = () => `m-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

function remember(set: Set<string>, value: string) {
  set.add(value);
  if (set.size > DEDUPE_CAP) {
    const oldest = set.values().next().value;
    if (oldest !== undefined) set.delete(oldest);
  }
}

function render() {
  const ui = ctxRef?.ui;
  if (!ui) return;
  const size = peers.length > 0 ? peers.length : 1;
  const icon = status === "online" ? "🟢" : status === "connecting" ? "🟡" : "🔴";
  ui.setStatus("team", `${icon} team:${self} (${size})`);
}

// ---------------------------------------------------------------- 卡片

/**
 * 在聊天流里插一张 team 卡片。
 *
 * 用 appendEntry 而不是 sendMessage —— 这是实测得出的结论:
 * sendMessage({display:true}) 虽然注释说"发送自定义消息",但它
 * 【会进 LLM 上下文】(用 context 事件验证过,消息序列里出现
 * "CUSTOM:team-message")。后果是模型看到自己发出的卡片,会误认为
 * 是收到的消息 —— 实测 Opus 5.5 明确说"看起来是我发出去的消息
 * 又被回送到了我这边"。
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
 * 措辞经过三次修正,别走回头路:
 *   1) 只说"这是另一个 Pi 节点的消息" → 模型回 "Ready for the task"
 *      / "请告诉我需要处理什么任务",完全不动手。
 *   2) 改成"这是一项已下达的任务,立即执行" → 治好了被动,但把
 *      多轮对话压成了任务下发(poetB 发来的第二句诗会被描述成
 *      "poetA 交给你的任务",关系是错的)。
 *   3) 现在:中性描述来源 + 按内容本身回应。既足够主动,又不预设语义。
 */
function injectAsUserMessage(from: string, text: string, hops: number): boolean {
  const ctx = ctxRef;
  if (!ctx) return false;

  pendingReply = { to: from, hops };

  const payload =
    `[来自 ${from} 的 team 消息]\n` +
    `${text}\n\n` +
    `---\n` +
    `上面是 teammate ${from} 发来的消息原文(不是真人用户在打字)。` +
    `按内容本身的意思回应:是任务就执行,是讨论/诗句/提问就接着往下走。` +
    `不要反问"需要我做什么",也不要复述确认。` +
    `你这一轮的最终输出会自动原样回传给 ${from}。`;

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

  let ws: WebSocket;
  try {
    ws = new WebSocket(`${brokerUrl}?name=${encodeURIComponent(self)}&token=${encodeURIComponent(token)}`);
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

  ws.addEventListener("close", () => {
    socket = null;
    status = "offline";
    peers = [];
    render();
    scheduleReconnect();
  });

  ws.addEventListener("error", () => {});
}

function send(to: string, body: Record<string, unknown>, re: string | null = null): boolean {
  if (!socket || socket.readyState !== WebSocket.OPEN) return false;
  socket.send(JSON.stringify({ from: self, to, id: newId(), re, body }));
  return true;
}

// ---------------------------------------------------------------- 收

function handleEnvelope(env: Envelope) {
  const body = env.body ?? {};
  const kind = typeof body.kind === "string" ? body.kind : undefined;

  // ---- broker 系统消息
  if (env.from === "broker") {
    const others = (Array.isArray(body.peers) ? (body.peers as string[]) : []).filter((p) => p !== self);
    switch (kind) {
      case "welcome":
      case "peer_joined":
      case "peer_left":
        peers = others;
        render();
        if (kind === "peer_joined" && typeof body.peer === "string" && body.peer !== self) {
          ctxRef?.ui.notify(`team:${body.peer} 上线`, "info");
        }
        break;

      case "undeliverable":
        ctxRef?.ui.notify(`team:发给 ${String(body.to)} 失败(${String(body.reason)})`, "warning");
        break;

      case "delivered":
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

  const hops = typeof body.hops === "number" ? body.hops : 0;
  if (hops >= MAX_HOPS) return;

  const text = typeof body.text === "string" ? body.text : "";
  if (!text.trim()) return;

  // 先在聊天流里插卡片 —— 让"收到对端消息"这件事在屏幕上可见
  showCard({ kind: "receive", peer: env.from, text });

  injectAsUserMessage(env.from, text, hops);
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
    if (status !== "online" || peers.length === 0) return;
    lastAssistantText = "";
    for (const peer of peers) {
      if (send(peer, { text, hops: 1, relayedFrom: self })) {
        showCard({ kind: "send", peer, text });
      }
    }
    return;
  }

  if (announceMode === "auto") {
    const reply = pendingReply;
    pendingReply = null;
    if (!reply) return;
    lastAssistantText = "";

    const hops = reply.hops + 1;
    if (send(reply.to, { text, hops, relayedFrom: self })) {
      if (hops >= MAX_HOPS) {
        // 到顶了就不再往下传,卡片上也说清楚,免得用户以为还会继续
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

// ---------------------------------------------------------------- 导出

export default function (pi: ExtensionAPI) {
  apiRef = pi;

  // ---- 卡片渲染器(entry 版:不进 LLM 上下文)
  //
  // registerEntryRenderer 而不是 registerMessageRenderer —— 见 showCard
  // 的注释:message 版会污染上下文,entry 版不会。
  pi.registerEntryRenderer<CardDetails>(CARD_TYPE, (entry, { expanded }, theme) => {
    const d = entry.data;
    const kind = d?.kind ?? "receive";
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
    if (d?.reason) {
      text += "\n" + theme.fg(m.color, `  ${d.reason}`);
    }

    const box = new Box(outputPad, 1, (t) => theme.bg("customMessageBg", t));
    box.addChild(new Text(text, 0, 0));
    return box;
  });
  // ---- CLI flags
  pi.registerFlag("team-url", { description: "pi-agent-team broker URL", type: "string" });
  pi.registerFlag("team-name", { description: "本节点在团队里的名字", type: "string" });
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
    ctx.ui.notify(`team:以 ${self} 连接 ${brokerUrl}(announce=${announceMode})`, "info");
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
    const others = peers.filter((p) => p !== self);
    const note = [
      "",
      "## Team",
      `你是多机 Pi 集群的一员,本节点名 \`${self}\`。`,
      others.length
        ? `当前在线节点:${others.map((p) => `\`${p}\``).join("、")}。`
        : "当前没有其他节点在线。",
      "",
      "**发送**:调用 `team_send({ to, text })`,或由用户执行 `/team say <名字> <内容>`。`to` 用完整节点名,不要自己拼。",
      "",
      "**接收**:输入里出现 `[来自 <名字> 的 team 消息]` 前缀时,那是另一个 agent 发来的消息,不是真人打字。",
      "按消息本身的意思回应:它可能是任务、回复、提问,或协作中的一步。是任务就动手,是对话就接着走。",
      "不要反问「需要我做什么」,也不要复述确认。你这一轮的最终输出会自动回传给发信方。",
      "**不要**在收到 team 消息后额外调用 `team_send` 回复 —— 那会造成重复投递。",
      "",
      "**克制**:不要主动给其他节点发消息,除非任务需要。每次发送都会占用对方一轮思考。",
      "",
    ].join("\n");

    return { systemPrompt: event.systemPrompt + note };
  });

  // ---- 模型可调用的发送工具
  pi.registerTool({
    name: "team_send",
    label: "Team Send",
    description:
      "给同一集群里的另一个 Pi 节点发消息。节点名从系统提示的 Team 段落或 /team peers 获取,'all' 表示广播。",
    promptSnippet: "team_send(to, text) — 给另一个 Pi 节点发消息",
    promptGuidelines: [
      "Use team_send only when the task genuinely spans another machine; every send costs the peer a full model turn.",
      "Never use team_send to reply to an incoming team message — the reply is automatic.",
    ],
    parameters: Type.Object({
      to: Type.String({ description: "目标节点名,或 'all' 表示广播" }),
      text: Type.String({ description: "消息内容:说清背景、期望产出和验收标准,一次说全" }),
    }),

    /** 发送侧的可视化:颜色区分单播/广播 */
    renderCall(args, theme) {
      const to = String(args?.to ?? "?");
      const isBroadcast = to === "all" || to === "*";
      const head =
        theme.fg("toolTitle", theme.bold("team_send ")) +
        theme.fg(isBroadcast ? "warning" : "accent", isBroadcast ? `📢 广播` : `📤 ${to}`);
      const body = String(args?.text ?? "");
      const lines = body.split("\n");
      const shown = lines.slice(0, 4);
      let text = head + "\n" + shown.map((l) => theme.fg("muted", `  ${l}`)).join("\n");
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
      const to = params.to === "all" ? "*" : params.to;

      if (!send(to, { text: params.text, hops: 0, relayedFrom: self })) {
        return {
          content: [{ type: "text", text: "发送失败:未连接 broker。" }],
          details: { delivered: false, to: params.to },
        };
      }

      // 聊天流里也插一张卡片,和入站卡片对称
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
    description: "Pi Agent Team:status / peers / say / announce / on / off",
    handler: async (args, ctx) => {
      ctxRef = ctx;
      const parts = args.trim().split(/\s+/);
      const sub = parts[0] ?? "";

      switch (sub) {
        case "":
        case "status": {
          const others = peers.filter((p) => p !== self);
          ctx.ui.notify(
            [
              `节点名    ${self}`,
              `状态      ${status}`,
              `broker    ${brokerUrl || "(未配置)"}`,
              `在线      ${[self, ...others].join(", ")}`,
              `announce  ${announceMode}`,
            ].join("\n"),
            "info",
          );
          break;
        }

        case "peers": {
          const others = peers.filter((p) => p !== self);
          ctx.ui.notify(
            others.length ? others.map((p, i) => `${i + 1}. ${p}`).join("\n") : `只有你自己在线(${self})`,
            others.length ? "info" : "warning",
          );
          break;
        }

        case "say": {
          const to = parts[1];
          const text = parts.slice(2).join(" ");
          if (!to || !text) {
            ctx.ui.notify("用法:/team say <名字|all> <内容>", "warning");
            break;
          }
          if (!send(to === "all" ? "*" : to, { text, hops: 0, relayedFrom: self })) {
            ctx.ui.notify("未连接 broker,消息没发出去", "error");
            break;
          }
          showCard({ kind: "send", peer: to, text });
          ctx.ui.notify(`已发给 ${to}`, "info");
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
          ctx.ui.notify("announce=off(只在 /team say 或 team_send 时发送)", "info");
          break;

        default:
          ctx.ui.notify("用法:/team [status|peers|say|announce|on|off]", "info");
      }
    },
  });
}
