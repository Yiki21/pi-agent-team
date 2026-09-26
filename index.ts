/**
 * Pi Agent Team —— 让多台机器上的 Pi 互相通信,像一个集群。
 *
 * ── 分层 ──
 *   src/session.js       会话状态机(纯逻辑,单测)
 *   src/transport.js     transport 接口 + BrokerTransport(网络与重连)
 *   src/team-config.js   team 配置读写(create/join/leave 落地)
 *   src/dispatch.js      命令与工具共用的动作分发(纯逻辑,单测)
 *   index.ts             只做 Pi API 接线,不放决策逻辑
 *
 * ── 双入口 ──
 *   /team ...        给人用,输出走 ui.notify / 交互菜单
 *   team_* 工具      给模型和自动化用,输出走结构化返回值
 *   两者都调 dispatch.js 的同一个函数,行为不会分叉。
 *
 * ── 两个 API 分属不同对象(踩过的坑)──
 *   pi.sendUserMessage() / pi.appendEntry()  在 ExtensionAPI 上
 *   ctx.isIdle() / ctx.ui.select()           在 ExtensionContext 上
 * 写成 ctx.sendUserMessage() 会抛 "is not a function"。
 *
 * ── 生命周期契约 ──
 *   不在 factory 里开 socket。socket 从 session_start 起,
 *   由幂等的 session_shutdown 关。
 */

import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Box, Text } from "@earendil-works/pi-tui";
import { hostname } from "node:os";
import { Type } from "typebox";

import { createBrokerTransport, CLOSE_REPLACED } from "./src/transport.js";
import { createSessionState, handleIncoming, knownLabels, onTurnSettled, others, teamSize } from "./src/session.js";
import { createTeam, joinTeam, leaveTeam, listTeams, readTeam, toSocketUrl } from "./src/team-config.js";
import { BULK_WARN_THRESHOLD, dispatch, doSend, sendMessage } from "./src/dispatch.js";

// ---------------------------------------------------------------- 类型

type CardKind = "receive" | "send" | "reply" | "failed";

type CardDetails = { kind: CardKind; peer: string; text: string; at: number; reason?: string };

const CARD_TYPE = "team-message";

type ConnState = "offline" | "connecting" | "online" | "replaced";

// ---------------------------------------------------------------- 运行时

let state = createSessionState();
let transport: ReturnType<typeof createBrokerTransport> | null = null;
let connState: ConnState = "offline";
let currentTeam: string | null = null;
let currentConfig: { url: string; token: string; labels?: string[] } | null = null;

let ctxRef: ExtensionContext | null = null;
let apiRef: ExtensionAPI | null = null;

/** dispatch 需要的环境快照 */
const envOf = () => ({ connState, team: currentTeam, config: currentConfig });

// ---------------------------------------------------------------- 意图执行

/**
 * 执行 dispatch 产出的意图。这是唯一的"意图 → 副作用"映射点,
 * 两个入口共用,所以行为不可能分叉。
 */
async function runIntentions(
  intentions: Array<Record<string, unknown>>,
  ctx: ExtensionContext,
): Promise<string[]> {
  const notes: string[] = [];

  for (const it of intentions) {
    switch (it.type) {
      case "notify":
        notes.push(String(it.message));
        ctx.ui.notify(String(it.message), (it.level as "info" | "warning" | "error") ?? "info");
        break;

      case "status":
        renderStatus();
        break;

      case "card":
        showCard({
          kind: it.kind as CardKind,
          peer: String(it.peer),
          text: String(it.text),
          reason: it.reason as string | undefined,
        });
        break;

      case "send": {
        // 契约:send 意图用顶层 text / hops / to / id / re。
        // 之前的 bug 是 index.ts 读 it.body 而 session.js 给 it.text,
        // 于是自动回信发出一个没有 body 的信封,被 broker 判为畸形。
        const okSent = transport?.send({
          to: it.to as string | string[],
          id: String(it.id),
          re: (it.re as string | null) ?? null,
          body: {
            text: String(it.text ?? ""),
            hops: typeof it.hops === "number" ? it.hops : 1,
            ...(it.fyi ? { fyi: true } : {}),
          },
        });
        if (!okSent) {
          const msg = "team:未连接,消息没发出去";
          notes.push(msg);
          ctx.ui.notify(msg, "error");
        }
        break;
      }

      case "inject": {
        try {
          if (ctx.isIdle()) apiRef?.sendUserMessage(String(it.payload));
          else apiRef?.sendUserMessage(String(it.payload), { deliverAs: "followUp" });
        } catch (err) {
          const msg = `team:注入失败 ${(err as Error).message}`;
          notes.push(msg);
          ctx.ui.notify(msg, "error");
        }
        break;
      }
    }
  }

  return notes;
}

/**
 * 处理 dispatch 返回的 party(生命周期动作)。
 * 这些动作需要连接管理,不属于意图执行。
 */
async function runParty(party: Record<string, unknown> | undefined, ctx: ExtensionContext): Promise<boolean> {
  if (!party) return true;

  switch (party.kind) {
    case "connect":
      connectWith(party.team as string | null, party.config as { url: string; token: string; labels?: string[] });
      return true;

    case "disconnect":
      transport?.stop();
      transport = null;
      connState = "offline";
      currentTeam = null;
      currentConfig = null;
      renderStatus();
      return true;

    case "reconnect": {
      const labels = party.labels as string[];
      if (transport && currentConfig) connectWith(currentTeam, { ...currentConfig, labels });
      return true;
    }

    case "confirmBulk": {
      const n = party.n as number;
      const proceed = await ctx.ui.confirm(
        `群发给 ${n} 个节点?`,
        "每个收件人都会跑一轮完整思考,消耗各自的 token。",
      );
      if (!proceed) {
        ctx.ui.notify("已取消", "info");
        return false;
      }
      // 确认后走同一条发送路径,不复制逻辑
      const to = party.to as string | string[];
      const local = { targets: new Array(n).fill("") as string[], unknown: [] };
      const r = doSend(to, String(party.text), (party.origin as "user" | "model") ?? "user", local, state, envOf());
      if (!r.ok) {
        ctx.ui.notify(r.error!, "error");
        return false;
      }
      await runIntentions(r.intentions ?? [], ctx);
      return true;
    }
  }
  return true;
}

/** dispatch + 执行。命令和工具的统一入口。 */
async function invoke(input: { sub: string; args: string[] }, ctx: ExtensionContext) {
  ctxRef = ctx;
  const r = dispatch(input, state, envOf());

  if (!r.ok) return { ok: false, lines: [] as string[], error: r.error!, notes: [] as string[] };

  const proceeded = await runParty(r.party, ctx);

  // confirmBulk 被取消时,party 已经处理过 intentions,不要再执行一次
  const notes = r.party?.kind === "confirmBulk"
    ? []
    : await runIntentions(r.intentions ?? [], ctx);

  renderStatus();
  return { ok: proceeded, lines: r.lines, error: undefined, notes };
}

// ---------------------------------------------------------------- 卡片 / 状态栏

function showCard(details: Omit<CardDetails, "at">) {
  apiRef?.appendEntry<CardDetails>(CARD_TYPE, { ...details, at: Date.now() });
}

function renderStatus() {
  const ui = ctxRef?.ui;
  if (!ui) return;
  if (connState === "replaced") {
    ui.setStatus("team", `⚠️ team:${state.self || "?"} (被顶替)`);
    return;
  }
  const icon = connState === "online" ? "🟢" : connState === "connecting" ? "🟡" : "🔴";
  ui.setStatus("team", `${icon} team:${state.self || "?"} (${teamSize(state)})`);
}

// ---------------------------------------------------------------- 连接

function connectWith(team: string | null, config: { url: string; token: string; labels?: string[] }) {
  transport?.stop();

  currentTeam = team;
  currentConfig = config;
  // 只在配置真的带了非空标签时才用它。
  // 空数组是 truthy —— 直接赋值会把命令行 --team-labels 覆盖成空,
  // 而 createTeam 写配置时恰好会留下 labels: []。
  if (config.labels?.length) state.selfLabels = config.labels;
  state.members = [];
  state.pendingReply = null;

  transport = createBrokerTransport({ url: toSocketUrl(config.url), token: config.token });

  transport.on("state", (next, detail) => {
    connState = next as ConnState;

    if (next === "replaced") {
      ctxRef?.ui.notify(
        `team:节点名 "${state.self}" 已被另一个实例接管,本实例停止重连。换一个名字,或关掉那个实例。`,
        "error",
      );
    } else if (next === "offline" && (detail as { code?: number })?.code === CLOSE_REPLACED) {
      ctxRef?.ui.notify("team:连接被同名实例顶替", "warning");
    }
    renderStatus();
  });

  transport.on("envelope", (env) => {
    const ctx = ctxRef;
    if (!ctx) return;
    // 决策全在 session.js;这里只执行它产出的动作
    const actions = handleIncoming(state, env as Parameters<typeof handleIncoming>[1]);
    void runIntentions(actions as unknown as Array<Record<string, unknown>>, ctx);
    renderStatus();
  });

  transport.start({ name: state.self, labels: state.selfLabels, host: safeHostname() });
  renderStatus();
}

// ---------------------------------------------------------------- 导出

export default function (pi: ExtensionAPI) {
  apiRef = pi;

  // ---- 卡片渲染器(entry 版:不进 LLM 上下文)
  pi.registerEntryRenderer<CardDetails>(CARD_TYPE, (entry, { expanded }, theme) => {
    const d = entry.data;
    const kind: CardKind = d?.kind ?? "receive";
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
      theme.fg("dim", `${m.arrow} ${d?.peer ?? "?"}`) +
      (d?.at ? theme.fg("dim", `  ${new Date(d.at).toLocaleTimeString()}`) : "");

    const lines = body.split("\n");
    const shown = expanded ? lines : lines.slice(0, 6);
    let text = head + "\n" + shown.map((l) => `  ${l}`).join("\n");
    if (!expanded && lines.length > 6) text += "\n" + theme.fg("dim", `  …还有 ${lines.length - 6} 行(展开查看)`);
    if (d?.reason) text += "\n" + theme.fg(m.color, `  ${d.reason}`);

    const box = new Box(0, 1, (t) => theme.bg("customMessageBg", t));
    box.addChild(new Text(text, 0, 0));
    return box;
  });

  // ---- CLI flags。--team 是常规入口,其余用于显式覆盖。
  pi.registerFlag("team", { description: "启动时加入的 team 名", type: "string" });
  pi.registerFlag("team-name", { description: "本节点的名字", type: "string" });
  pi.registerFlag("team-labels", { description: "本节点的标签,逗号分隔", type: "string" });
  pi.registerFlag("team-url", { description: "直接给 broker URL(不读 team 配置)", type: "string" });
  pi.registerFlag("team-announce", { description: "自动回信模式:off | auto | always", type: "string" });

  // ---- 生命周期
  pi.on("session_start", async (_event, ctx) => {
    ctxRef = ctx;

    state.self =
      (pi.getFlag("team-name") as string) ??
      process.env.TEAM_NAME ??
      process.cwd().split("/").filter(Boolean).pop() ??
      "pi";

    const labels = (pi.getFlag("team-labels") as string) ?? process.env.TEAM_LABELS ?? "";
    state.selfLabels = labels.split(",").map((s) => s.trim()).filter(Boolean);

    const announce = (pi.getFlag("team-announce") as string) ?? process.env.TEAM_ANNOUNCE;
    if (announce === "off" || announce === "auto" || announce === "always") state.announce = announce;
    else if (process.env.TEAM_QUIET === "1") state.announce = "off";

    const team = (pi.getFlag("team") as string) ?? process.env.TEAM ?? "";
    const url = (pi.getFlag("team-url") as string) ?? process.env.TEAM_URL ?? "";
    const token = process.env.TEAM_TOKEN ?? "";

    if (team) {
      // joinTeam 只读本地配置 / 或记录新配置,不涉及网络
      const r = joinTeam({ team, url: url || undefined, token: token || undefined, save: false });
      if (!r.ok) {
        ctx.ui.notify(`team:${r.reason}`, "error");
        return;
      }
      connectWith(team, r.config);
      return;
    }

    if (url && token) {
      connectWith(null, { url, token, labels: state.selfLabels });
      return;
    }

    const known = listTeams();
    ctx.ui.notify(
      known.length
        ? `team:未指定 team。本机已有:${known.join(", ")}。用 --team <名字> 或 /team join`
        : "team:未加入任何 team。用 /team create 或 /team join",
      "warning",
    );
  });

  pi.on("session_shutdown", async () => {
    transport?.stop();
    transport = null;
    ctxRef = null;
  });

  pi.on("turn_start", async (_event, ctx) => {
    ctxRef = ctx;
  });

  // ---- 累积 assistant 文本
  //
  // agent_settled 的事件对象只有 { type },没有 messages,所以文本
  // 必须在这里攒。
  pi.on("message_end", async (event) => {
    const m = event.message as { role?: string; content?: unknown };
    if (m?.role !== "assistant") return;
    state.lastText = extractAssistantText(m.content);
  });

  pi.on("agent_settled", async (_event, ctx) => {
    ctxRef = ctx;
    const actions = onTurnSettled(state);
    void runIntentions(actions as unknown as Array<Record<string, unknown>>, ctx);
  });

  // ---- 系统提示
  pi.on("before_agent_start", async (event) => {
    const list = others(state);
    const labels = knownLabels(state);
    const mode = process.env.TEAM_MODE ?? "broker";

    const roster = list.length
      ? [...list]
          .sort((a, b) => a.name.localeCompare(b.name))
          .map((m) => `  ${m.name}${m.host ? ` (${m.host})` : ""}${m.labels?.length ? ` [${m.labels.join(" ")}]` : ""}`)
          .join("\n")
      : "  (无其他节点在线)";

    const note = [
      "",
      "## Team",
      `你是多机 Pi 集群的一员。节点名 \`${state.self}\`,team \`${currentTeam ?? "(直接连接)"}\`,模式 \`${mode}\`。`,
      state.selfLabels?.length ? `你的标签:${state.selfLabels.join(", ")}` : "",
      "",
      "在线节点:",
      roster,
      labels.length ? `可用分组:${labels.map((l) => `@${l}`).join(" ")}` : "",
      "",
      "**发送**:调用 `team_send({ to, text })`。`to` 可以是节点名、`@label`(分组)、`\"*\"`(全员)、`\"@default\"`(默认组),或数组。",
      "**查成员**:调用 `team_roster()`,或 `team_info({ what: \"peers\" })`。",
      "",
      "**接收**:输入里出现 `[来自 <名字> 的 team 消息]` 前缀时,那是另一个 agent 发来的请求,不是真人打字。",
      "按内容本身的意思回应:是任务就执行,是讨论就接着走。不要反问「需要我做什么」。",
      "你这一轮的最终输出会自动回传给发信方。**不要**再用 team_send 回复,那会造成重复投递。",
      "看到 `[来自 <名字> 的 team 回复]` 前缀时,你这一轮**不会**自动回传;要继续对话才显式调用 team_send。",
      "",
      "**克制**:每次发送都占用对方一轮完整思考,群发更贵。除非任务需要,不要主动发消息。",
      "",
    ]
      .filter((l) => l !== "")
      .join("\n");

    return { systemPrompt: event.systemPrompt + note };
  });

  // ---- 工具入口(给模型和自动化)
  pi.registerTool({
    name: "team_send",
    label: "Team Send",
    description:
      "给同一 team 里的其他 Pi 节点发消息。to 可以是节点名、'@label' 分组、'*' 全员、'@default' 默认组,或逗号分隔的名字数组。名字从 team_roster 或系统提示的 Team 段落获取。",
    promptSnippet: "team_send(to, text) — 给一个或一组 Pi 节点发消息",
    promptGuidelines: [
      "Use team_send only when the task spans another machine; each recipient costs a full model turn.",
      "Never use team_send to reply to an incoming team message; that reply is automatic.",
      "Broadcasting with '*' or '@label' wakes every matching node; prefer naming recipients.",
    ],
    parameters: Type.Object({
      to: Type.String({ description: "节点名、'@label'、'*'、'@default',或逗号分隔多收件人" }),
      text: Type.String({ description: "消息内容:背景、期望产出、验收标准一次说清" }),
    }),

    renderCall(args, theme) {
      const raw = String(args?.to ?? "?");
      const bulk = raw === "*" || raw === "@default" || raw.includes(",") || raw.startsWith("@") || raw.startsWith("#");
      const head =
        theme.fg("toolTitle", theme.bold("team_send ")) +
        theme.fg(bulk ? "warning" : "accent", bulk ? `📢 ${raw}` : `📤 ${raw}`);
      const lines = String(args?.text ?? "").split("\n");
      let text = head + "\n" + lines.slice(0, 4).map((l) => theme.fg("muted", `  ${l}`)).join("\n");
      if (lines.length > 4) text += "\n" + theme.fg("dim", `  …还有 ${lines.length - 4} 行`);
      return new Text(text, 0, 0);
    },

    renderResult(result, _options, theme) {
      const d = result.details as { delivered?: boolean; to?: string; error?: string } | undefined;
      if (d?.delivered === false) {
        return new Text(theme.fg("error", `⚠️ ${d.error ?? "未连接,消息没发出去"}`), 0, 0);
      }
      return new Text(theme.fg("success", `✓ 已投递给 ${d?.to ?? "?"}`), 0, 0);
    },

    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      ctxRef = ctx;
      const r = await invoke({ sub: "send", args: [params.to, params.text] }, ctx);

      if (!r.ok) {
        return {
          content: [{ type: "text", text: `发送失败:${r.error}` }],
          details: { delivered: false, to: params.to, error: r.error },
        };
      }
      return {
        content: [
          {
            type: "text",
            text: `${r.lines.join(" ")}。回执只表示对方 socket 收到了,不表示对方已处理完。`,
          },
        ],
        details: { delivered: true, to: params.to },
      };
    },
  });

  pi.registerTool({
    name: "team_roster",
    label: "Team Roster",
    description: "列出在线节点及其所在机器和标签。用于按机器或标签挑选收件人,或确认谁在线。",
    promptSnippet: "team_roster() — 列出在线节点与标签",
    parameters: Type.Object({}),

    renderCall(_args, theme) {
      return new Text(theme.fg("toolTitle", theme.bold("team_roster ")), 0, 0);
    },

    renderResult(result, _options, theme) {
      const d = result.details as { members?: Array<{ name: string; host: string | null; labels: string[] }> } | undefined;
      const list = d?.members ?? [];
      return new Text(
        theme.fg(
          "muted",
          list.length
            ? list.map((m) => `${m.name}${m.labels.length ? ` [${m.labels.join(" ")}]` : ""} @${m.host ?? "?"}`).join("\n")
            : "(无其他节点)",
        ),
        0,
        0,
      );
    },

    async execute(_toolCallId, _params, _signal, _onUpdate, ctx) {
      ctxRef = ctx;
      const members = others(state).map((m) => ({ name: m.name, host: m.host, labels: m.labels ?? [] }));
      const labels = knownLabels(state);
      return {
        content: [
          {
            type: "text",
            text: members.length
              ? [
                  ...members.map((m) => `${m.name}${m.labels.length ? ` [${m.labels.join(" ")}]` : ""} host=${m.host ?? "?"}`),
                  labels.length ? `\n可用分组:${labels.map((l) => `@${l}`).join(" ")}` : "",
                ].join("\n")
              : "没有其他节点在线",
          },
        ],
        details: { members, labels },
      };
    },
  });

  pi.registerTool({
    name: "team_info",
    label: "Team Info",
    description: "查看本节点在 team 里的状态:team 名、连接状态、broker、在线数量、自动回信模式。",
    promptSnippet: "team_info() — 查看 team 状态",
    parameters: Type.Object({
      what: Type.Optional(Type.String({ description: "'status'(默认)或 'peers'" })),
    }),

    renderCall(args, theme) {
      return new Text(
        theme.fg("toolTitle", theme.bold("team_info ")) + theme.fg("muted", String(args?.what ?? "status")),
        0,
        0,
      );
    },

    renderResult(result, _options, theme) {
      const d = result.details as { lines?: string[] } | undefined;
      return new Text(theme.fg("muted", (d?.lines ?? []).join("\n")), 0, 0);
    },

    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      ctxRef = ctx;
      const r = await invoke({ sub: params.what === "peers" ? "peers" : "status", args: [] }, ctx);
      return {
        content: [{ type: "text", text: r.ok ? r.lines.join("\n") : `失败:${r.error}` }],
        details: { lines: r.ok ? r.lines : [], ok: r.ok },
      };
    },
  });

  /**
   * team_join / team_leave / team_label 也做成工具。
   *
   * 它们会改本机配置或连接状态,属于配置操作。给模型开放是因为
   * 自动化场景确实需要(让 agent 自己加入一个 team 并开始协作),
   * 但说明里写清楚它们有副作用。
   */
  pi.registerTool({
    name: "team_join",
    label: "Team Join",
    description:
      "加入一个 team。team 已在本机配置里时只需 team 名;首次加入需要 url 和 token。会连接 broker 并影响后续消息收发。",
    promptSnippet: "team_join(team, url?, token?) — 加入 team",
    parameters: Type.Object({
      team: Type.String({ description: "team 名(小写字母数字)" }),
      url: Type.Optional(Type.String({ description: "broker URL,首次加入时必需" })),
      token: Type.Optional(Type.String({ description: "team token,首次加入时必需" })),
    }),

    renderCall(args, theme) {
      return new Text(
        theme.fg("toolTitle", theme.bold("team_join ")) + theme.fg("accent", String(args?.team ?? "?")),
        0,
        0,
      );
    },

    renderResult(result, _options, theme) {
      const d = result.details as { ok?: boolean; lines?: string[] } | undefined;
      return new Text(theme.fg(d?.ok ? "success" : "error", (d?.lines ?? []).join("\n")), 0, 0);
    },

    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      const r = await invoke({ sub: "join", args: [params.team, params.url ?? "", params.token ?? ""].filter(Boolean) }, ctx);
      return {
        content: [{ type: "text", text: r.ok ? r.lines.join("\n") : `失败:${r.error}` }],
        details: { ok: r.ok, lines: r.ok ? r.lines : [r.error] },
      };
    },
  });

  pi.registerTool({
    name: "team_leave",
    label: "Team Leave",
    description: "离开当前 team:断开连接并删除本机配置。不影响 broker 或其它节点。",
    promptSnippet: "team_leave(team?) — 离开 team",
    parameters: Type.Object({
      team: Type.Optional(Type.String({ description: "要离开的 team 名,省略则离开当前" })),
    }),

    renderCall(args, theme) {
      return new Text(
        theme.fg("toolTitle", theme.bold("team_leave ")) + theme.fg("muted", String(args?.team ?? "(当前)")),
        0,
        0,
      );
    },

    renderResult(result, _options, theme) {
      const d = result.details as { ok?: boolean; lines?: string[] } | undefined;
      return new Text(theme.fg(d?.ok ? "success" : "error", (d?.lines ?? []).join("\n")), 0, 0);
    },

    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      const r = await invoke({ sub: "leave", args: params.team ? [params.team] : [] }, ctx);
      return {
        content: [{ type: "text", text: r.ok ? r.lines.join("\n") : `失败:${r.error}` }],
        details: { ok: r.ok, lines: r.ok ? r.lines : [r.error] },
      };
    },
  });

  pi.registerTool({
    name: "team_label",
    label: "Team Label",
    description:
      "管理本节点的标签,用于被别人按 @label 群发。add/remove 会重连 broker(标签是 broker 侧的分组依据)。",
    promptSnippet: "team_label(action, labels?) — 管理本节点标签",
    parameters: Type.Object({
      action: Type.String({ description: "'list'、'add' 或 'remove'" }),
      labels: Type.Optional(Type.String({ description: "逗号分隔的标签名" })),
    }),

    renderCall(args, theme) {
      return new Text(
        theme.fg("toolTitle", theme.bold("team_label ")) +
          theme.fg("accent", String(args?.action ?? "list")) +
          (args?.labels ? theme.fg("muted", ` ${args.labels}`) : ""),
        0,
        0,
      );
    },

    renderResult(result, _options, theme) {
      const d = result.details as { ok?: boolean; lines?: string[] } | undefined;
      return new Text(theme.fg(d?.ok ? "success" : "error", (d?.lines ?? []).join("\n")), 0, 0);
    },

    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      const list = String(params.labels ?? "").split(",").map((s) => s.trim()).filter(Boolean);
      const r = await invoke({ sub: "label", args: [String(params.action ?? "list"), ...list] }, ctx);
      return {
        content: [{ type: "text", text: r.ok ? r.lines.join("\n") : `失败:${r.error}` }],
        details: { ok: r.ok, lines: r.ok ? r.lines : [r.error] },
      };
    },
  });

  // ---- 命令入口(给人用)
  pi.registerCommand("team", {
    description: "Pi Agent Team:状态 / 成员 / 发送 / team 生命周期 / 标签",
    getArgumentCompletions(prefix) {
      const subs = ["status", "peers", "create", "join", "leave", "label", "send", "announce", "on", "off"];
      if (!prefix.includes(" ")) {
        return subs.filter((s) => s.startsWith(prefix)).map((s) => ({ value: s, label: s, description: `/team ${s}` }));
      }

      const [sub, ...rest] = prefix.split(" ");
      const partial = rest.join(" ");

      if (sub === "label") {
        return ["list", "add", "remove"].filter((o) => o.startsWith(partial)).map((o) => ({ value: o, label: o }));
      }

      if (sub === "send" && rest.length <= 1) {
        const items = [
          { value: "@default", label: "@default", description: "默认组(全员)" },
          { value: "*", label: "*", description: "全员" },
          ...knownLabels(state).map((l) => ({ value: `@${l}`, label: `@${l}`, description: "分组" })),
          ...others(state).map((m) => ({
            value: m.name,
            label: m.name,
            description: m.host ? `${m.host}${m.labels?.length ? ` · ${m.labels.join(" ")}` : ""}` : undefined,
          })),
        ];
        return items.filter((i) => i.value.startsWith(partial));
      }

      if (sub === "join" && rest.length === 0) {
        return listTeams().map((t) => ({ value: t, label: t, description: "本机已有配置" }));
      }

      if (sub === "announce") {
        return ["off", "auto", "always"]
          .filter((m) => m.startsWith(partial))
          .map((m) => ({ value: m, label: m }));
      }

      return null;
    },

    handler: async (args, ctx) => {
      ctxRef = ctx;
      const trimmed = args.trim();
      if (!trimmed) {
        await menu(ctx);
        return;
      }

      const parts = trimmed.split(/\s+/);
      const r = await invoke({ sub: parts[0], args: parts.slice(1) }, ctx);

      if (!r.ok) {
        ctx.ui.notify(r.error!, "warning");
        return;
      }
      // 意图执行阶段已经 notify 过 notes,这里只报 lines
      if (r.lines.length) ctx.ui.notify(r.lines.join("\n"), "info");
    },
  });

  // ---------------------------------------------------------------- 菜单

  async function menu(ctx: ExtensionContext) {
    const list = others(state);

    const choices: { label: string; run: () => Promise<void> }[] = [
      {
        label: "📋 查看成员",
        run: async () => {
          const r = await invoke({ sub: "peers", args: [] }, ctx);
          if (r.lines.length) ctx.ui.notify(r.lines.join("\n"), r.ok ? "info" : "warning");
        },
      },
      {
        label: "✉️  发消息给某个节点",
        run: async () => {
          if (!list.length) return void ctx.ui.notify("没有其他节点在线", "warning");
          const pick = await ctx.ui.select(
            "发给谁?",
            list.map((m) => `${m.name}${m.host ? `  —  ${m.host}` : ""}${m.labels?.length ? `  [${m.labels.join(" ")}]` : ""}`),
          );
          if (!pick) return;
          const target = list.find((m) => pick.startsWith(m.name));
          if (!target) return;
          const text = await ctx.ui.input(`发给 ${target.name}`, "消息内容");
          if (!text?.trim()) return;
          const r = await invoke({ sub: "send", args: [target.name, text] }, ctx);
          if (!r.ok) ctx.ui.notify(r.error!, "warning");
        },
      },
      {
        label: "📢 群发",
        run: async () => {
          if (!list.length) return void ctx.ui.notify("没有其他节点在线", "warning");
          const labels = knownLabels(state);
          const options = [
            `@default  —  默认组(${list.length} 个节点)`,
            `*  —  全员(${list.length} 个节点)`,
            ...labels.map((l) => `@${l}  —  ${list.filter((m) => m.labels?.includes(l)).length} 个节点`),
          ];
          const pick = await ctx.ui.select("群发给哪一组?", options);
          if (!pick) return;

          const to = pick.split("  ")[0];
          const text = await ctx.ui.input(`群发给 ${to}`, "消息内容");
          if (!text?.trim()) return;

          const r = await invoke({ sub: "send", args: [to, text] }, ctx);
          if (!r.ok) ctx.ui.notify(r.error!, "warning");
        },
      },
      {
        label: "🏷️  管理标签",
        run: async () => {
          const op = await ctx.ui.select("标签操作", [
            "list  —  查看当前标签",
            "add  —  添加",
            "remove  —  移除",
          ]);
          if (!op) return;
          const action = op.split(" ")[0];

          if (action === "list") {
            const r = await invoke({ sub: "label", args: ["list"] }, ctx);
            return void ctx.ui.notify(r.lines.join("\n"), "info");
          }

          const input = await ctx.ui.input(`${action === "add" ? "添加" : "移除"}哪些标签?`, "逗号分隔");
          if (!input?.trim()) return;
          const names = input.split(",").map((s) => s.trim()).filter(Boolean);
          const r = await invoke({ sub: "label", args: [action, ...names] }, ctx);
          if (!r.ok) ctx.ui.notify(r.error!, "warning");
        },
      },
      {
        label: "🔗 team 管理",
        run: async () => {
          const op = await ctx.ui.select("team 操作", [
            "list  —  列出本机已有 team",
            "join  —  加入一个 team",
            "create  —  创建一个 team",
            "leave  —  离开当前 team",
          ]);
          if (!op) return;
          const action = op.split(" ")[0];

          if (action === "list") {
            const known = listTeams();
            return void ctx.ui.notify(
              known.length ? known.map((t) => (t === currentTeam ? `${t}  ← 当前` : t)).join("\n") : "本机没有 team 配置",
              "info",
            );
          }

          if (action === "leave") {
            const r = await invoke({ sub: "leave", args: [] }, ctx);
            return void ctx.ui.notify(r.ok ? r.lines.join("\n") : r.error!, r.ok ? "info" : "warning");
          }

          if (action === "join") {
            const known = listTeams();
            const NEW = "(输入新的 team)";
            const picked = await ctx.ui.select("加入哪个 team?", [...known, NEW]);
            if (!picked) return;

            let teamName: string;
            let url: string | undefined;
            let token: string | undefined;

            if (picked === NEW) {
              const t = await ctx.ui.input("team 名", "小写字母数字");
              if (!t?.trim()) return;
              teamName = t.trim();
              const u = await ctx.ui.input("broker URL", "http://<tailscale-ip>:8787");
              if (!u?.trim()) return;
              url = u.trim();
              const k = await ctx.ui.input("token", "openssl rand -hex 32 生成的那个");
              if (!k?.trim()) return;
              token = k.trim();
            } else {
              teamName = picked;
            }

            const r = await invoke({ sub: "join", args: [teamName, url, token].filter(Boolean) as string[] }, ctx);
            return void ctx.ui.notify(r.ok ? r.lines.join("\n") : r.error!, r.ok ? "info" : "warning");
          }

          if (action === "create") {
            const t = await ctx.ui.input("新 team 名", "小写字母数字");
            if (!t?.trim()) return;
            const u = await ctx.ui.input("broker URL", "http://<tailscale-ip>:8787");
            if (!u?.trim()) return;

            const r = await invoke({ sub: "create", args: [t.trim(), u.trim()] }, ctx);
            if (!r.ok) return void ctx.ui.notify(r.error!, "warning");
            // token 只显示这一次,单独提示,避免被后续 notify 冲掉
            const tokenLine = r.lines.find((l) => /^[0-9a-f]{64}$/.test(l));
            if (tokenLine) {
              ctx.ui.notify(
                `team "${t.trim()}" 已创建并连接。\n\ntoken(只显示这一次,也在配置文件里):\n${tokenLine}\n\n其它机器用:/team join ${t.trim()} ${u.trim()} <token>`,
                "info",
              );
            } else {
              ctx.ui.notify(r.lines.join("\n"), "info");
            }
          }
        },
      },
      {
        label: `🔔 自动回信模式  (当前:${state.announce})`,
        run: async () => {
          const pick = await ctx.ui.select("announce 模式", [
            "off  —  只手动发送",
            "auto  —  收到请求后自动回一次",
            "always  —  每轮都推给所有节点(两边都开会互相刷屏)",
          ]);
          if (!pick) return;
          const r = await invoke({ sub: "announce", args: [pick.split(" ")[0]] }, ctx);
          if (!r.ok) ctx.ui.notify(r.error!, "warning");
        },
      },
      {
        label: "📊 状态",
        run: async () => {
          const r = await invoke({ sub: "status", args: [] }, ctx);
          if (r.lines.length) ctx.ui.notify(r.lines.join("\n"), "info");
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

// ---------------------------------------------------------------- 工具函数

function safeHostname(): string | null {
  try {
    return hostname() || null;
  } catch {
    return null;
  }
}

function extractAssistantText(content: unknown): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .filter((c: { type?: string }) => c?.type === "text")
      .map((c: { text?: string }) => c.text ?? "")
      .join("");
  }
  return "";
}
