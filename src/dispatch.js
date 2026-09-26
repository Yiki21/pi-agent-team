/**
 * Team 动作分发 —— 纯函数,不依赖 Pi、不依赖网络。
 *
 * 命令(/team ...)和工具(team_*)都调这里的 dispatch(),所以两个
 * 入口的行为不可能分叉。它们只负责:
 *   - 把参数整理成 { sub, args }
 *   - 把返回的 intentions 执行掉并选择输出方式
 *
 * dispatch 不产生副作用。它返回一份"打算做什么"的描述,由调用方
 * 执行。这是它能被单测的原因:
 *
 *   const r = dispatch({ sub: "send", args: ["peer", "hi"] }, state, env)
 *   assert.equal(r.intentions[0].type, "send")
 */

import {
  knownLabels,
  others as othersOf,
  parseRecipients,
  resolveLocal,
  teamSize,
} from "./session.js";
import { createTeam, joinTeam, leaveTeam, listTeams, readTeam } from "./team-config.js";

/** 群发前确认阈值。每个收件人都会跑一轮完整思考,不该手滑就发生。 */
export const BULK_WARN_THRESHOLD = 5;

/**
 * @typedef {{
 *   ok: boolean,
 *   lines: string[],
 *   error?: string,
 *   intentions?: Array<Record<string, unknown>>,
 *   party?: { kind: string } & Record<string, unknown>,
 * }} Result
 *
 * Intentions 是调用方要执行的动作:
 *   { type: "send", to, id, re, body }       通过 transport 发出
 *   { type: "card", kind, peer, text, reason? }  在聊天流插卡片
 *   { type: "notify", level, message }       通知用户
 *   { type: "status" }                       刷新状态栏
 *
 * party 是生命周期动作(连接/断开/重连),由调用方处理:
 *   { kind: "connect", team, config }
 *   { kind: "disconnect" }
 *   { kind: "reconnect", labels }   标签变了要重连 broker 才会知道
 *   { kind: "confirmBulk", n }      需要用户确认的群发
 */

const ok = (lines, extra = {}) => ({ ok: true, lines, intentions: [], ...extra });
// bad() 也带 intentions:[] —— 让调用方不必区别 undefined 和空数组。
// 之前 send 离线时返回 undefined,调用方要么写 defensive 判断,
// 要么就漏掉,两种都容易出错。
const bad = (error) => ({ ok: false, lines: [], intentions: [], error });

/**
 * 消息 id 生成。
 *
 * 必须全 cluster 唯一。早期版本是 `m-<时间>-<进程内计数器>`,
 * 缺了进程标识 —— 两个节点在同一毫秒各发一条时,时间相同、计数
 * 器都从 0 开始,id 必然碰撞。接收方靠 id 去重,于是第二条被当成
 * 重复投递静默丢弃。
 *
 * 症状极难定位:发送方看到"投递 1/1",接收方毫无反应。
 *
 * 组成:时间(可排序)+ 进程级随机数(跨进程不重复)+ 进程内计数器
 * (同毫秒内不重复)。
 */
const PROC_TAG = Math.random().toString(36).slice(2, 8);
let idSeq = 0;
const newId = () => `m-${Date.now().toString(36)}-${PROC_TAG}-${(idSeq++).toString(36)}`;

/**
 * 主分发。
 *
 * @param {{ sub: string, args: string[] }} input
 * @param {import("./session.js").SessionState} state  读写:只改 announce / selfLabels / outbound
 * @param {{ connState: string, team: string|null, config: {url,token,labels?}|null, host?: string }} env
 * @returns {Result}
 */
export function dispatch(input, state, env) {
  const { sub, args } = input;

  switch (sub) {
    case "":
    case "status":
      return ok(statusLines(state, env));

    case "peers":
      return peersResult(state);

    case "create":
      return createResult(args, state);

    case "join":
      return joinResult(args);

    case "leave":
      return leaveResult(args, state, env);

    case "label":
      return labelResult(args, state, env);

    case "send":
    case "say":
      return sendResult(args, state, env);

    case "announce":
      return announceResult(args, state);

    case "on":
      state.announce = "auto";
      return ok(["announce=auto(收到请求后自动回一次)"]);

    case "off":
      state.announce = "off";
      return ok(["announce=off(只在 /team send 或 team_send 时发送)"]);

    default:
      return bad(`未知子命令 "${sub}"。直接运行 /team 打开菜单。`);
  }
}

// ---------------------------------------------------------------- status / peers

function statusLines(state, env) {
  const cfg = env.config ?? (env.team ? readTeam(env.team) : null);
  return [
    `team       ${env.team ?? "(未加入)"}`,
    `节点名     ${state.self || "(未设置)"}`,
    `本机标签   ${state.selfLabels?.length ? state.selfLabels.join(", ") : "(无)"}`,
    `连接       ${env.connState}`,
    `broker     ${cfg?.url ?? "(未配置)"}`,
    `在线       ${teamSize(state)} 个节点(${othersOf(state).length} 个其他节点)`,
    `自动回信   ${state.announce}`,
    `本机 team  ${listTeams().join(", ") || "(无)"}`,
  ];
}

function peersResult(state) {
  const list = othersOf(state);
  if (!list.length) return { ok: true, lines: [`只有你自己在线(${state.self})`], intentions: [] };

  const lines = [];
  const byHost = new Map();
  for (const m of list) {
    const k = m.host ?? "(未知主机)";
    if (!byHost.has(k)) byHost.set(k, []);
    byHost.get(k).push(m);
  }
  for (const [host, arr] of [...byHost].sort((a, b) => a[0].localeCompare(b[0]))) {
    lines.push(`${host}  (${arr.length})`);
    for (const m of [...arr].sort((x, y) => x.name.localeCompare(y.name))) {
      lines.push(`  ${m.name}${m.labels?.length ? `  [${m.labels.join(" ")}]` : ""}`);
    }
  }
  const labels = knownLabels(state);
  if (labels.length) lines.push("", `可用分组:${labels.map((l) => `@${l}`).join(" ")}`);
  return ok(lines);
}

// ---------------------------------------------------------------- team 生命周期

function createResult(args, state) {
  const [team, url, token] = args;
  if (!team || !url) return bad("用法:/team create <team> <url> [token]");

  const r = createTeam({ team, url, token, labels: state.selfLabels ?? [] });
  if (!r.ok) return bad(r.reason);

  const lines = [`已创建 team "${team}"  →  ${r.path}`, `broker: ${r.config.url}`];
  if (r.created) {
    lines.push(
      "",
      "生成的 token(只显示这一次,配置文件里也有一份):",
      r.token,
      "",
      "其它机器用这条加入:",
      `/team join ${team} ${r.config.url} <token>`,
    );
  }
  // 创建后直接连上,省得用户再敲一次 join
  return ok(lines, { party: { kind: "connect", team, config: r.config } });
}

function joinResult(args) {
  const [team, url, token] = args;
  if (!team) return bad("用法:/team join <team> [url] [token]");

  const r = joinTeam({ team, url, token });
  if (!r.ok) return bad(r.reason);

  const lines = [`加入 team "${team}"`, `broker: ${r.config.url}`];
  if (r.adopted) lines.push("(首次加入,已记到本机配置)");
  return ok(lines, { party: { kind: "connect", team, config: r.config } });
}

function leaveResult(args, state, env) {
  const target = args[0] ?? env.team;
  if (!target) return bad("用法:/team leave <team>");

  const r = leaveTeam({ team: target });
  if (!r.ok) return bad(r.reason);
  return ok([`已离开 team "${target}"(本地配置已删)`], { party: { kind: "disconnect" } });
}

// ---------------------------------------------------------------- label

function labelResult(args, state, env) {
  const [op, ...rest] = args;
  const labels = new Set(state.selfLabels ?? []);

  if (!op || op === "list") {
    return ok([`本机标签:${labels.size ? [...labels].join(", ") : "(无)"}`]);
  }

  if (op === "add" || op === "remove" || op === "rm") {
    const names = rest.filter(Boolean);
    if (!names.length) return bad(`用法:/team label ${op} <名字...>`);
    for (const l of names) (op === "add" ? labels.add(l) : labels.delete(l));
    state.selfLabels = [...labels];

    const lines = [`标签已更新:${state.selfLabels.join(", ") || "(无)"}`];
    // 标签是 broker 侧的分组依据,改了必须重连它才知道
    if (env.connState === "online") lines.push("(标签变了,正在重连 broker)");
    return ok(lines, { party: { kind: "reconnect", labels: state.selfLabels } });
  }

  return bad("用法:/team label [list|add <名字...>|remove <名字...>]");
}

// ---------------------------------------------------------------- send

function sendResult(args, state, env) {
  const rawTo = args[0];
  const text = args.slice(1).join(" ");
  if (!rawTo || !text) return bad("用法:/team send <名字|@分组|*|@default|a,b> <内容>");

  return sendMessage(rawTo, text, "user", state, env);
}

/**
 * 发送的实际执行 —— command 和 tool 共用这一条路径。
 *
 * 群发超过阈值时不直接发,返回 confirmBulk 让上层决定怎么问:
 * 命令走 confirm 对话框,工具走结构化返回让模型自己判断。
 */
export function sendMessage(rawTo, text, origin, state, env) {
  const to = parseRecipients(rawTo);
  const local = resolveLocal(state, to);

  if (local.targets.length === 0) {
    return bad(
      local.unknown.length
        ? `没有匹配的收件人(${local.unknown.join(",")})`
        : "没有其他节点在线",
    );
  }

  const isBulk =
    to === "*" || to === "@default" || Array.isArray(to) || (typeof to === "string" && to.startsWith("@"));
  if (isBulk && local.targets.length > BULK_WARN_THRESHOLD) {
    return ok([`准备群发给 ${local.targets.length} 个节点:${local.targets.join(", ")}`], {
      party: { kind: "confirmBulk", n: local.targets.length, to, text, origin },
    });
  }

  return doSend(to, text, origin, local, state, env);
}

/**
 * 真正发出。确认过群发之后也走这里,避免两条路径。
 */
export function doSend(to, text, origin, local, state, env) {
  if (env.connState !== "online") return bad("未连接,消息没发出去");

  const id = newId();
  // 记下 origin:对方回复时靠它判断"模型知道这回事吗"
  state.outbound.set(id, { text, origin, to });

  return ok([`已发给 ${formatTarget(to)}(${local.targets.length} 个节点)`], {
    intentions: [
      // 契约(与 session.js 一致):send 意图用**顶层** text / hops,
      // 由 index.ts 组装成信封的 body。
      // 曾经一边写 body:{text} 一边读 it.text,导致 /team send 发出
      // 空正文的消息 —— 对方只看到空字符串,症状是"投递成功但对方没反应"。
      { type: "send", to, id, re: null, text, hops: 0 },
      { type: "card", kind: "send", peer: formatTarget(to), text },
    ],
  });
}

function formatTarget(to) {
  if (Array.isArray(to)) return to.join(",");
  if (to === "*") return "全员";
  if (to === "@default") return "默认组";
  return String(to).replace(/^@/, "#");
}

// ---------------------------------------------------------------- announce

function announceResult(args, state) {
  const mode = args[0];
  if (mode !== "off" && mode !== "auto" && mode !== "always") {
    return bad(`当前 announce=${state.announce}。用法:/team announce <off|auto|always>`);
  }
  state.announce = mode;
  return ok([`announce=${mode}`]);
}
