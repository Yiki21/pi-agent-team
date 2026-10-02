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
  bindReply,
  knownLabels,
  others as othersOf,
  parseRecipients,
  resolveLocal,
  teamSize,
} from "./session.js";
import { createTeam, joinTeam, leaveTeam, listTeams, readTeam } from "./team-config.js";
import { OPTION_HELP, checkModeRequirements, parseOptionArgs, validateOptions } from "./options.js";
import { MODES } from "./mode.js";
import { MAX_WATCHES, validateWatchTarget } from "./watch.js";

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
 * @param {import("./session.js").SessionState} state  读写:只改 reply / selfLabels / outbound
 * @param {{ connState: string, team: string|null, config: {url,token,labels?}|null, host?: string }} env
 * @returns {Result}
 */
function watchResult(args, state, env) {
  // 订阅靠 broker 转发通知,mesh/swim 里没有 broker 可以登记。
  // 要说得髺,不能静默 no-op —— 用户会以为在看,实际什么都没发生。
  const mode = env?.mode ?? env?.config?.mode ?? "broker";
  if (mode !== "broker") {
    return bad(`订阅只在 broker 模式下可用(当前 ${mode})。mesh/swim 里通知没有地方转发。`);
  }

  const action = (args[0] ?? "").trim();
  const target = (args[1] ?? "").trim();

  if (!action || action === "list") {
    const list = [...state.watch.targets].sort();
    return ok(
      list.length
        ? [
            `正在订阅:${list.join(", ")}`,
            "",
            "收到的是对方的最终回答(只出卡片,不会叫醒你)。",
            "对方不知道你在看。broker 重启后会自动重新登记。",
          ]
        : [
            "没有订阅任何人",
            "",
            "用法:/team watch add <节点名>",
            `上限 ${MAX_WATCHES} 个。只在 broker 模式可用。`,
          ],
    );
  }

  if (action === "add") {
    const err = validateWatchTarget(target, state.self);
    if (err) return bad(err);
    if (state.watch.targets.has(target)) return ok([`已经订阅了 ${target}`]);
    if (state.watch.targets.size >= MAX_WATCHES) {
      return bad(`最多订阅 ${MAX_WATCHES} 个,先 /team watch remove 掉一个`);
    }
    // 不在这里改本地集合:broker 才是权威。watch_ack 回来才真的算订阅上。
    return ok([`正在订阅 ${target}…`], {
      intentions: [{ type: "watch", action: "add", target }],
    });
  }

  if (action === "remove" || action === "off") {
    if (!target) return bad("要取消订阅谁?/team watch remove <节点名>");
    if (!state.watch.targets.has(target)) {
      // 本地不知道但 broker 可能有(比如另一个会话登记的),照样发出去
      return ok([`本来就没有订阅 ${target}`], {
        intentions: [{ type: "watch", action: "remove", target }],
      });
    }
    return ok([`已取消订阅 ${target}`], {
      intentions: [{ type: "watch", action: "remove", target }],
    });
  }

  return bad(`未知的订阅动作 "${action}"。用 add / remove / list。`);
}

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
      return joinResult(args, state);

    case "leave":
      return leaveResult(args, state, env);

    case "mode":
      return modeResult(args, state, env);

    case "label":
      return labelResult(args, state, env);

    case "send":
    case "say":
      return sendResult(args, state, env, input);

    // reply 是现在的名字;announce 保留为别名,免得已有的肌肉记忆失效
    case "reply":
    case "announce":
      return replyResult(args, state, sub);

    case "watch":
      return watchResult(args, state, env);

    case "on":
      state.reply = "remind";
      return ok(["reply=remind(请求没被回复时提醒一次)"]);

    case "off":
      state.reply = "off";
      return ok(["reply=off(不提醒,发不发由模型自己决定)"]);

    default:
      return bad(`未知子命令 "${sub}"。直接运行 /team 打开菜单。`);
  }
}

// ---------------------------------------------------------------- status / peers

function statusLines(state, env) {
  const cfg = env.config ?? (env.team ? readTeam(env.team) : null);
  const mode = env.mode ?? cfg?.mode ?? "broker";

  const lines = [
    // 这里说的是"本次连接属于哪个 team",和末尾"已存 team 列表"不是一回事。
    // 措辞要区分开,否则用环境变量连接时会同时看到"未加入"和"已存 dev",
    // 看起来自相矛盾(实测中确实被误读成一个 bug)。
    `当前 team  ${env.team ?? "(未绑定,由环境变量直连)"}`,
    `节点名     ${state.self || "(未设置)"}`,
    `本机标签   ${state.selfLabels?.length ? state.selfLabels.join(", ") : "(无)"}`,
    `模式       ${mode}`,
    `连接       ${env.connState}`,
  ];

  if (mode === "broker") {
    lines.push(`broker     ${cfg?.url ?? "(未配置)"}`);
  } else {
    lines.push(`seeds      ${(cfg?.seeds ?? []).join(", ") || "(无,只能被动等待别人连你)"}`);
    lines.push(`投递端口   ${env.listenPort ?? "(未就绪)"}`);

    // 种子地址说的是哪个端口,取决于模式:
    //   mesh  直接连对端的投递端口
    //   swim  连对端的 gossip 端口,投递端口由成员信息带出来
    if (mode === "swim") {
      lines.push(`gossip 端口 ${env.gossipPort ?? "(未就绪)"}`);
      if (env.gossipPort) lines.push(`种子写法   <本机可达地址>:${env.gossipPort}`);
    } else if (env.listenPort) {
      lines.push(`种子写法   <本机可达地址>:${env.listenPort}`);
    }
  }

  lines.push(
    `在线       ${teamSize(state)} 个节点(${othersOf(state).length} 个其他节点)`,
    `回信策略   ${state.reply}`,
    `已存 team  ${listTeams().join(", ") || "(无)"}`,
  );
  return lines;
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

/**
 * 缺 team 名时的提示。
 *
 * 以前只报一句"用法:…"加一张选项表。用户给齐了 --url 和 --token,
 * 看起来该给的都给了,所以读那张表找不出错在哪 —— 实际上漏的是
 * 最前面那个位置参数。这里把"缺的是什么"说在第一行。
 */
function missingTeamName(sub, parsed) {
  const given = Object.keys(parsed.values);
  const lines = [];

  if (given.length) {
    // 给了选项但没给名字:这是最容易犯的错,要直接点出来
    lines.push(
      `缺少 team 名。你给了 ${given.map((k) => `--${k}`).join(" ")},但 team 名要放在最前面:`,
      "",
      `  /team ${sub} <team名> ${given.map((k) => `--${k} …`).join(" ")}`,
    );
  } else {
    lines.push(`缺少 team 名。`, "", `  /team ${sub} <team名> [选项]`);
  }

  const known = listTeams();
  if (sub === "join" && known.length) {
    lines.push("", `本机已有:${known.join(", ")}`, `已配置过的 team 直接 /team join ${known[0]} 就行,不用再给 url 和 token。`);
  }
  if (sub === "create") {
    lines.push("", "team 名只能用小写字母和数字,例如 dev、prod2。");
  }

  lines.push("", "选项:", OPTION_HELP);
  return lines.join("\n");
}

function createResult(args, state) {
  // 位置形式(/team create t <url> [token])和选项形式都接受。
  // 位置形式保留是因为它用了很久,选项形式是为了设置 mode/seeds。
  const parsed = parseOptionArgs(args);
  if (parsed.unknown.length) return bad(`认不出的选项:${parsed.unknown.join(", ")}\n${OPTION_HELP}`);

  const [team, posUrl, posToken] = parsed.rest;
  if (!team) return bad(missingTeamName("create", parsed));

  const values = { ...parsed.values };
  if (posUrl && !values.url) values.url = posUrl;
  if (posToken && !values.token) values.token = posToken;

  const v = validateOptions(values);
  if (!v.ok) return bad(v.reason);

  const mode = v.team.mode ?? "broker";
  const req = checkModeRequirements({
    mode,
    url: v.team.url,
    seeds: v.team.seeds,
    token: v.team.token ?? "(待生成)",
  });
  if (!req.ok) return bad(req.reason);

  const r = createTeam({
    team,
    url: v.team.url,
    token: v.team.token,
    mode,
    seeds: v.team.seeds ?? [],
  });
  if (!r.ok) return bad(r.reason);

  const lines = [`已创建 team "${team}"  →  ${r.path}`, `模式: ${r.config.mode}`];
  if (r.config.url) lines.push(`broker: ${r.config.url}`);
  if (r.config.seeds?.length) lines.push(`seeds: ${r.config.seeds.join(", ")}`);
  if (req.warning) lines.push("", `注意:${req.warning}`);

  if (r.created) {
    lines.push("", "生成的 token(只显示这一次,配置文件里也有一份):", r.token);
  }

  // create 只写本地配置、让本机去连。它不启动任何东西。
  //
  // 这一点以前没说,于是用户会以为 team 已经建好可以用了 —— 而此时
  // 对面可能根本没有 broker 在监听,或者在监听但用的是另一个 token。
  // 两种都表现为"连不上",看起来像网络问题。
  if (r.config.mode === "broker") {
    const host = (() => {
      try {
        return new URL(r.config.url).hostname;
      } catch {
        return "<broker 所在机器>";
      }
    })();
    const port = (() => {
      try {
        return new URL(r.config.url).port || "8787";
      } catch {
        return "8787";
      }
    })();
    lines.push(
      "",
      `下一步:broker 需要你自己在 ${host} 上启动 —— /team create 不会替你启动它。`,
      "在那台机器上运行(只需要 Node,不需要 Pi):",
      "",
      `  TEAM_TOKEN='${r.token}' npx -y -p @yiki21/pi-agent-team pi-agent-team-broker --bind ${host} --port ${port}`,
      "",
      "token 必须和这里一致,否则 broker 会拒绝连接。",
      "要常驻运行,见 docs/systemd.md。",
    );
  }

  lines.push(
    "",
    "其它机器用这条加入:",
    `  /team join ${team} --url ${r.config.url ?? "<url>"} --token ${r.token}${
      r.config.mode !== "broker" ? ` --mode ${r.config.mode}` : ""
    }`,
  );
  // 创建后直接连上,省得用户再敲一次 join
  return ok(lines, {
    party: {
      kind: "connect",
      team,
      config: r.config,
      session: v.session,
    },
  });
}

function joinResult(args, state) {
  const parsed = parseOptionArgs(args);
  if (parsed.unknown.length) return bad(`认不出的选项:${parsed.unknown.join(", ")}\n${OPTION_HELP}`);

  const [team, posUrl, posToken] = parsed.rest;
  if (!team) return bad(missingTeamName("join", parsed));

  const values = { ...parsed.values };
  if (posUrl && !values.url) values.url = posUrl;
  if (posToken && !values.token) values.token = posToken;

  const v = validateOptions(values);
  if (!v.ok) return bad(v.reason);

  // 已有配置时,mode/seeds 必须能改 —— 否则想从 broker 换成 mesh
  // 就只能 leave 再 join,而那会把 token 也一起忘掉。
  const existing = readTeam(team);
  const mode = v.team.mode ?? existing?.mode ?? "broker";

  const req = checkModeRequirements({
    mode,
    url: v.team.url ?? existing?.url,
    seeds: v.team.seeds ?? existing?.seeds,
    token: v.team.token ?? existing?.token,
  });
  if (!req.ok) return bad(req.reason);

  const r = joinTeam({
    team,
    url: v.team.url,
    token: v.team.token,
    mode: v.team.mode,
    seeds: v.team.seeds,
  });
  if (!r.ok) return bad(r.reason);

  const lines = [`加入 team "${team}"`, `模式: ${r.config.mode}`];
  if (r.config.url) lines.push(`broker: ${r.config.url}`);
  if (r.config.seeds?.length) lines.push(`seeds: ${r.config.seeds.join(", ")}`);
  if (r.adopted) lines.push("(首次加入,已记到本机配置)");
  if (r.updated) lines.push("(配置已更新)");
  if (req.warning) lines.push("", `注意:${req.warning}`);

  return ok(lines, {
    party: {
      kind: "connect",
      team,
      config: r.config,
      session: v.session,
    },
  });
}

/** /team mode —— 查看或切换模式,不必重敲 url/token */
function modeResult(args, state, env) {
  const want = (args[0] ?? "").toLowerCase();

  if (!want) {
    const current = env.mode ?? env.config?.mode ?? "broker";
    return ok([
      `当前模式:${current}`,
      "",
      "切换:",
      "  /team mode broker   经 broker 中转,需要 url",
      "  /team mode mesh     节点直连,需要 seeds",
      "  /team mode swim     SWIM 管成员 + 直连投递,需要 seeds 和边车",
      "",
      "只切换模式不会动 url / token / seeds —— 它们在 team 配置里。",
      "需要改那些就用 /team join <team> --url ... --token ...。",
    ]);
  }

  if (!MODES.includes(want)) return bad(`模式只能是 ${MODES.join(" / ")},实际 "${want}"`);

  const team = env.team;
  if (!team) {
    return bad("还没绑定 team,无法保存模式。用 /team join <team> --mode " + want + " ...");
  }

  const existing = readTeam(team);
  if (!existing) return bad(`本地没有 team "${team}" 的配置`);

  const req = checkModeRequirements({
    mode: want,
    url: existing.url,
    seeds: existing.seeds,
    token: existing.token,
  });
  if (!req.ok) return bad(req.reason);

  const r = joinTeam({ team, mode: want });
  if (!r.ok) return bad(r.reason);

  const lines = [`模式已切换:${existing.mode ?? "broker"} → ${want}`];
  if (req.warning) lines.push("", `注意:${req.warning}`);
  if (want === "swim") lines.push("", "swim 需要边车:cd swim && go build -o ../.tmp/swim-sidecar .");

  return ok(lines, { party: { kind: "connect", team, config: r.config, session: sessionFrom(state) } });
}

/** 把当前会话级的选项打包给 connect */
function sessionFrom(state) {
  const out = {};
  if (state.self) out.name = state.self;
  if (state.selfLabels?.length) out.labels = state.selfLabels;
  return out;
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

function sendResult(args, state, env, input = {}) {
  const rawTo = args[0];
  const text = args.slice(1).join(" ");
  if (!rawTo || !text) return bad("用法:/team send <名字|@分组|*|@default|a,b> <内容>");

  // origin 以前写死成 "user",连 team_send 工具也是 —— 于是模型发出的消息
  // 被记成人发的,对方回复时只显示卡片,模型永远看不到那条回复。
  // 工具路径在 index.ts 里传 origin:"model"。
  const origin = input.origin === "model" ? "model" : "user";
  return sendMessage(rawTo, text, origin, state, env);
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
      // targets 必须带上。确认后重跑 doSend 时,bindReply 靠它判断"这条是不是在
      // 回复某个待回复的请求",并据此决定 re/hops。上层的 confirmBulk 处理器拿不到
      // 解析结果,只能自己编一个,而编出来的空名字会让 re 绑不上任何东西 ——
      // 群发就不再受 hop 上限保护。
      party: { kind: "confirmBulk", n: local.targets.length, targets: local.targets, to, text, origin },
    });
  }

  return doSend(to, text, origin, local, state, env);
}

/**
 * 真正发出。确认过群发之后也走这里,避免两条路径。
 */
/**
 * 消息是否超过一帧能装下的体积。
 *
 * ── 为什么在发送前就查 ──
 * 实测:超限时发送侧没有任何本地报错 —— socket.send 接受它,broker 读到
 * 长度头就断开连接,发送方只看到 close 1006,和网线被拔一模一样。
 * 用户会去查网络、防火墙、地址,而真正的原因是文本太长。
 *
 * 在本地拦住可以:保住连接、给出可操作的错误(让模型把内容拆短),
 * 而且三种 transport 都受益。
 *
 * 估算用顶层信封的字节数。body 之外还有 from/to/id/re 这些字段,
 * 所以这里留一点余量 —— 宁可稍微早报,也不要漏过去把连接搞断。
 */
export const ENVELOPE_HEADROOM = 512;

export function oversizeBy(text) {
  const frameLimit = 64 * 1024;
  const bytes = Buffer.byteLength(String(text ?? ""), "utf8");
  const total = bytes + ENVELOPE_HEADROOM;
  return total > frameLimit ? { bytes, limit: frameLimit, total } : null;
}

export function doSend(to, text, origin, local, state, env) {
  // 先查体积再查连接:超长是本地就能判断的问题,不该依赖连接状态
  const over = oversizeBy(text);
  if (over) {
    return bad(
      `消息太长,发不出去:${over.bytes} 字节(单条上限约 ${over.limit - ENVELOPE_HEADROOM} 字节)。` +
        `把它拆成几条,或者改成让对端自己去读文件。`,
    );
  }

  if (env.connState !== "online") return bad("未连接,消息没发出去");

  const id = newId();
  // 记下 origin:对方回复时靠它判断"模型知道这回事吗"
  state.outbound.set(id, { text, origin, to });

  // 发给一个正有待回复请求的队友 → 认成对那条请求的回复。
  // 带上 re,对端才知道这是回复、不该再自动回信;不带的话两个 agent
  // 会互相触发下去,只能靠跳数上限兜住。
  const { replyTo, re, hops } = bindReply(state, local.targets);

  const lines = [`已发给 ${formatTarget(to)}(${local.targets.length} 个节点)`];
  if (replyTo) lines.push(`(作为对 ${replyTo} 那条请求的回复)`);

  return ok(lines, {
    intentions: [
      // 契约(与 session.js 一致):send 意图用**顶层** text / hops,
      // 由 index.ts 组装成信封的 body。
      // 曾经一边写 body:{text} 一边读 it.text,导致 /team send 发出
      // 空正文的消息 —— 对方只看到空字符串,症状是"投递成功但对方没反应"。
      { type: "send", to, id, re, text, hops },
      { type: "card", kind: replyTo ? "reply" : "send", peer: formatTarget(to), text },
    ],
  });
}

function formatTarget(to) {
  if (Array.isArray(to)) return to.join(",");
  if (to === "*") return "全员";
  if (to === "@default") return "默认组";
  return String(to).replace(/^#/, "@");
}

// ---------------------------------------------------------------- reply

/** 旧的模式名映射到新的,别让已有脚本静默失效 */
const REPLY_ALIASES = { auto: "remind", always: "mirror" };
const REPLY_MODES = ["off", "remind", "mirror"];

export function normalizeReplyMode(raw) {
  const v = String(raw ?? "").trim();
  if (REPLY_MODES.includes(v)) return { mode: v, legacy: false };
  if (REPLY_ALIASES[v]) return { mode: REPLY_ALIASES[v], legacy: true };
  return { mode: null, legacy: false };
}

function replyResult(args, state, sub = "reply") {
  const raw = args[0];
  if (!raw) {
    return ok([
      `当前 reply=${state.reply}`,
      "",
      "  off     不提醒 —— 消息照常送达,回不回由模型自己决定",
      "  remind  请求没被回复时提醒一次(回复仍由模型显式用 team_send 发出)",
      "  mirror  每轮输出都镜像给所有节点(fyi,不叫醒对方)",
      "",
      `用法:/team ${sub} <off|remind|mirror>`,
    ]);
  }

  const { mode, legacy } = normalizeReplyMode(raw);
  if (!mode) {
    return bad(`模式只能是 ${REPLY_MODES.join(" / ")},实际 "${raw}"。当前 reply=${state.reply}`);
  }

  state.reply = mode;
  const note =
    mode === "off"
      ? "不提醒 —— 消息照常送达,是否需要回复完全由模型决定"
      : mode === "remind"
        ? "请求没得到回复时提醒一次"
        : "每轮输出都镜像给所有节点(fyi,不叫醒对方)";

  const lines = [`reply=${mode}`, note];
  if (legacy) lines.push(`(旧名字 "${raw}" 仍可用,但现在叫 "${mode}")`);
  return ok(lines);
}
