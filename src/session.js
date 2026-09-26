/**
 * Team 会话状态机 —— 纯逻辑,不依赖 Pi、不依赖网络。
 *
 * 为什么单独一层:index.ts 里有 1000 行,状态、决策和 Pi API 调用混在
 * 一起,导致"broker 重启后怎么办""对端在请求和回复之间离线怎么办"
 * 这类场景只能靠真机手测。
 *
 * 这个模块只做决策,不做 I/O。调用方(扩展)负责把它的输出变成
 * 实际动作:
 *
 *   const s = createSessionState()
 *   for (const action of handleIncoming(s, env, outbound)) {
 *     switch (action.type) {
 *       case "inject":  pi.sendUserMessage(action.payload); break
 *       case "send":    socket.send(...); break
 *       case "card":    pi.appendEntry(...); break
 *       case "notify":  ctx.ui.notify(...); break
 *     }
 *   }
 *
 * 这样每个场景都能用几行测试覆盖,不需要起 Pi、不需要起 broker。
 */

/** 跳数上限:最后一道防线,正常对话形状不应碰到它 */
const MAX_HOPS = 4;

/** 成员标签上限,和 broker 保持一致 */
const MAX_LABELS = 8;

/** 已发出消息的记录上限,防止长会话内存增长 */
const OUTBOUND_CAP = 1000;

// ---------------------------------------------------------------- 类型

/**
 * @typedef {{ name: string, host: string|null, addr: string|null, labels: string[], since: number }} Member
 * @typedef {{ text: string, origin: "user"|"model", to: string|string[] }} Outbound
 * @typedef {{ seen: Set<string>, injected: Set<string>, outbound: Map<string, Outbound>,
 *             members: Member[], self: string, selfLabels: string[],
 *             announce: "off"|"auto"|"always",
 *             pendingReply: { to: string, hops: number, re: string }|null,
 *             lastText: string }} SessionState
 *
 * @typedef {{ type: "inject", payload: string, from: string }
 *   | { type: "card", kind: "receive"|"send"|"reply"|"failed"|"fyi", peer: string, text: string, reason?: string }
 *   | { type: "send", to: string|string[], text: string, hops: number, re: string|null, origin: "user"|"model" }
 *   | { type: "notify", level: "info"|"warning"|"error", message: string }
 *   | { type: "status" }} Action
 */

// ---------------------------------------------------------------- 构造

const newId = () => `m-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

export function createSessionState(self = "") {
  return {
    seen: new Set(),
    injected: new Set(),
    outbound: new Map(),
    members: [],
    self,
    /** 本节点自己的标签。broker 需要它来解析 @label 群发。 */
    selfLabels: [],
    announce: "auto",
    pendingReply: null,
    lastText: "",
  };
}

/** 有上限的 Map:超过容量丢最早的条目 */
function rememberBounded(map, key, value, cap = OUTBOUND_CAP) {
  map.set(key, value);
  if (map.size > cap) {
    const oldest = map.keys().next().value;
    if (oldest !== undefined) map.delete(oldest);
  }
}

function remember(set, value, cap = OUTBOUND_CAP) {
  set.add(value);
  if (set.size > cap) {
    const oldest = set.values().next().value;
    if (oldest !== undefined) set.delete(oldest);
  }
}

// ---------------------------------------------------------------- 成员视图

/** 其他成员(排除自己) */
export const others = (s) => s.members.filter((m) => m.name !== s.self);

/** 树规模 = 其他成员 + 自己 */
export const teamSize = (s) => others(s).length + 1;

/** 所有已知 label,用于补全和分组显示 */
export function knownLabels(s) {
  const out = new Set();
  for (const m of others(s)) for (const l of m.labels ?? []) out.add(l);
  return [...out].sort();
}

/**
 * 应用 broker 推来的成员快照。
 *
 * 同时接受新格式(members,带元数据)和旧格式(peers,只有名字数组),
 * 因为 broker 可能先于扩展升级。
 */
/**
 * 应用 broker 推来的成员快照。
 *
 * 同时接受新格式(members,带元数据)和旧格式(peers,只有名字数组),
 * 因为 broker 可能先于扩展升级。
 *
 * 字段名映射:broker 的 wire 字段是 `tags`(历史遗留),而扩展内部和
 * 用户面向的词汇是 `labels`。这个接缝在这里收,上层看到的永远是 labels。
 * 曾经因为没收这个接缝,标签在成员列表里静默丢失过。
 */
export function applyRoster(s, body) {
  /** @type {Member[]} */
  let list;
  if (Array.isArray(body?.members)) {
    list = body.members.map((m) => ({
      name: m.name,
      host: m.host ?? null,
      addr: m.addr ?? null,
      labels: m.labels ?? m.tags ?? [], // 兼容两种字段名
      since: m.since ?? 0,
    }));
  } else if (Array.isArray(body?.peers)) {
    list = body.peers.map((name) => ({ name, host: null, addr: null, labels: [], since: 0 }));
  } else {
    return false;
  }
  s.members = list.filter((m) => m && typeof m.name === "string" && m.name !== s.self);
  return true;
}

// ---------------------------------------------------------------- 收件人解析

/**
 * 把用户输入或工具参数解析成收件人表达式。
 *
 *   "laptop"          → "laptop"
 *   "all" / "*"       → "*"
 *   "@web" / "#web"   → "@web"
 *   "a,b,@web"        → ["a","b","@web"]
 *   (空)              → "@default"   默认组
 */
export function parseRecipients(raw) {
  const t = String(raw ?? "").trim();
  if (!t) return "@default";
  if (t === "all" || t === "*") return "*";

  const parts = t
    .split(",")
    .map((x) => {
      const v = x.trim();
      if (!v) return null;
      if (v === "all" || v === "*") return "*";
      if (v.startsWith("#")) return `@${v.slice(1)}`; // 兼容旧写法
      return v;
    })
    .filter(Boolean);

  if (parts.length === 0) return "@default";
  if (parts.length === 1) return parts[0];
  return parts;
}

/**
 * 本地计算某个收件人表达式会命中几个成员。
 *
 * 为什么本地算:群发前要确认"这真的会发给 5 个人以上吗",而确认对话框
 * 必须在发送前弹出。broker 的回执是发送后才知道的,来不及。
 *
 * `@default` 视为全员(见 group 模型:不指定收件人 = 默认组 = 全体)。
 */
export function resolveLocal(s, to) {
  const requested = Array.isArray(to) ? to : [to];
  const targets = new Set();
  const unknown = [];
  const mine = others(s);

  for (const raw of requested) {
    if (typeof raw !== "string") continue;
    const t = raw.trim();
    if (!t) continue;

    if (t === "*" || t === "@default") {
      for (const m of mine) targets.add(m.name);
      continue;
    }
    if (t.startsWith("@")) {
      const label = t.slice(1);
      let hit = 0;
      for (const m of mine) {
        if ((m.labels ?? []).includes(label)) {
          targets.add(m.name);
          hit++;
        }
      }
      if (hit === 0) unknown.push(t);
      continue;
    }
    if (!mine.some((m) => m.name === t)) {
      unknown.push(t);
      continue;
    }
    targets.add(t);
  }

  return { targets: [...targets], unknown };
}

// ---------------------------------------------------------------- 出站

/**
 * 记录一条我们发出的消息,并产出动作。
 *
 * origin 是要紧的:对方回复时,靠它判断"模型知道这回事吗"。
 * 用户用 /team send 发的消息,模型完全不知道,不该被叫醒来疑惑。
 */
export function sendMessage(s, { to, text, hops = 0, re = null, origin = "user", fyi = false }) {
  const targets = resolveLocal(s, to);
  if (targets.targets.length === 0) {
    return [
      {
        type: "notify",
        level: "warning",
        message: targets.unknown.length
          ? `没有匹配的收件人(${targets.unknown.join(",")})`
          : "没有其他节点在线",
      },
    ];
  }

  const id = newId();
  rememberBounded(s.outbound, id, { text, origin, to });

  return [
    { type: "send", to, text, hops, re, origin, fyi, id, targets: targets.targets },
    { type: "card", kind: "send", peer: formatTarget(to), text },
  ];
}

function formatTarget(to) {
  if (Array.isArray(to)) return to.join(",");
  return to === "*" ? "全员" : to === "@default" ? "默认组" : to.replace(/^@/, "#");
}

// ---------------------------------------------------------------- 入站

/**
 * 分类一条入站消息。这是对话形状的唯一权威。
 *
 * 规则(每一条都对应一次真实故障):
 *   - 请求            → 注入,自动回信一次
 *   - 回复我们的消息   → 注入,**不回信**(否则请求→回复→回复…打到跳数上限)
 *   - 回复用户的消息   → 只显示卡片(模型没见过那条消息,叫醒它只会说"正文是空的")
 *   - 回复但原消息未知 → 注入,不回信
 *   - fyi 广播        → 只显示卡片(announce=always 的镜像推送,不该叫醒模型)
 */
export function classifyInbound(s, env) {
  const body = env.body ?? {};
  const text = typeof body.text === "string" ? body.text : "";
  if (!text.trim()) return { action: "drop", reason: "empty" };

  const hops = typeof body.hops === "number" ? body.hops : 0;
  if (hops >= MAX_HOPS) return { action: "drop", reason: "hops" };

  if (body.fyi === true) return { action: "card", kind: "fyi", autoReply: false };

  if (env.re) {
    const orig = s.outbound.get(env.re) ?? null;
    if (orig?.origin === "user") {
      return { action: "card", kind: "reply", autoReply: false, original: orig.text };
    }
    return { action: "inject", kind: "reply", autoReply: false, original: orig?.text ?? null };
  }

  return { action: "inject", kind: "request", autoReply: true };
}

/** 构造注入给模型的文本。请求和回复的措辞必须不同,见 classifyInbound 注释。 */
export function buildPayload(from, text, cls) {
  if (cls.kind === "reply") {
    const quote = cls.original ? `你之前发给它的消息「${excerpt(cls.original, 120)}」` : "你之前发出的消息";
    return (
      `[来自 ${from} 的 team 回复]\n${text}\n\n---\n` +
      `上面是 teammate ${from} 对${quote}的回复(不是真人用户在打字)。` +
      `你这一轮的输出【不会】自动回传给 ${from}。` +
      `如果需要继续和它对话,显式调用 team_send;否则直接处理这条回复即可。`
    );
  }
  return (
    `[来自 ${from} 的 team 消息]\n${text}\n\n---\n` +
    `上面是 teammate ${from} 发来的消息原文(不是真人用户在打字)。` +
    `按内容本身的意思回应:是任务就执行,是讨论/诗句/提问就接着往下走。` +
    `不要反问"需要我做什么",也不要复述确认。` +
    `你这一轮的最终输出会自动原样回传给 ${from}。`
  );
}

export function excerpt(text, max = 120) {
  const one = String(text ?? "").replace(/\s+/g, " ").trim();
  return one.length > max ? `${one.slice(0, max)}…` : one;
}

/**
 * 处理一条入站信封,产出动作列表。
 *
 * @returns {Action[]}
 */
export function handleIncoming(s, env, now = Date.now()) {
  const body = env.body ?? {};
  const kind = typeof body.kind === "string" ? body.kind : undefined;

  // ---- broker 控制消息
  if (env.from === "broker") {
    switch (kind) {
      case "welcome":
      case "peer_joined":
      case "peer_left": {
        const changed = applyRoster(s, body);
        const actions = changed ? [{ type: "status" }] : [];
        if (kind === "peer_joined" && typeof body.peer === "string" && body.peer !== s.self) {
          const m = others(s).find((x) => x.name === body.peer);
          actions.push({
            type: "notify",
            level: "info",
            message: `team:${body.peer} 上线${m?.host ? ` (${m.host})` : ""}`,
          });
        }
        return actions;
      }

      case "undeliverable":
        return [
          {
            type: "notify",
            level: "warning",
            message: `team:发给 ${String(body.to ?? "?")} 失败 —— ${describeUndeliverable(body)}`,
          },
        ];

      case "delivered": {
        // 部分成功也要说,否则用户以为"发出去了"就全到了
        const failed = Array.isArray(body.failed) ? body.failed : [];
        const unknown = Array.isArray(body.unknown) ? body.unknown : [];
        if (!failed.length && !unknown.length) return [];
        const parts = [`投递 ${body.delivered}/${body.total}`];
        if (failed.length) parts.push(`写入失败:${failed.join(",")}`);
        if (unknown.length) parts.push(`未知:${unknown.join(",")}`);
        return [{ type: "notify", level: "warning", message: `team:${parts.join(" · ")}` }];
      }

      case "ping":
        return [];
    }
    return [];
  }

  // ---- 团队消息
  if (s.seen.has(env.id)) return [];
  remember(s.seen, env.id);

  if (s.injected.has(env.id)) return [];
  remember(s.injected, env.id);

  const cls = classifyInbound(s, env);
  if (cls.action === "drop") return [];

  const text = typeof body.text === "string" ? body.text : "";

  if (cls.action === "card") {
    return [
      {
        type: "card",
        kind: cls.kind === "fyi" ? "send" : "receive",
        peer: env.from,
        text,
        ...(cls.original ? { reason: `回复:${excerpt(cls.original, 60)}` } : {}),
      },
    ];
  }

  const hops = typeof body.hops === "number" ? body.hops : 0;
  if (cls.autoReply) {
    // 只有一个待回复槽。并发请求时后到的覆盖先到的 —— 这是已知缺陷,
    // 见 roadmap 第 10 节"Known limitation"。抽出来测是为了让它的
    // 行为明确,而不是偶尔被发现。
    s.pendingReply = { to: env.from, hops, re: env.id };
  }

  return [
    { type: "card", kind: "receive", peer: env.from, text },
    { type: "inject", payload: buildPayload(env.from, text, cls), from: env.from },
  ];
}

function describeUndeliverable(body) {
  const unknown = Array.isArray(body.unknown) ? body.unknown : [];
  if (unknown.length && unknown.every((u) => String(u).startsWith("@"))) {
    return `分组 ${unknown.join(",")} 里没有在线节点`;
  }
  if (unknown.length) return `找不到 ${unknown.join(",")}(不在线或名字写错)`;
  if (body.reason === "unknown_recipient") return "收件人不在线或不存在";
  if (body.reason === "no_recipients") return "没有可投递的对象";
  return String(body.reason ?? "未知原因");
}

// ---------------------------------------------------------------- 出站决策

/**
 * 轮次结束后决定是否推送、推给谁。产出动作。
 *
 * 用 agent_settled 触发,不用 agent_end:后者之后还可能有重试、
 * compaction、queued continuation,拿它当"结束"会推中间态。
 */
export function onTurnSettled(s) {
  const text = s.lastText;
  if (!text.trim()) return [];

  if (s.announce === "always") {
    const list = others(s);
    if (list.length === 0) return [];
    s.lastText = "";
    // fyi:true 让收件人只显示卡片,不叫醒它的模型。否则 N 个节点都开
    // always 时,每轮都会触发 N-1 轮新思考。
    return list.map((m) => ({
      type: "send",
      to: m.name,
      text,
      hops: 1,
      re: null,
      origin: "model",
      fyi: true,
      id: newId(),
      targets: [m.name],
      card: { kind: "send", peer: m.name, text },
    }));
  }

  if (s.announce === "auto") {
    const reply = s.pendingReply;
    s.pendingReply = null;
    if (!reply) return [];

    s.lastText = "";
    const hops = reply.hops + 1;

    // 对方可能在回信之前就下线了。这一轮仍值得留住文本,但不能假装发成功。
    if (!others(s).some((m) => m.name === reply.to)) {
      return [
        { type: "card", kind: "failed", peer: reply.to, text, reason: `${reply.to} 已离线,回复没有送出` },
      ];
    }

    if (hops >= MAX_HOPS) {
      return [
        {
          type: "send",
          to: reply.to,
          text,
          hops,
          re: reply.re,
          origin: "model",
          fyi: false,
          id: newId(),
          targets: [reply.to],
          card: { kind: "send", peer: reply.to, text, reason: `已达跳数上限 ${MAX_HOPS},对端不会再回传` },
        },
      ];
    }

    return [
      {
        type: "send",
        to: reply.to,
        text,
        hops,
        re: reply.re,
        origin: "model",
        fyi: false,
        id: newId(),
        targets: [reply.to],
        card: { kind: "reply", peer: reply.to, text },
      },
    ];
  }

  // off:清掉待回复,什么都不发
  s.pendingReply = null;
  return [];
}

/** 从 assistant 消息里取文本 */
export function extractText(content) {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .filter((c) => c?.type === "text")
      .map((c) => c.text ?? "")
      .join("");
  }
  return "";
}

export { MAX_HOPS, MAX_LABELS };
