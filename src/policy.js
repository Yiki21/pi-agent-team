/**
 * 入站消息处理策略 —— 纯函数,不依赖 Pi,可单测。
 *
 * 为什么单独抽出来:这是整个扩展里最容易出"对话形状"错误的地方。
 * 实测踩过的坑:auto 模式对"回复"也自动回信,导致请求→回复→回复的
 * 回复……一路打转到跳数上限。跳数上限是安全网,不应该承担对话形状。
 *
 * 规则:
 *   请求(re 为空)                 → 注入模型,本轮输出自动回传(带 re)
 *   回复,原消息由模型发出           → 注入模型并附上原问题,不自动回传
 *   回复,原消息由用户手动发出       → 只显示卡片,不打扰模型
 *   回复,原消息查不到(重启后等)   → 注入模型,不自动回传
 *   fyi 广播(announce=always)      → 只显示卡片
 */

/** 跳数上限:最后一道防线,正常对话形状不应碰到它。 */
export const MAX_HOPS = 4;

/**
 * @typedef {{ text: string, origin: "user" | "model", to: string | string[] }} Outbound
 *
 * @typedef {{
 *   action: "drop" | "card" | "inject",
 *   kind?: "request" | "reply" | "fyi",
 *   autoReply?: boolean,
 *   original?: string | null,
 *   reason?: string,
 * }} Classification
 */

/**
 * 决定一条入站团队消息怎么处理。
 *
 * @param {{ from: string, id: string, re: string | null, body: Record<string, unknown> }} env
 * @param {Map<string, Outbound>} outbound  我们发出过的消息,按 id 索引
 * @returns {Classification}
 */
export function classifyInbound(env, outbound) {
  const body = env.body ?? {};
  const text = typeof body.text === "string" ? body.text : "";
  if (!text.trim()) return { action: "drop", reason: "empty" };

  const hops = typeof body.hops === "number" ? body.hops : 0;
  if (hops >= MAX_HOPS) return { action: "drop", reason: "hops" };

  // announce=always 的镜像推送:给人看的,不叫醒模型。
  // 否则 N 个节点每轮都互相触发 N-1 轮思考。
  if (body.fyi === true) return { action: "card", kind: "fyi", autoReply: false };

  if (env.re) {
    const orig = outbound.get(env.re) ?? null;
    if (orig?.origin === "user") {
      return { action: "card", kind: "reply", autoReply: false, original: orig.text };
    }
    return { action: "inject", kind: "reply", autoReply: false, original: orig?.text ?? null };
  }

  return { action: "inject", kind: "request", autoReply: true };
}

/** 截断长文本,用于在提示和卡片里引用原消息 */
export function excerpt(text, max = 120) {
  const one = String(text ?? "").replace(/\s+/g, " ").trim();
  return one.length > max ? `${one.slice(0, max)}…` : one;
}

/**
 * 构造注入给模型的文本。
 *
 * 请求和回复的措辞必须不同:模型需要知道"这一轮输出会不会回传",
 * 否则它无法决定是直接作答,还是需要显式调用 team_send 继续对话。
 *
 * @param {string} from
 * @param {string} text
 * @param {Classification} cls
 */
export function buildPayload(from, text, cls) {
  if (cls.kind === "reply") {
    const quote = cls.original ? `你之前发给它的消息「${excerpt(cls.original)}」` : "你之前发出的消息";
    return (
      `[来自 ${from} 的 team 回复]\n` +
      `${text}\n\n` +
      `---\n` +
      `上面是 teammate ${from} 对${quote}的回复(不是真人用户在打字)。` +
      `你这一轮的输出【不会】自动回传给 ${from}。` +
      `如果需要继续和它对话,显式调用 team_send;否则直接处理这条回复即可。`
    );
  }

  return (
    `[来自 ${from} 的 team 消息]\n` +
    `${text}\n\n` +
    `---\n` +
    `上面是 teammate ${from} 发来的消息原文(不是真人用户在打字)。` +
    `按内容本身的意思回应:是任务就执行,是讨论/诗句/提问就接着往下走。` +
    `不要反问"需要我做什么",也不要复述确认。` +
    `你这一轮的最终输出会自动原样回传给 ${from}。`
  );
}

/**
 * 有上限的 Map:超过容量丢最早的条目。
 * outbound 表不能无限增长 —— 长时间运行的 TUI 会话可能发出上千条。
 */
export function rememberBounded(map, key, value, cap = 1000) {
  map.set(key, value);
  if (map.size > cap) {
    const oldest = map.keys().next().value;
    if (oldest !== undefined) map.delete(oldest);
  }
}
