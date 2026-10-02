/**
 * 订阅状态机 —— 纯逻辑,不依赖 Pi、不依赖网络。
 *
 * ── 这东西是什么 ──
 * "X 每轮结束时说了一句什么,我也想知道" —— 而 X 不需要做任何特别的事,
 * 我也不需要收到 X 收到的每一条消息。
 *
 * ── 为什么不是已有的那两个机制 ──
 *   reply=mirror      每一轮输出都镜像给所有人。要的是"特定一个人"时做不到,
 *                     且 X 端必须主动配置。
 *   reply=remind      只覆盖"我发出去的请求有没有被回",和 X 自己说什么无关。
 * 订阅是唯一能表达"被动地看某个特定对端的输出"的形状。
 *
 * ── 方向是单向的 ──
 * 被订阅者不知道自己在被看,broker 也不会给他任何回执 —— 回执本身就是泄露。
 * 所以订阅的登记只发生在订阅者这一侧。
 *
 * ── 循环保护是结构性的 ──
 * 收到 watch_notify 只产生一张卡片,不产生 inject。所以两个互相订阅的节点
 * A→B、B→A 不会 ping-pong:A 输出 → B 收到卡片 → B 的模型不跑 → 没有下一步。
 *
 * session.js 里的 MAX_HOPS 管不到这里:那条上限只在 classifyInbound 里生效,
 * 而 watch_notify 根本不走那条路径。这也是为什么通知帧必须是
 * from="broker" 且 re=null —— 一旦带上 re,它会被当成一次回复注入,
 * 循环就从深度 1 变成无限。改动这里之前先读这段。
 */

/** 一个节点最多订阅多少个目标,和 broker 侧保持一致 */
export const MAX_WATCHES = 8;

/** 摘要展示上限,和 broker 侧的截断一致 */
export const SUMMARY_LIMIT = 200;

/** 订阅者 → 订阅的目标集合 */
export function createWatchState() {
  return { targets: new Set() };
}

/**
 * 应用 broker 的 watch_ack / watch_error。
 *
 * 返回 { lines, level } 供上层展示。broker 是权威:本地只在收到 ack 后
 * 才改目标集合,避免"界面说订阅了但 broker 没登记"。
 */
export function applyWatchAck(watch, body) {
  const kind = body?.kind;

  if (kind === "watch_error") {
    const reason = body.reason;
    const text =
      reason === "limit"
        ? `订阅数已达上限 ${body.limit ?? MAX_WATCHES},先移除一个再试`
        : reason === "self"
          ? "不能订阅自己"
          : reason === "bad_target"
            ? `不是合法的节点名:${body.target ?? "?"}`
            : reason === "empty"
              ? "这一轮没有输出可发布"
              : reason === "bad_action"
                ? `认不出的订阅动作 "${body.action ?? "?"}"`
                : `订阅失败:${reason ?? "未知原因"}`;
    return { lines: [text], level: "warning" };
  }

  if (kind === "watch_none") {
    // 没人在听。不是错误,但必须说出来 —— 否则模型以为有人会收到。
    return { lines: ["现在没有人订阅你,这条没有送出去"], level: "info" };
  }

  if (kind !== "watch_ack") return null;

  if (body.action === "list") {
    const list = Array.isArray(body.watches) ? body.watches : [];
    return {
      lines: list.length ? [`正在订阅:${list.join(", ")}`] : ["没有订阅任何人"],
      level: "info",
      watches: list,
    };
  }

  if (body.action === "add" && body.target) {
    watch.targets.add(body.target);
    return { lines: [`已订阅 ${body.target} 的回答`], level: "info" };
  }

  if (body.action === "remove" && body.target) {
    watch.targets.delete(body.target);
    return {
      lines: [
        body.state === "absent" ? `本来就没有订阅 ${body.target}` : `已取消订阅 ${body.target}`,
      ],
      level: "info",
    };
  }

  return null;
}

/**
 * 把一条 watch_notify 变成给人看的文字。
 *
 * 返回 null 表示这不是一条订阅通知,调用方应当继续走正常路径。
 */
export function formatWatchNotify(env) {
  const body = env?.body;
  if (!body || body.kind !== "watch_notify") return null;
  if (env.from !== "broker") return null;

  const target = body.target ?? "?";
  const summary = typeof body.summary === "string" ? body.summary : "";
  const lines = [`[订阅] ${target}:${summary}`];

  if (typeof body.overflow === "number" && body.overflow > 0) {
    lines.push(`(上面这几条之间还有 ${body.overflow} 条被合并了)`);
  }
  if (typeof body.fullLength === "number") {
    lines.push(`(原文 ${body.fullLength} 字,这里只截了前 ${SUMMARY_LIMIT} 字;要全文就 team_send 问它)`);
  }

  return { peer: target, text: lines.join("\n") };
}

/**
 * 本地校验,给早失败用(真正的权威是 broker)。
 * 返回错误字符串,或 null 表示看起来没问题。
 */
export function validateWatchTarget(raw, self) {
  const v = String(raw ?? "").trim();
  if (!v) return "要订阅谁?给一个节点名";
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$/.test(v)) return `不是合法的节点名:${v}`;
  if (v === self) return "不能订阅自己";
  return null;
}

/** 状态栏/系统提示用的一行 */
export function describeWatches(watch) {
  const list = [...watch.targets].sort();
  return list.length ? list.join(", ") : "";
}
