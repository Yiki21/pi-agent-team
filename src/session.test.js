/**
 * Team 会话状态机测试。跑:node --test src/
 *
 * 重点覆盖 roadmap 第 9 节列出的三个测试缺口。它们之前只靠真机手测,
 * 而现在能纯函数测 —— 这正是把状态抽出来的目的:
 *
 *   1. broker 重启后重连(roster 重建)
 *   2. 对端在"请求发出"和"回复到达"之间离线
 *   3. 一轮内两个请求到达(并发请求各自收到自己的回答)
 *
 * 最后一组是对话形状回归,包括一个复现旧 bug 的对照组。
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  MAX_HOPS,
  TEAM_MESSAGE_TYPE,
  applyRoster,
  bindReply,
  buildPayload,
  classifyInbound,
  createSessionState,
  excerpt,
  extractText,
  handleIncoming,
  knownLabels,
  observeMessage,
  onTurnSettled,
  others,
  parseRecipients,
  rememberPending,
  resolveLocal,
  sendMessage,
  teamSize,
} from "./session.js";

// ---------------------------------------------------------------- 辅助

const member = (name, over = {}) => ({
  name,
  host: over.host ?? null,
  addr: over.addr ?? null,
  labels: over.labels ?? [],
  since: over.since ?? 0,
});

/** 造一个已连上、roster 已填充的会话 */
function session(self = "me", peers = [member("peer")], over = {}) {
  const s = createSessionState(self);
  applyRoster(s, { members: [member(self), ...peers] });
  Object.assign(s, over);
  return s;
}

const types = (actions) => actions.map((a) => a.type);

// ================================================================ 成员视图

test("roster:排除自己,teamSize 算上自己", () => {
  const s = createSessionState("me");
  applyRoster(s, { members: [member("me"), member("a"), member("b")] });

  assert.deepEqual(others(s).map((m) => m.name), ["a", "b"]);
  assert.equal(teamSize(s), 3, "3 个节点的团队应显示 3");
});

test("roster:单独在线时 teamSize 为 1", () => {
  const s = createSessionState("me");
  applyRoster(s, { members: [member("me")] });
  assert.equal(teamSize(s), 1);
});

test("roster:兼容旧格式 peers(只有名字)", () => {
  const s = createSessionState("me");
  assert.equal(applyRoster(s, { peers: ["me", "old-node"] }), true);
  assert.deepEqual(others(s).map((m) => m.name), ["old-node"]);
  assert.deepEqual(others(s)[0].labels, [], "旧格式没有 label,应为空数组而不是 undefined");
});

test("roster:两种格式都缺时返回 false,不破坏已有视图", () => {
  const s = session();
  const before = others(s).map((m) => m.name);
  assert.equal(applyRoster(s, {}), false);
  assert.equal(applyRoster(s, { members: "not-an-array" }), false);
  assert.deepEqual(others(s).map((m) => m.name), before);
});

test("knownLabels:汇总他人的 label,不含自己的", () => {
  const s = session("me", [member("a", { labels: ["web"] }), member("b", { labels: ["web", "db"] })]);
  assert.deepEqual(knownLabels(s), ["db", "web"]);
});

// ================================================================ 缺口 1:broker 重启

test("缺口 1:broker 重启后 welcome 重建 roster", () => {
  const s = session("me", [member("old-peer")]);
  rememberPending(s, { from: "old-peer", id: "r", hops: 0, ref: "p" });

  // broker 重启:连接断开 → 客户端清空视图 → 重连收到新 welcome
  s.members = [];
  assert.equal(teamSize(s), 1, "断连后只应剩自己");

  const actions = handleIncoming(s, {
    from: "broker",
    id: "sys-1",
    re: null,
    body: {
      kind: "welcome",
      peer: "me",
      members: [member("me"), member("new-peer", { host: "dev01" })],
    },
  });

  assert.deepEqual(others(s).map((m) => m.name), ["new-peer"], "roster 应重建");
  assert.equal(teamSize(s), 2);
  assert.ok(types(actions).includes("status"), "应产出 status 让状态栏刷新");
});

test("缺口 1:重启后到达的回复,原消息未知 → 注入但不自动回信", () => {
  // 这是真实场景:broker 重启期间对方发的回复到了,但我们的
  // outbound 表里查不到那条 re 指向的消息(表在内存里,没持久化)。
  const s = session();
  s.outbound.clear();

  const cls = classifyInbound(s, { from: "peer", id: "r1", re: "unknown-id", body: { text: "答复" } });
  assert.equal(cls.action, "inject");
  assert.equal(cls.kind, "reply");
  assert.equal(cls.autoReply, false, "原消息未知时不该自动回信,否则可能形成新环路");
  assert.equal(cls.original, null);
});

test("缺口 1:重启后待回复失效,settled 时不误发", () => {
  // 重启后 roster 是空的,而队列里还留着一条来自已消失节点的请求。
  const s = session("me", []);
  rememberPending(s, { from: "gone-peer", id: "r", hops: 0, ref: "p" });
  s.pendingReplies[0].seen = true;

  const actions = onTurnSettled(s);
  assert.equal(actions[0].type, "card");
  assert.equal(actions[0].kind, "failed", "对端已不在 roster,不能假装提醒送到了");
  assert.match(actions[0].reason, /已离线/);
  assert.equal(s.pendingReplies.length, 0);
});

// ================================================================ 缺口 2:对端中途离线

test("缺口 2:对端离线时,它的待回复请求出 fail 卡片", () => {
  const s = session("me", [member("peer")]);
  rememberPending(s, { from: "peer", id: "req-1", hops: 0, ref: "payload" });
  s.pendingReplies[0].seen = true; // 模型看到过

  applyRoster(s, { members: [member("me")] }); // 对端掉线

  const actions = onTurnSettled(s);
  assert.equal(actions.length, 1);
  assert.equal(actions[0].type, "card");
  assert.equal(actions[0].kind, "failed");
  assert.match(actions[0].reason, /peer 已离线/);
  assert.equal(s.pendingReplies.length, 0, "离线的不该继续留在队列里");
});

test("缺口 2:看到过但没回复 → 提醒一次,带上 re 原文", () => {
  const s = session("me", [member("peer")]);
  rememberPending(s, { from: "peer", id: "req-1", hops: 1, ref: "payload-原文" });
  s.pendingReplies[0].seen = true;

  const first = onTurnSettled(s);
  assert.equal(first.length, 1);
  assert.equal(first[0].type, "remind", "应产出提醒");
  assert.deepEqual(first[0].pending, ["peer"]);
  assert.equal(first[0].ref, "payload-原文", "提醒要把原请求带上");
  assert.equal(s.pendingReplies.length, 1, "提醒过还不算回复");

  // 再 settle:已经提醒过,不再重复打扰
  const second = onTurnSettled(s);
  assert.equal(second.length, 1);
  assert.equal(second[0].type, "card");
  assert.equal(second[0].kind, "failed", "两次都不回就本机说清楚");
  assert.match(second[0].reason, /仍未被回复/);
});

test("缺口 2:模型还没看到时不提醒 —— 它会被排进下一个 turn", () => {
  const s = session("me", [member("peer")]);
  rememberPending(s, { from: "peer", id: "req-1", hops: 0, ref: "payload" });
  // seen 仍是 false:忙时送达、还没轮到模型看

  const actions = onTurnSettled(s);
  assert.deepEqual(actions, [], "没看到就不该催,下一轮它会自己触发");
  assert.equal(s.pendingReplies.length, 1, "仍留在队列里");
});

test("缺口 2:没有待回复时 settled 不发任何东西", () => {
  const s = session();
  s.lastText = "用户问的问题我自己答的";
  assert.deepEqual(onTurnSettled(s), [], "用户手动提问的轮次不该推给任何人");
});

// ================================================================ 显式回复

test("bindReply:回给有待回复请求的队友 → 带上原来的 re,跳数 +1", () => {
  const s = session("me", [member("peer")]);
  rememberPending(s, { from: "peer", id: "req-9", hops: 1, ref: "p" });

  const r = bindReply(s, ["peer"]);
  assert.equal(r.replyTo, "peer");
  assert.equal(r.re, "req-9", "不带 re 的话对端会当成新请求,两个 agent 会互相触发下去");
  assert.equal(r.hops, 2);
  assert.equal(s.pendingReplies.length, 0, "回一次就算答完,从队列移除");
});

test("bindReply:回给别人 / 群发 → 不绑定", () => {
  const s = session("me", [member("peer"), member("other")]);
  rememberPending(s, { from: "peer", id: "req-9", hops: 0, ref: "p" });

  assert.equal(bindReply(s, ["other"]).re, null, "回给别人不是回复 peer 的请求");
  assert.equal(bindReply(s, ["peer", "other"]).re, null, "群发不该被当成某一条的回复");
  assert.equal(s.pendingReplies.length, 1, "没绑上就不该动队列");
});

test("bindReply:跳数到上限时封顶,不越界", () => {
  const s = session("me", [member("peer")]);
  rememberPending(s, { from: "peer", id: "r", hops: MAX_HOPS - 1, ref: "p" });
  assert.equal(bindReply(s, ["peer"]).hops, MAX_HOPS);
});

test("bindReply:没有待回复时原样返回,不报错", () => {
  const s = session("me", [member("peer")]);
  const r = bindReply(s, ["peer"]);
  assert.equal(r.replyTo, null);
  assert.equal(r.re, null);
  assert.equal(r.hops, 0);
});

test("observeMessage:自定义消息按 customType 认出来并标记 seen", () => {
  const s = session("me", [member("peer")]);
  rememberPending(s, { from: "peer", id: "req-1", hops: 0, ref: "队友消息原文" });
  assert.equal(s.pendingReplies[0].seen, false);

  // 队友消息到达时 role 是 "custom",不是 "user" —— 实测的形状
  observeMessage(s, "custom", "队友消息原文", TEAM_MESSAGE_TYPE);
  assert.equal(s.pendingReplies[0].seen, true, "确认模型看到了它");

  // 别的 customType 不该误伤
  const s2 = session("me", [member("peer")]);
  rememberPending(s2, { from: "peer", id: "r", hops: 0, ref: "x" });
  observeMessage(s2, "custom", "x", "subagent-notify");
  assert.equal(s2.pendingReplies[0].seen, false, "别人的自定义消息不该算队友消息");
});

test("observeMessage:assistant 文本记进 lastText,供 announce=always 用", () => {
  const s = session();
  observeMessage(s, "assistant", "这一轮的输出");
  assert.equal(s.lastText, "这一轮的输出");
});

test("缺口 3:重复投递同一 id 只注入一次", () => {
  const s = session();
  const env = { from: "peer", id: "dup-1", re: null, body: { text: "只应出现一次" } };

  const a1 = handleIncoming(s, env);
  const a2 = handleIncoming(s, env);

  assert.equal(a1.filter((x) => x.type === "inject").length, 1);
  assert.deepEqual(a2, [], "第二次投递应完全被忽略");
});

// ================================================================ 收件人解析

test("parseRecipients:各种写法", () => {
  assert.equal(parseRecipients("laptop"), "laptop");
  assert.equal(parseRecipients("all"), "*");
  assert.equal(parseRecipients("*"), "*");
  assert.equal(parseRecipients("@web"), "@web");
  assert.equal(parseRecipients("#web"), "@web", "旧 # 写法应兼容成 @");
  assert.deepEqual(parseRecipients("a,b"), ["a", "b"]);
  assert.deepEqual(parseRecipients("a, #web"), ["a", "@web"]);
  assert.equal(parseRecipients(""), "@default", "空输入 = 默认组");
  assert.equal(parseRecipients("   "), "@default");
});

test("resolveLocal:@default 和 * 都指全员", () => {
  const s = session("me", [member("a"), member("b")]);
  assert.deepEqual(resolveLocal(s, "@default").targets.sort(), ["a", "b"]);
  assert.deepEqual(resolveLocal(s, "*").targets.sort(), ["a", "b"]);
});

test("resolveLocal:@label 命中且排除自己", () => {
  const s = session("me", [
    member("a", { labels: ["web"] }),
    member("b", { labels: ["web"] }),
    member("c", { labels: ["db"] }),
  ]);
  const r = resolveLocal(s, "@web");
  assert.deepEqual(r.targets.sort(), ["a", "b"]);
  assert.deepEqual(r.unknown, []);
});

test("resolveLocal:空 label 组报 unknown,不静默成功", () => {
  const s = session("me", [member("a", { labels: ["web"] })]);
  const r = resolveLocal(s, "@nobody");
  assert.deepEqual(r.targets, []);
  assert.deepEqual(r.unknown, ["@nobody"]);
});

test("resolveLocal:并集去重", () => {
  const s = session("me", [member("a", { labels: ["web"] }), member("b", { labels: ["db"] })]);
  const r = resolveLocal(s, ["@web", "a", "b"]);
  assert.deepEqual(r.targets.sort(), ["a", "b"], "a 同时被 @web 和显式点名,只应算一次");
});

test("sendMessage:无匹配收件人时只出警告,不发送", () => {
  const s = session("me", []);
  const actions = sendMessage(s, { to: "@nobody", text: "x" });
  assert.deepEqual(types(actions), ["notify"]);
  assert.equal(actions[0].level, "warning");
});

test("sendMessage:记录 origin 供日后判断,并产出 send + card", () => {
  const s = session("me", [member("peer")]);
  const actions = sendMessage(s, { to: "peer", text: "你好", origin: "user" });

  assert.deepEqual(types(actions), ["send", "card"]);
  const id = actions[0].id;
  assert.equal(s.outbound.get(id).origin, "user");
  assert.equal(actions[1].kind, "send");
});

// ================================================================ 对话形状

test("对话形状:我们发出的消息被别人回复 → 注入、不回信", () => {
  const s = session();
  const [sent] = sendMessage(s, { to: "peer", text: "原始提问", origin: "model" });
  const id = sent.id;

  const cls = classifyInbound(s, { from: "peer", id: "r-1", re: id, body: { text: "答复" } });
  assert.equal(cls.action, "inject");
  assert.equal(cls.autoReply, false);
  assert.equal(cls.original, "原始提问");

  const actions = handleIncoming(s, { from: "peer", id: "r-1", re: id, body: { text: "答复" } });
  assert.equal(s.pendingReplies.length, 0, "回复不该设置待回复");
  assert.ok(types(actions).includes("inject"));
});

test("对话形状:用户 /team send 发出的消息被回复 → 只显示卡片", () => {
  // 注意:这里必须用真实存在的收件人。用 @web 而 fixture 里没有 web label 的话,
  // sendMessage 只会返回一条 warning,拿不到真正发出的 id,测试就测错了对象。
  const s = session("me", [member("peer")]);
  const [sent] = sendMessage(s, { to: "peer", text: "用户手动问的", origin: "user" });
  assert.equal(sent.type, "send", "前置条件:一定要拿到真实的 send 动作");

  const actions = handleIncoming(s, { from: "peer", id: "r-2", re: sent.id, body: { text: "答复" } });

  assert.deepEqual(types(actions), ["card"], "不该注入 —— 模型没见过那条消息");
  assert.equal(actions[0].kind, "receive");
  assert.match(actions[0].reason, /回复:/);
  assert.match(actions[0].reason, /用户手动问的/);
});

test("对话形状:新请求 → 卡片 + 注入 + 设置待回复", () => {
  const s = session();
  const actions = handleIncoming(s, { from: "peer", id: "req-x", re: null, body: { text: "帮我跑测试", hops: 0 } });

  assert.deepEqual(types(actions), ["card", "inject"]);
  assert.match(actions[1].payload, /\[来自 peer 的 team 消息\]/);
  assert.equal(s.pendingReplies.length, 1);
  assert.equal(s.pendingReplies[0].re, "req-x", "re 指向入站消息 id,用于回信时闭合");
  assert.equal(
    s.pendingReplies[0].ref,
    actions[1].payload,
    "入队的 ref 必须和注入的 payload 完全一致,否则 message_end 里认不出它",
  );
});

test("对话形状:fyi 广播只出卡片", () => {
  const s = session();
  const actions = handleIncoming(s, { from: "peer", id: "fyi-1", re: null, body: { text: "状态", fyi: true } });

  assert.deepEqual(types(actions), ["card"]);
  assert.equal(s.pendingReplies.length, 0, "fyi 不该产生待回复");
  assert.equal(s.lastText, "", "fyi 不该写入待推文本");
});

test("对话形状:跳数到上限丢弃", () => {
  const s = session();
  const actions = handleIncoming(s, {
    from: "peer",
    id: "hop-1",
    re: null,
    body: { text: "太深了", hops: MAX_HOPS },
  });
  assert.deepEqual(actions, []);
});

test("对话形状:announce=always 推出 fyi,且不设待回复", () => {
  const s = session("me", [member("a"), member("b")], { announce: "always", lastText: "广播内容" });
  const actions = onTurnSettled(s);

  assert.equal(actions.length, 2);
  assert.ok(actions.every((a) => a.fyi === true), "fyi 让收件人只显示卡片,不叫醒它的模型");
  assert.ok(actions.every((a) => a.hops === 1));
});

test("对话形状:一来一回之后停下来,不会互相触发下去", () => {
  // 走完整链路:A 发请求 → B 收到(自定义消息)→ B 显式回复 → A 收到回复,
  // 到此为止。两个 agent 互相触发会一直打下去。
  const nodes = { A: session("A", [member("B")]), B: session("B", [member("A")]) };
  const wire = [];

  const first = sendMessage(nodes.A, { to: "B", text: "原始提问", origin: "model" });
  const sa = first.find((a) => a.type === "send");
  wire.push({ to: "B", env: { from: "A", id: sa.id, re: null, body: { text: "原始提问", hops: 0 } } });

  for (let i = 0; i < wire.length && i < 10; i++) {
    const msg = wire[i];
    const receiver = nodes[msg.to];
    const actions = handleIncoming(receiver, msg.env);

    const injected = actions.find((a) => a.type === "inject");
    if (!injected) break;

    // Pi 实际发出的事件:自定义消息 role="custom"
    observeMessage(receiver, "custom", injected.payload, TEAM_MESSAGE_TYPE);
    observeMessage(receiver, "assistant", `answer-${i}`);

    // 只有"请求"才需要回复。收到回复时不再回 —— 这才是对话能停下来的原因,
    // 而不是靠跳数上限兜住。
    const cls = classifyInbound(receiver, msg.env);
    if (!cls.autoReply) continue;

    const bound = bindReply(receiver, [receiver === nodes.B ? "A" : "B"]);
    wire.push({
      to: msg.env.from,
      env: {
        from: receiver.self,
        id: `out-${i}`,
        re: bound.re,
        // 跳数在 body 里,不在信封顶层 —— 契约就是这么定的
        body: { text: `answer-${i}`, hops: bound.hops },
      },
    });

    // settle 不该自己产生任何 send
    for (const a of onTurnSettled(receiver)) {
      assert.notEqual(a.type, "send", "settle 不再自动发消息");
    }
  }

  assert.equal(wire.length, 2, "请求 + 回复,共 2 条就该停");
  const reply = wire[1].env;
  assert.equal(reply.re, sa.id, "回复要带上原请求的 id,对端才认得出是回复");
  assert.equal(reply.body.hops, 1, "跳数随回复递增");

  // A 收到回复后不该再产生新的消息
  const aActions = handleIncoming(nodes.A, reply);
  assert.equal(aActions.some((a) => a.type === "send"), false, "收到回复后不该再自动回复");
  assert.equal(nodes.A.pendingReplies.length, 0, "回复不该进待回复队列");
});

test("一来一回:回复不会把接收方拖进新的待回复", () => {
  // 这是循环能否终止的关键。若回复也进队列,双方就会一直互相催下去。
  const s = session("A", [member("B")]);
  const out = sendMessage(s, { to: "B", text: "我方提问", origin: "model" });
  const id = out.find((a) => a.type === "send").id;

  handleIncoming(s, { from: "B", id: "reply-1", re: id, body: { text: "对方的答复", hops: 1 } });
  assert.equal(s.pendingReplies.length, 0, "收到回复不应要求我们再回复");
});

// ================================================================ 杂项

test("extractText:字符串与内容数组", () => {
  assert.equal(extractText("直接字符串"), "直接字符串");
  assert.equal(extractText([{ type: "text", text: "a" }, { type: "thinking", thinking: "x" }, { type: "text", text: "b" }]), "ab");
  assert.equal(extractText([]), "");
  assert.equal(extractText(null), "");
});

test("buildPayload:请求与回复措辞不同", () => {
  const req = buildPayload("p", "任务", { kind: "request" });
  assert.match(req, /【不会】自动回传/, "要明确说回传不是自动的 —— 以前说会,现在不会了");
  assert.match(req, /team_send\(\{ to: "p"/, "要告诉它具体怎么回复,收件人直接填好");

  const rep = buildPayload("p", "答复", { kind: "reply", original: "原来的问题" });
  assert.match(rep, /【不会】自动回传/);
  assert.match(rep, /原来的问题/);
});

test("excerpt:压缩空白并截断", () => {
  assert.equal(excerpt("a\n\n  b"), "a b");
  assert.equal(excerpt("x".repeat(200), 10), `${"x".repeat(10)}…`);
});

test("出站记录有上限,不无限增长", () => {
  const s = session("me", [member("peer")]);
  for (let i = 0; i < 1200; i++) sendMessage(s, { to: "peer", text: `m${i}`, origin: "model" });
  assert.ok(s.outbound.size <= 1000, `outbound 应被限制,实际 ${s.outbound.size}`);
});

// ---------------------------------------------------------------- 字段名接缝

test("roster:broker 的 tags 字段映射成 labels", () => {
  // 这是真机上踩过的坑:broker 发 tags,扩展读 labels,标签静默丢失。
  // 上层只看 labels,接缝在 applyRoster 收。
  const s = createSessionState("me");
  applyRoster(s, {
    members: [
      { name: "me", host: null, addr: null, labels: ["x"], since: 0 },
      { name: "peer", host: "dev01", addr: "1.2.3.4", tags: ["web", "fe"], since: 0 },
    ],
  });

  const p = others(s)[0];
  assert.deepEqual(p.labels, ["web", "fe"], "tags 应被映射成 labels");
  assert.equal(p.host, "dev01");
});

test("roster:同时有 labels 和 tags 时优先 labels", () => {
  const s = createSessionState("me");
  applyRoster(s, { members: [{ name: "peer", labels: ["new"], tags: ["old"], host: null, addr: null, since: 0 }] });
  assert.deepEqual(others(s)[0].labels, ["new"]);
});

test("roster:两者都缺时 labels 为空数组,不是 undefined", () => {
  const s = createSessionState("me");
  applyRoster(s, { members: [{ name: "peer", host: null, addr: null, since: 0 }] });
  assert.deepEqual(others(s)[0].labels, []);
});

test("@label 群发能命中经 tags 映射来的节点", () => {
  const s = createSessionState("me");
  applyRoster(s, {
    members: [
      { name: "me", labels: [], host: null, addr: null, since: 0 },
      { name: "a", tags: ["web"], host: null, addr: null, since: 0 },
      { name: "b", tags: ["web"], host: null, addr: null, since: 0 },
      { name: "c", tags: ["db"], host: null, addr: null, since: 0 },
    ],
  });
  assert.deepEqual(resolveLocal(s, "@web").targets.sort(), ["a", "b"]);
  assert.deepEqual(knownLabels(s), ["db", "web"]);
});

// ---------------------------------------------------------------- 意图契约

/**
 * send 意图的字段契约。
 *
 * 真机上出过一次:session.js 产出 { text },而 index.ts 读 { body },
 * 于是自动回信发出一个没有 body 的信封,broker 判为"畸形信封"直接丢弃。
 * 症状是"对方明明回信了但我收不到",很难从现象反推。
 *
 * 这组测试把契约钉在生产者一侧:send 意图必须在顶层带 text 和 hops。
 */
test("契约:auto 模式下 onTurnSettled 不再替模型发回信", () => {
  // 以前 settle 时把某段文本当成回复自动发出去。实测模型会在同一 run 里
  // 先回应队友、再做完原任务,按段落猜归属会把原任务的收尾发给队友。
  // 现在回复由模型显式调 team_send 发出,settle 只负责提醒。
  const s = session("me", [member("peer")]);
  rememberPending(s, { from: "peer", id: "req-1", hops: 0, ref: "p" });
  s.pendingReplies[0].seen = true;
  s.lastText = "这是原任务的收尾,不该被当成给 peer 的回复";

  const actions = onTurnSettled(s);
  assert.equal(actions.some((a) => a.type === "send"), false, "不能自动发送");
  assert.equal(actions[0].type, "remind");
});

test("契约:announce=always 的 send 意图同样带 text / hops / fyi", () => {
  const s = session("me", [member("a")], { announce: "always", lastText: "广播内容" });
  const [send] = onTurnSettled(s);

  assert.equal(send.type, "send");
  assert.equal(send.text, "广播内容");
  assert.equal(send.hops, 1);
  assert.equal(send.fyi, true);
  assert.ok(send.id);
});

test("契约:sendMessage 的 send 意图带 text / hops / id", () => {
  const s = session("me", [member("peer")]);
  const actions = sendMessage(s, { to: "peer", text: "手动发出", origin: "user" });
  const send = actions.find((a) => a.type === "send");

  assert.ok(send);
  assert.equal(send.text, "手动发出");
  assert.equal(send.hops, 0);
  assert.ok(send.id);
  assert.equal(send.re, null);
});

test("契约:所有 send 意图都不使用 body 字段(由调用方组装)", () => {
  const s = session("me", [member("peer")]);
  s.lastText = "x";

  const fromSettled = onTurnSettled({ ...s, announce: "always", lastText: "x" });
  const fromSend = sendMessage(s, { to: "peer", text: "y", origin: "user" });

  for (const a of [...fromSettled, ...fromSend].filter((x) => x.type === "send")) {
    assert.equal("body" in a, false, "send 意图不该自带 body —— 那是 index.ts 的职责");
  }
});
