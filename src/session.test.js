/**
 * Team 会话状态机测试。跑:node --test src/
 *
 * 重点覆盖 roadmap 第 9 节列出的三个测试缺口。它们之前只靠真机手测,
 * 而现在能纯函数测 —— 这正是把状态抽出来的目的:
 *
 *   1. broker 重启后重连(roster 重建)
 *   2. 对端在"请求发出"和"回复到达"之间离线
 *   3. 一轮内两个请求到达(已知缺陷:只有一个待回复槽)
 *
 * 最后一组是对话形状回归,包括一个复现旧 bug 的对照组。
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  MAX_HOPS,
  applyRoster,
  buildPayload,
  classifyInbound,
  createSessionState,
  excerpt,
  extractText,
  handleIncoming,
  knownLabels,
  onTurnSettled,
  others,
  parseRecipients,
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
  s.pendingReply = { to: "old-peer", hops: 0, re: "x" };

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

test("缺口 1:重启后 pendingReply 失效,settled 时不误发", () => {
  const s = session("me", []);
  s.pendingReply = { to: "gone-peer", hops: 0, re: "x" };
  s.lastText = "想回的答复";

  const actions = onTurnSettled(s);
  assert.equal(actions[0].type, "card");
  assert.equal(actions[0].kind, "failed", "对端已不在 roster,不能假装发送成功");
  assert.match(actions[0].reason, /已离线/);
});

// ================================================================ 缺口 2:对端中途离线

test("缺口 2:请求发出后对端离线,回信失败并出 fail 卡片", () => {
  const s = session("me", [member("peer")]);
  s.pendingReply = { to: "peer", hops: 0, re: "req-1" };
  s.lastText = "答复内容";

  // 对端掉线,roster 更新
  applyRoster(s, { members: [member("me")] });

  const actions = onTurnSettled(s);
  assert.equal(actions.length, 1);
  assert.equal(actions[0].type, "card");
  assert.equal(actions[0].kind, "failed");
  assert.match(actions[0].reason, /peer 已离线/);
  assert.equal(s.pendingReply, null, "失败后待回复槽应清空");
  assert.equal(s.lastText, "", "文本已消费,不该在下轮重复推");
});

test("缺口 2:对端在线时正常回信,带 re 且跳数 +1", () => {
  const s = session("me", [member("peer")]);
  s.pendingReply = { to: "peer", hops: 1, re: "req-1" };
  s.lastText = "答复";

  const [action] = onTurnSettled(s);
  assert.equal(action.type, "send");
  assert.equal(action.re, "req-1", "re 是闭合环路的关键,必须带上");
  assert.equal(action.hops, 2, "自动回信时跳数递增");
  assert.equal(action.origin, "model");
  assert.equal(action.card.kind, "reply");
});

test("缺口 2:没有待回复时 settled 不发任何东西", () => {
  const s = session();
  s.lastText = "用户问的问题我自己答的";
  assert.deepEqual(onTurnSettled(s), [], "用户手动提问的轮次不该推给任何人");
});

// ================================================================ 缺口 3:并发请求

test("缺口 3:一轮内两个请求,当前只有一个待回复槽(已知缺陷)", () => {
  const s = session("me", [member("a"), member("b")]);

  const a1 = handleIncoming(s, { from: "a", id: "req-a", re: null, body: { text: "来自 a" } });
  const first = { ...s.pendingReply };
  assert.equal(first.to, "a");
  assert.ok(types(a1).includes("inject"), "a 的请求仍然注入模型");

  const a2 = handleIncoming(s, { from: "b", id: "req-b", re: null, body: { text: "来自 b" } });
  assert.ok(types(a2).includes("inject"), "b 的请求也注入模型");

  assert.equal(s.pendingReply.to, "b", "后到的覆盖先到的 —— 这就是已知缺陷");
  assert.notDeepEqual(s.pendingReply, first, "a 的待回复被挤掉了");

  // 模型这一轮同时处理了两条注入,产出一段文本
  s.lastText = "综合两条的回答";
  const actions = onTurnSettled(s);

  const sends = actions.filter((x) => x.type === "send");
  assert.equal(sends.length, 1, "只能发一条自动回信");
  assert.equal(sends[0].to, "b", "只有 b 拿到自动回信");
  assert.equal(sends[0].re, "req-b", "回信关联的是最后那条请求");
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
  assert.equal(s.pendingReply, null, "回复不该设置待回复槽");
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
  assert.equal(s.pendingReply.re, "req-x", "re 指向入站消息 id,用于回信时闭合");
});

test("对话形状:fyi 广播只出卡片", () => {
  const s = session();
  const actions = handleIncoming(s, { from: "peer", id: "fyi-1", re: null, body: { text: "状态", fyi: true } });

  assert.deepEqual(types(actions), ["card"]);
  assert.equal(s.pendingReply, null);
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

test("对话形状回归:旧行为(回复也自动回信)会打到跳数上限", () => {
  // 对照组:用完整的 handleIncoming / onTurnSettled 走一遍两个会话。
  // 关键是方向 —— 收到消息的那一方跑轮次并回信,不是发送方。
  function run(firstOrigin) {
    const nodes = { A: session("A", [member("B")]), B: session("B", [member("A")]) };
    const wire = [];

    const first = sendMessage(nodes.A, { to: "B", text: "原始提问", origin: firstOrigin });
    const sa = first.find((a) => a.type === "send");
    wire.push({ to: "B", env: { from: "A", id: sa.id, re: null, body: { text: "原始提问", hops: 0 } } });

    for (let i = 0; i < wire.length && i < 20; i++) {
      const msg = wire[i];
      const receiver = nodes[msg.to];
      handleIncoming(receiver, msg.env);

      receiver.lastText = `answer-${i}`;
      for (const a of onTurnSettled(receiver)) {
        if (a.type === "send") {
          wire.push({
            to: a.to,
            env: { from: receiver.self, id: a.id, re: a.re, body: { text: a.text, hops: a.hops } },
          });
        }
      }
    }
    return wire.length;
  }

  assert.equal(run("model"), 2, "模型发起的请求:一来一回共 2 条");
  assert.equal(run("user"), 2, "用户发起的请求:同样 2 条,不该有第二条回复");
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
  assert.match(req, /会自动原样回传/);

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
test("契约:onTurnSettled 的 send 意图在顶层带 text 和 hops", () => {
  const s = session("me", [member("peer")]);
  s.pendingReply = { to: "peer", hops: 0, re: "req-1" };
  s.lastText = "回复内容";

  const [send] = onTurnSettled(s);
  assert.equal(send.type, "send");
  assert.equal(send.text, "回复内容", "text 必须在顶层 —— index.ts 靠它组装 body");
  assert.equal(typeof send.hops, "number");
  assert.equal(send.hops, 1);
  assert.equal(send.re, "req-1");
  assert.equal(send.id, undefined === send.id ? undefined : send.id, "id 由生产者生成");
  assert.ok(send.id, "send 意图必须自带 id");
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

  const fromSettled = onTurnSettled({ ...s, pendingReply: { to: "peer", hops: 0, re: "r" } });
  const fromSend = sendMessage(s, { to: "peer", text: "y", origin: "user" });

  for (const a of [...fromSettled, ...fromSend].filter((x) => x.type === "send")) {
    assert.equal("body" in a, false, "send 意图不该自带 body —— 那是 index.ts 的职责");
  }
});
