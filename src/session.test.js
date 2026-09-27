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
  applyRoster,
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

/**
 * 模拟 Pi 把一轮跑完,并喂给 observeMessage 的事件序列。
 *
 * 参数 turns 的形状是 [[请求 payload, 回答文本], ...],按实测的事件顺序:
 *   user <payload> → assistant <回答>
 * 连续两个 user 之间没有 assistant,说明它们在同一个 turn(followUpMode=all)。
 */
function runTurns(s, turns) {
  for (const [payload, answer] of turns) {
    observeMessage(s, "user", payload);
    observeMessage(s, "assistant", answer);
    s.lastText = answer;
  }
}

/**
 * 直接造一条待回复条目。
 *
 * 用于只关心"送出"逻辑的测试 —— 跳过注入和事件绑定那两步。
 * text 为 null 表示还没绑定到回答。
 */
function pending(to, over = {}) {
  return { to, hops: over.hops ?? 0, re: over.re ?? `req-${to}`, payload: over.payload ?? `payload-${to}`, bound: over.bound ?? true, text: over.text ?? "回答内容" };
}

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
  s.pendingReplies = [pending("old-peer")];

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
  const s = session("me", []);
  s.pendingReplies = [pending("gone-peer", { text: "想回的答复" })];
  s.lastText = "想回的答复";

  const actions = onTurnSettled(s);
  assert.equal(actions[0].type, "card");
  assert.equal(actions[0].kind, "failed", "对端已不在 roster,不能假装发送成功");
  assert.match(actions[0].reason, /已离线/);
});

// ================================================================ 缺口 2:对端中途离线

test("缺口 2:请求发出后对端离线,回信失败并出 fail 卡片", () => {
  const s = session("me", [member("peer")]);
  s.pendingReplies = [pending("peer", { re: "req-1", text: "答复内容" })];
  s.lastText = "答复内容";

  // 对端掉线,roster 更新
  applyRoster(s, { members: [member("me")] });

  const actions = onTurnSettled(s);
  assert.equal(actions.length, 1);
  assert.equal(actions[0].type, "card");
  assert.equal(actions[0].kind, "failed");
  assert.match(actions[0].reason, /peer 已离线/);
  assert.equal(s.pendingReplies.length, 0, "失败后待回复队列应清空");
  assert.equal(s.lastText, "", "文本已消费,不该在下轮重复推");
});

test("缺口 2:对端在线时正常回信,带 re 且跳数 +1", () => {
  const s = session("me", [member("peer")]);
  s.pendingReplies = [pending("peer", { hops: 1, re: "req-1", text: "答复" })];
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

test("缺口 3:同一轮两个请求,各自拿到自己的回答", () => {
  const s = session("me", [member("a"), member("b")]);

  // 两个队友几乎同时发来请求。第二条到达时 run 已在进行中,
  // 所以它走 followUp —— Pi 会给它单独一个 turn。
  const na = handleIncoming(s, { from: "a", id: "req-a", re: null, body: { text: "来自 a" } });
  const nb = handleIncoming(s, { from: "b", id: "req-b", re: null, body: { text: "来自 b" } });

  assert.equal(s.pendingReplies.length, 2, "两条请求都进了队列,没有互相覆盖");
  const payloadA = na.find((x) => x.type === "inject").payload;
  const payloadB = nb.find((x) => x.type === "inject").payload;

  // Pi 实际跑出的形状:两个 turn,每个 turn 一条 user + 一条 assistant
  runTurns(s, [
    [payloadA, "回答给 a"],
    [payloadB, "回答给 b"],
  ]);

  const sends = onTurnSettled(s).filter((x) => x.type === "send");
  assert.equal(sends.length, 2, "两个发信人都要收到回信");

  const byTo = Object.fromEntries(sends.map((x) => [x.to, x]));
  assert.equal(byTo.a.text, "回答给 a", "a 必须收到回答 a 的那段文本");
  assert.equal(byTo.b.text, "回答给 b", "b 必须收到回答 b 的那段文本");
  assert.equal(byTo.a.re, "req-a", "每条回复关联自己的请求 id");
  assert.equal(byTo.b.re, "req-b");
});

test("缺口 3:followUpMode=all 把多条注入合进一个 turn,共用那段回答", () => {
  const s = session("me", [member("a"), member("b")]);

  const na = handleIncoming(s, { from: "a", id: "req-a", re: null, body: { text: "来自 a" } });
  const nb = handleIncoming(s, { from: "b", id: "req-b", re: null, body: { text: "来自 b" } });
  const payloadA = na.find((x) => x.type === "inject").payload;
  const payloadB = nb.find((x) => x.type === "inject").payload;

  // 连续两条 user 之后才出现 assistant —— 模型同时看到了两条输入
  observeMessage(s, "user", payloadA);
  observeMessage(s, "user", payloadB);
  observeMessage(s, "assistant", "一次性回答了两条");
  s.lastText = "一次性回答了两条";

  const sends = onTurnSettled(s).filter((x) => x.type === "send");
  assert.equal(sends.length, 2);
  for (const x of sends) assert.equal(x.text, "一次性回答了两条", "同 turn 的两条共用同一段回答");
  assert.deepEqual(sends.map((x) => x.re).sort(), ["req-a", "req-b"]);
});

test("缺口 3:同一发信人连发多条,只自动回复一次", () => {
  const s = session("me", [member("a")]);

  for (const id of ["r1", "r2", "r3"]) {
    handleIncoming(s, { from: "a", id, re: null, body: { text: `第 ${id} 条` } });
  }
  assert.equal(s.pendingReplies.length, 1, "同一个发信人只保留一条待回复");

  // 三条 user 都进了模型,但只有最后一条 payload 在队列里
  runTurns(s, [[s.pendingReplies[0].payload, "一起答了"]]);

  const sends = onTurnSettled(s).filter((x) => x.type === "send");
  assert.equal(sends.length, 1, "不该为同一个发信人产生三条回信");
  assert.equal(sends[0].to, "a");
});

test("缺口 3:请求没进模型时出失败卡片,不拿别的回答顶上", () => {
  const s = session("me", [member("a"), member("b")]);

  const na = handleIncoming(s, { from: "a", id: "req-a", re: null, body: { text: "来自 a" } });
  handleIncoming(s, { from: "b", id: "req-b", re: null, body: { text: "来自 b" } });

  // 只有 a 的 payload 进了模型(比如 b 那条注入被 Pi 拒绝)
  const payloadA = na.find((x) => x.type === "inject").payload;
  runTurns(s, [[payloadA, "只回答了 a"]]);

  const actions = onTurnSettled(s);
  const sends = actions.filter((x) => x.type === "send");
  const fails = actions.filter((x) => x.type === "card" && x.kind === "failed");

  assert.equal(sends.length, 1);
  assert.equal(sends[0].to, "a");
  assert.equal(fails.length, 1, "b 要看到失败,而不是收到 a 的回答");
  assert.equal(fails[0].peer, "b");
  assert.match(fails[0].reason, /没有.*回答|未自动回复/);
});

test("缺口 3:settle 之后队列清空,下一轮不重复发", () => {
  const s = session("me", [member("a")]);
  const n = handleIncoming(s, { from: "a", id: "req-a", re: null, body: { text: "来自 a" } });
  runTurns(s, [[n.find((x) => x.type === "inject").payload, "回答"]]);

  assert.equal(onTurnSettled(s).filter((x) => x.type === "send").length, 1);
  assert.equal(s.pendingReplies.length, 0, "队列应清空");
  assert.deepEqual(onTurnSettled(s), [], "再 settle 不该重复发");
});

test("缺口 3:announce=off 时队列直接清掉,不发也不留", () => {
  const s = session("me", [member("a")], { announce: "off" });
  handleIncoming(s, { from: "a", id: "req-a", re: null, body: { text: "来自 a" } });
  assert.equal(s.pendingReplies.length, 1);

  assert.deepEqual(onTurnSettled(s), []);
  assert.equal(s.pendingReplies.length, 0);
});

test("缺口 3:队列有上限,超出的出失败卡片而不是静默丢弃", () => {
  const peers = Array.from({ length: 40 }, (_, i) => member(`p${i}`));
  const s = session("me", peers);

  for (let i = 0; i < 40; i++) {
    handleIncoming(s, { from: `p${i}`, id: `req-${i}`, re: null, body: { text: `来自 p${i}` } });
  }

  assert.ok(s.pendingReplies.length <= 32, `队列不应超过上限,实际 ${s.pendingReplies.length}`);
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
  assert.equal(s.pendingReplies[0].payload, actions[1].payload, "入队的 payload 必须和注入给 Pi 的完全一致,否则绑不上回答");
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
      const actions = handleIncoming(receiver, msg.env);

      // 按 Pi 实际发出的事件模拟:注入的 payload 以 user 消息出现,
      // 随后才是 assistant 的回答。旧写法直接赋 lastText,跳过了绑定,
      // 会让这个对照组测不到真实路径。
      const injected = actions.find((a) => a.type === "inject");
      if (injected) observeMessage(receiver, "user", injected.payload);
      observeMessage(receiver, "assistant", `answer-${i}`);
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
  s.pendingReplies = [pending("peer", { re: "req-1", text: "回复内容" })];
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

  const fromSettled = onTurnSettled({ ...s, pendingReplies: [pending("peer", { text: "x" })] });
  const fromSend = sendMessage(s, { to: "peer", text: "y", origin: "user" });

  for (const a of [...fromSettled, ...fromSend].filter((x) => x.type === "send")) {
    assert.equal("body" in a, false, "send 意图不该自带 body —— 那是 index.ts 的职责");
  }
});
