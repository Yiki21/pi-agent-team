/**
 * 入站策略单测。跑:node --test src/
 *
 * 最后一组"对话形状"测试是回归测试:它们复现了真机上观察到的
 * 来回打转(broker 日志 5 条来回,直到跳数上限才停)。
 */
import test from "node:test";
import assert from "node:assert/strict";
import { classifyInbound, buildPayload, excerpt, rememberBounded, MAX_HOPS } from "./policy.js";

const env = (over = {}) => ({
  from: "peer",
  id: "in-1",
  re: null,
  body: { text: "hello", hops: 0 },
  ...over,
});

test("新请求:注入并自动回传", () => {
  const cls = classifyInbound(env(), new Map());
  assert.equal(cls.action, "inject");
  assert.equal(cls.kind, "request");
  assert.equal(cls.autoReply, true);
});

test("回复模型发出的消息:注入、附原文、不自动回传", () => {
  const out = new Map([["m-1", { text: "请回复节点名", origin: "model", to: "peer" }]]);
  const cls = classifyInbound(env({ re: "m-1" }), out);
  assert.equal(cls.action, "inject");
  assert.equal(cls.kind, "reply");
  assert.equal(cls.autoReply, false);
  assert.equal(cls.original, "请回复节点名");
});

test("回复用户手动发出的消息:只显示卡片,不叫醒模型", () => {
  // 真机上的 bug 起点:/team send 由用户发出,模型不知道这回事,
  // 收到回复时一头雾水,说"正文是空的"。
  const out = new Map([["m-1", { text: "请回复节点名", origin: "user", to: "#api" }]]);
  const cls = classifyInbound(env({ re: "m-1" }), out);
  assert.equal(cls.action, "card");
  assert.equal(cls.autoReply, false);
  assert.equal(cls.original, "请回复节点名");
});

test("回复但原消息查不到(例如重启后):注入,不自动回传", () => {
  const cls = classifyInbound(env({ re: "gone" }), new Map());
  assert.equal(cls.action, "inject");
  assert.equal(cls.kind, "reply");
  assert.equal(cls.autoReply, false);
  assert.equal(cls.original, null);
});

test("fyi 广播只显示卡片", () => {
  const cls = classifyInbound(env({ body: { text: "status", hops: 1, fyi: true } }), new Map());
  assert.equal(cls.action, "card");
  assert.equal(cls.kind, "fyi");
});

test("跳数到上限丢弃", () => {
  const cls = classifyInbound(env({ body: { text: "x", hops: MAX_HOPS } }), new Map());
  assert.equal(cls.action, "drop");
  assert.equal(cls.reason, "hops");
});

test("空正文丢弃", () => {
  for (const text of ["", "   ", "\n\t"]) {
    assert.equal(classifyInbound(env({ body: { text, hops: 0 } }), new Map()).action, "drop");
  }
  assert.equal(classifyInbound(env({ body: {} }), new Map()).action, "drop");
});

test("请求的注入文本说明会自动回传", () => {
  const p = buildPayload("laptop", "跑一下测试", { kind: "request" });
  assert.match(p, /\[来自 laptop 的 team 消息\]/);
  assert.match(p, /跑一下测试/);
  assert.match(p, /自动原样回传给 laptop/);
});

test("回复的注入文本说明不会回传,并引用原问题", () => {
  const p = buildPayload("dev01", "dev01-api / sgqz-0001", { kind: "reply", original: "请回复你的节点名和机器名" });
  assert.match(p, /\[来自 dev01 的 team 回复\]/);
  assert.match(p, /请回复你的节点名和机器名/);
  assert.match(p, /【不会】自动回传/);
  assert.match(p, /team_send/);
});

test("excerpt 压缩空白并截断", () => {
  assert.equal(excerpt("a\n\n  b"), "a b");
  assert.equal(excerpt("x".repeat(200), 10), `${"x".repeat(10)}…`);
});

test("rememberBounded 超过上限丢最早的", () => {
  const m = new Map();
  for (let i = 0; i < 5; i++) rememberBounded(m, `k${i}`, i, 3);
  assert.deepEqual([...m.keys()], ["k2", "k3", "k4"]);
});

// ------------------------------------------------------------------ 对话形状

/**
 * 模拟两个节点,都处在 announce=auto。
 * 每个节点:收到消息 → classify → 若 autoReply 则回一条带 re 的消息。
 * 返回线上实际流过的消息条数。
 */
function simulate(firstOrigin) {
  const nodes = {
    A: { outbound: new Map() },
    B: { outbound: new Map() },
  };
  let seq = 0;
  const wire = [];

  const sendFrom = (from, to, text, re, hops, origin) => {
    const id = `${from}-${seq++}`;
    rememberBounded(nodes[from].outbound, id, { text, origin, to });
    wire.push({ from, to, id, re, body: { text, hops } });
  };

  sendFrom("A", "B", "请回复节点名", null, 0, firstOrigin);

  // 最多跑 20 步;正确的实现应该很快停下
  for (let i = 0; i < wire.length && i < 20; i++) {
    const msg = wire[i];
    const receiver = nodes[msg.to];
    const cls = classifyInbound(msg, receiver.outbound);
    if (cls.action === "inject" && cls.autoReply) {
      sendFrom(msg.to, msg.from, `answer-${i}`, msg.id, msg.body.hops + 1, "model");
    }
  }
  return wire;
}

test("对话形状:模型发起的请求,一来一回就结束(回归)", () => {
  const wire = simulate("model");
  assert.equal(wire.length, 2, `应为 请求+回复 两条,实际 ${wire.length} 条`);
  assert.equal(wire[1].re, wire[0].id, "回复必须带 re 指向原请求");
});

test("对话形状:用户手动发起的请求,同样一来一回就结束(回归)", () => {
  const wire = simulate("user");
  assert.equal(wire.length, 2, `应为 请求+回复 两条,实际 ${wire.length} 条`);
});

test("对话形状:旧行为(回复也自动回信)会打转到跳数上限", () => {
  // 对照组:如果把"回复不自动回传"这条去掉,会复现真机上的 5 条来回。
  // 这条测试保证我们理解 bug 的机制,而不只是让数字碰巧对上。
  const wire = [{ from: "A", to: "B", id: "A-0", re: null, body: { text: "q", hops: 0 } }];
  for (let i = 0; i < wire.length && i < 20; i++) {
    const m = wire[i];
    if (m.body.hops >= MAX_HOPS) continue;
    wire.push({ from: m.to, to: m.from, id: `x${i}`, re: m.id, body: { text: "a", hops: m.body.hops + 1 } });
  }
  assert.equal(wire.length, MAX_HOPS + 1, "旧行为的消息条数 = 跳数上限 + 1");
});
