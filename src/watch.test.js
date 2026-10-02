/**
 * 订阅测试。跑:node --test src/
 *
 * 这里覆盖三件事,都是这个特性最容易出问题的地方:
 *   1. 循环保护 —— 通知只能是卡片,绝不能变成 inject。一旦变成 inject,
 *      两个互相订阅的节点就无限 ping-pong。这是结构性的,不是参数。
 *   2. broker 重启后重新登记。
 *   3. 限流是合并而不是丢弃。
 */
import test from "node:test";
import assert from "node:assert/strict";
import { createSessionState, handleIncoming, onTurnSettled } from "./session.js";
import { dispatch } from "./dispatch.js";
import {
  MAX_WATCHES,
  applyWatchAck,
  createWatchState,
  describeWatches,
  formatWatchNotify,
  validateWatchTarget,
} from "./watch.js";

const brokerEnv = (body) => ({ from: "broker", id: "sys-1", re: null, body });

// ---------------------------------------------------------------- 纯逻辑

test("validateWatchTarget 拒掉自己、空名字和不合法的名字", () => {
  assert.equal(validateWatchTarget("peer", "me"), null);
  assert.match(validateWatchTarget("me", "me"), /不能订阅自己/);
  assert.match(validateWatchTarget("", "me"), /给一个节点名/);
  assert.match(validateWatchTarget("-bad", "me"), /不是合法的节点名/);
  assert.match(validateWatchTarget("has space", "me"), /不是合法的节点名/);
});

test("applyWatchAck 只在 ack 之后才改本地集合", () => {
  const w = createWatchState();
  assert.equal(w.targets.size, 0, "初始为空");

  // error 不该改状态
  const err = applyWatchAck(w, { kind: "watch_error", reason: "limit", limit: 8 });
  assert.equal(w.targets.size, 0);
  assert.equal(err.level, "warning");

  // ack 才改
  applyWatchAck(w, { kind: "watch_ack", action: "add", target: "peer" });
  assert.deepEqual([...w.targets], ["peer"]);

  // 重复 add 幂等
  applyWatchAck(w, { kind: "watch_ack", action: "add", target: "peer" });
  assert.deepEqual([...w.targets], ["peer"]);

  applyWatchAck(w, { kind: "watch_ack", action: "remove", target: "peer" });
  assert.deepEqual([...w.targets], []);

  // 再删一次不算错,但要说清楚"本来就没有" —— 静默成功会让用户以为
  // 自己取消掉了别的什么东西。
  const again = applyWatchAck(w, { kind: "watch_ack", action: "remove", target: "peer", state: "absent" });
  assert.match(again.lines[0], /本来就没有/);
});

test("watch_none 不是错误,但必须说出来", () => {
  const r = applyWatchAck(createWatchState(), { kind: "watch_none" });
  assert.equal(r.level, "info");
  assert.match(r.lines[0], /没有人订阅你/);
});

// ---------------------------------------------------------------- 循环保护

test("watch_notify 只产出卡片,绝不产出 inject", () => {
  // 这是整个特性的安全性质。如果这里变成 inject,互相订阅就会无限循环:
  // A 输出 → B 被注入 → B 跑一轮 → B 输出 → A 被注入 → …
  const s = createSessionState("me");
  const actions = handleIncoming(
    s,
    brokerEnv({
      kind: "watch_notify",
      target: "peer",
      at: Date.now(),
      summary: "我跑完了",
    }),
  );

  assert.equal(actions.length, 1);
  assert.equal(actions[0].type, "card", "只能是卡片");
  assert.equal(actions[0].kind, "watch");
  assert.equal(actions[0].peer, "peer");

  for (const a of actions) {
    assert.notEqual(a.type, "inject", "watch 通知绝不可以注入模型");
    assert.notEqual(a.type, "send", "watch 通知不该引发任何发送");
  }
});

test("watch_notify 即使带着 re 也不会被当成回复注入", () => {
  // 防御性:即使将来有人给通知帧加了 re,也不该走回复那条路。
  const s = createSessionState("me");
  const actions = handleIncoming(s, {
    from: "broker",
    id: "sys-2",
    re: "some-outbound-id",
    body: { kind: "watch_notify", target: "peer", summary: "x" },
  });
  assert.equal(actions.length, 1);
  assert.equal(actions[0].type, "card");
});

test("互相订阅不会产生第二条消息", () => {
  // A 订阅 B、B 订阅 A,两端都收到通知 —— 每端只应多出一张卡片,
  // 不该产生任何 send。这就是"循环终止在深度 1"的机制。
  const a = createSessionState("A");
  const b = createSessionState("B");

  const fromB = handleIncoming(a, brokerEnv({ kind: "watch_notify", target: "B", summary: "B says hi" }));
  const fromA = handleIncoming(b, brokerEnv({ kind: "watch_notify", target: "A", summary: "A says hi" }));

  for (const actions of [fromB, fromA]) {
    assert.equal(actions.filter((x) => x.type === "send").length, 0, "不该有任何 send");
    assert.equal(actions.filter((x) => x.type === "inject").length, 0, "不该有任何 inject");
  }
});

test("formatWatchNotify 拒掉形状不对的帧", () => {
  assert.equal(formatWatchNotify({ from: "broker", body: { kind: "welcome" } }), null);
  assert.equal(formatWatchNotify({ from: "peer", body: { kind: "watch_notify" } }), null, "必须来自 broker");
  assert.equal(formatWatchNotify(undefined), null);
});

test("formatWatchNotify 说明被截断和被合并的部分", () => {
  const f = formatWatchNotify(
    brokerEnv({ kind: "watch_notify", target: "peer", summary: "短", overflow: 7, fullLength: 900 }),
  );
  assert.match(f.text, /\[订阅\] peer:短/);
  assert.match(f.text, /还有 7 条被合并/);
  assert.match(f.text, /原文 900 字/);
});

// ---------------------------------------------------------------- 发布

test("有人在看我时才发布本轮的输出", () => {
  const s = createSessionState("me");
  s.members = [{ name: "other", host: null, addr: null, labels: [], since: 0 }];

  s.lastText = "本轮结论";
  assert.deepEqual(onTurnSettled(s), [], "没人在看时什么都不发");

  // 门控是 watcherCount(别人订阅我),不是我自己的订阅列表(我想看谁)。
  // 曾经搞反过:结果是自己没订阅任何人时,订阅我的人永远收不到东西。
  s.watch.targets.add("someone-i-watch");
  s.lastText = "本轮结论";
  assert.deepEqual(onTurnSettled(s), [], "我订阅别人,不代表有人订阅我");

  s.watcherCount = 1;
  s.lastText = "本轮结论";
  const actions = onTurnSettled(s);
  assert.equal(actions.length, 1);
  assert.equal(actions[0].type, "publish");
  assert.equal(actions[0].text, "本轮结论");
});

test("没有输出时不发布", () => {
  const s = createSessionState("me");
  s.watcherCount = 3;
  s.lastText = "   ";
  assert.deepEqual(onTurnSettled(s), []);
});

test("reply=off 也照样发布订阅", () => {
  // 订阅是"别人想看我",reply 是"我想怎么回别人"。两件事不能互相绑架,
  // 否则把 reply 设成 off 就会静默让订阅失效。
  const s = createSessionState("me");
  s.reply = "off";
  s.watcherCount = 1;
  s.lastText = "结论";

  const actions = onTurnSettled(s);
  assert.equal(actions.length, 1);
  assert.equal(actions[0].type, "publish");
});

// ---------------------------------------------------------------- broker 重启

test("welcome 带来订阅者计数,并重新登记自己的订阅", () => {
  // 订阅不落盘,broker 重启就没了。welcome 是"世界重来一遍"的信号。
  const s = createSessionState("me");
  s.watch.targets.add("alpha");
  s.watch.targets.add("beta");

  const actions = handleIncoming(s, brokerEnv({ kind: "welcome", members: [], watchers: 2 }));
  const reAdds = actions.filter((a) => a.type === "watch" && a.action === "add");
  assert.deepEqual(reAdds.map((a) => a.target).sort(), ["alpha", "beta"]);
  assert.equal(s.watcherCount, 2, "welcome 要带上订阅者计数");

  // peer_joined 不该触发重新登记 —— 那不是"世界重来"
  const s2 = createSessionState("me");
  s2.watch.targets.add("alpha");
  const joined = handleIncoming(s2, brokerEnv({ kind: "peer_joined", peer: "x", members: [] }));
  assert.equal(joined.filter((a) => a.type === "watch").length, 0);
});

// ---------------------------------------------------------------- dispatch

function envOf(state, mode = "broker") {
  return {
    connState: "online",
    team: "demo",
    config: { url: "http://x:1", token: "t", mode },
    mode,
  };
}

test("mesh/swim 下 watch 响亮失败,不静默 no-op", () => {
  for (const mode of ["mesh", "swim"]) {
    const s = createSessionState("me");
    const r = dispatch({ sub: "watch", args: ["add", "peer"] }, s, envOf(s, mode));
    assert.equal(r.ok, false, `${mode} 下必须失败`);
    assert.match(r.error, /broker/);
  }
});

test("watch add 产出登记意图,且不提前改本地状态", () => {
  const s = createSessionState("me");
  const r = dispatch({ sub: "watch", args: ["add", "peer"] }, s, envOf(s));
  assert.equal(r.ok, true);
  assert.equal(r.intentions.length, 1);
  assert.deepEqual(r.intentions[0], { type: "watch", action: "add", target: "peer" });
  assert.equal(s.watch.targets.size, 0, "broker 才是权威,ack 之前不改本地");
});

test("watch list 在没订阅时给出用法", () => {
  const s = createSessionState("me");
  const r = dispatch({ sub: "watch", args: ["list"] }, s, envOf(s));
  assert.equal(r.ok, true);
  assert.match(r.lines.join("\n"), /没有订阅任何人/);
  assert.match(r.lines.join("\n"), /\/team watch add/);
});

test("watch add 到达上限时拒绝", () => {
  const s = createSessionState("me");
  for (let i = 0; i < MAX_WATCHES; i++) s.watch.targets.add(`peer${i}`);
  const r = dispatch({ sub: "watch", args: ["add", "onemore"] }, s, envOf(s));
  assert.equal(r.ok, false);
  assert.match(r.error, new RegExp(String(MAX_WATCHES)));
});

test("subscribe 自己会被拒", () => {
  const s = createSessionState("me");
  const r = dispatch({ sub: "watch", args: ["add", "me"] }, s, envOf(s));
  assert.equal(r.ok, false);
  assert.match(r.error, /不能订阅自己/);
});

test("未知动作报错而不是猜", () => {
  const s = createSessionState("me");
  const r = dispatch({ sub: "watch", args: ["frobnicate", "peer"] }, s, envOf(s));
  assert.equal(r.ok, false);
  assert.match(r.error, /add \/ remove \/ list/);
});

test("describeWatches 排序输出", () => {
  const w = createWatchState();
  w.targets.add("zeta");
  w.targets.add("alpha");
  assert.equal(describeWatches(w), "alpha, zeta");
  assert.equal(describeWatches(createWatchState()), "");
});

// ---------------------------------------------------------------- 限流语义

test("限流是合并计数,不是丢弃", async () => {
  // broker 侧的行为在这里用文档化的契约表述:被限掉的通知不消失,
  // 它的数量会在下一条的 overflow 里报出来。
  const { readFileSync } = await import("node:fs");
  const broker = readFileSync(new URL("../broker.mjs", import.meta.url), "utf8");
  assert.match(broker, /bucket\.overflow \+= 1/, "超限时必须累加计数");
  assert.match(broker, /overflow/, "并把它带在后续的帧上");
  assert.equal(
    /overflow[^\n]*=\s*0[^\n]*\n[^\n]*continue/.test(broker),
    false,
    "不能把 overflow 清零后直接丢弃",
  );
});
