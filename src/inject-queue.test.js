/**
 * 注入队列测试。
 *
 * 核心用例直接照搬探针测到的真实时序 —— 那是这个模块存在的唯一理由。
 */
import test from "node:test";
import assert from "node:assert/strict";
import { createInjectState, hasQueued, onInject, onRunStart, onSettled } from "./inject-queue.js";

test("空闲时第一条直发,并由它开始一个 run", () => {
  const s0 = createInjectState();
  const r = onInject(s0, "A");
  assert.equal(r.mode, "direct");
  assert.equal(r.state.phase, "starting", "已经发出去了,等 agent_start");
  assert.deepEqual(r.state.queued, []);
});

test("同 tick 第二条排队 —— 这就是探针里丢消息的情形", () => {
  // 探针实测:两条都调 direct,第二条被覆盖,永远没进模型。
  // 现在第二条应该排队。
  let s = createInjectState();
  const first = onInject(s, "X");
  s = first.state;
  const second = onInject(s, "Y");
  s = second.state;

  assert.equal(first.mode, "direct");
  assert.equal(second.mode, "queued", "第二条绝不能也直发");
  assert.deepEqual(s.queued, ["Y"]);
});

test("同 tick 第三条也排队,保持到达顺序", () => {
  let s = createInjectState();
  for (const p of ["X", "Y", "Z"]) {
    s = onInject(s, p).state;
  }
  assert.deepEqual(s.queued, ["Y", "Z"], "按到达顺序,先到的先投");
});

test("agent_start 时把排队的一次性投出", () => {
  let s = createInjectState();
  s = onInject(s, "X").state;
  s = onInject(s, "Y").state;
  s = onInject(s, "Z").state;

  const r = onRunStart(s);
  assert.deepEqual(r.flush, ["Y", "Z"]);
  assert.equal(r.state.phase, "running");
  assert.deepEqual(r.state.queued, [], "投出后队列清空");
});

test("agent_start 到达时队列为空是正常的(只有一条消息)", () => {
  let s = createInjectState();
  s = onInject(s, "X").state;
  const r = onRunStart(s);
  assert.deepEqual(r.flush, []);
  assert.equal(r.state.phase, "running");
});

test("run 在跑时直接 followUp,不排队", () => {
  let s = createInjectState();
  s = onRunStart(onInject(s, "X").state).state;

  const r = onInject(s, "Y");
  assert.equal(r.mode, "followUp", "run 在跑时 Pi 支持 followUp,不必绕队列");
  assert.deepEqual(r.state.queued, []);
});

test("settled 之后回到空闲,下一条重新直发", () => {
  let s = createInjectState();
  s = onInject(s, "X").state;
  s = onRunStart(s).state;
  s = onSettled(s);

  assert.equal(s.phase, "ready");
  assert.equal(onInject(s, "next").mode, "direct");
});

test("settled 会清掉残留队列 —— 不重复投递", () => {
  // 极端情况:agent_start 从没到过就 settled 了(比如被用户 Esc 中止)。
  let s = createInjectState();
  s = onInject(s, "X").state;
  s = onInject(s, "Y").state;
  assert.ok(hasQueued(s), "前置条件:队列里确实有东西");

  s = onSettled(s);
  assert.equal(hasQueued(s), false, "settled 必须清空,否则下一轮会重投一遍");
  assert.equal(s.phase, "ready");
});

test("不修改传入的状态(纯函数)", () => {
  const s0 = createInjectState();
  const afterDirect = onInject(s0, "A").state;
  assert.equal(s0.phase, "ready", "原对象不该被改动");
  assert.deepEqual(s0.queued, []);

  const queuedAfter = onInject(afterDirect, "B").state;
  assert.deepEqual(afterDirect.queued, [], "排队不该原地改上一个状态");
  assert.deepEqual(queuedAfter.queued, ["B"]);
});

test("完整时序:空闲→直发→排队→flush→followUp→settled", () => {
  // 一次走完三种 phase,确保转移没有遗漏
  let s = createInjectState();

  assert.equal(onInject(s, "1").mode, "direct");
  s = onInject(s, "1").state;

  assert.equal(onInject(s, "2").mode, "queued");
  s = onInject(s, "2").state;

  const started = onRunStart(s);
  assert.deepEqual(started.flush, ["2"]);
  s = started.state;

  assert.equal(onInject(s, "3").mode, "followUp");
  s = onInject(s, "3").state;

  s = onSettled(s);
  assert.equal(s.phase, "ready");
  assert.equal(hasQueued(s), false);
});
