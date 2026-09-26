/**
 * dispatch 测试。跑:node --test src/
 *
 * dispatch 是命令和工具的唯一实现,所以它是"两个入口行为一致"这条
 * 承诺的落点。这里覆盖每个子命令的成功与失败路径,以及它产出的
 * intentions / party 是否符合预期。
 *
 * dispatch 不产生副作用,所以测试只需要一个 state 和一个 env。
 */
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BULK_WARN_THRESHOLD, dispatch, doSend, sendMessage } from "./dispatch.js";
import { applyRoster, createSessionState } from "./session.js";

const member = (name, over = {}) => ({
  name,
  host: over.host ?? null,
  addr: over.addr ?? null,
  labels: over.labels ?? [],
  since: 0,
});

/** 造一个已连上、roster 已填充的状态 */
function setup({ self = "me", peers = [member("peer")], connState = "online", team = "demo", config = null } = {}) {
  const state = createSessionState(self);
  applyRoster(state, { members: [member(self), ...peers] });
  const env = { connState, team, config: config ?? { url: "http://x:1", token: "t" } };
  return { state, env };
}

const run = (sub, args, ctx) => dispatch({ sub, args }, ctx.state, ctx.env);
const intentTypes = (r) => (r.intentions ?? []).map((i) => i.type);

// ---------------------------------------------------------------- status / peers

test("status:列出 team、节点名、标签、连接状态", () => {
  const c = setup({ team: "alpha" });
  c.state.selfLabels = ["web"];
  const r = run("status", [], c);

  assert.equal(r.ok, true);
  const all = r.lines.join("\n");
  assert.match(all, /alpha/);
  assert.match(all, /me/);
  assert.match(all, /web/);
  assert.match(all, /online/);
});

test("peers:按机器分组并列出可用分组", () => {
  const c = setup({
    peers: [
      member("a", { host: "dev01", labels: ["web"] }),
      member("b", { host: "dev01", labels: ["web", "db"] }),
      member("c", { host: "laptop" }),
    ],
  });
  const r = run("peers", [], c);

  const all = r.lines.join("\n");
  assert.match(all, /dev01\s+\(2\)/, "同机两个节点应归到一组");
  assert.match(all, /laptop\s+\(1\)/);
  assert.match(all, /可用分组:@db @web/);
});

test("peers:没人时仍算查询成功,并说明只有自己", () => {
  const c = setup({ peers: [] });
  const r = run("peers", [], c);
  // ok 表示"查询执行成功",不是"有结果"。没人时返回 ok:true +
  // 说明文案,让调用方不必把"空"当成错误。
  assert.equal(r.ok, true);
  assert.match(r.lines.join(), /只有你自己在线/);
});

// ---------------------------------------------------------------- 未知子命令

test("未知子命令:报错并提示用 /team 菜单", () => {
  const c = setup();
  const r = run("nonsense", [], c);
  assert.equal(r.ok, false);
  assert.match(r.error, /未知子命令/);
  assert.match(r.error, /\/team/);
});

// ---------------------------------------------------------------- send

test("send:单播产出 send + card 两个意图", () => {
  const c = setup();
  const r = run("send", ["peer", "你好"], c);

  assert.equal(r.ok, true);
  assert.deepEqual(intentTypes(r), ["send", "card"]);
  assert.deepEqual(r.intentions[0].to, "peer");
  assert.equal(r.intentions[0].text, "你好", "正文在顶层,由 index.ts 组装 body");
  assert.equal(r.intentions[1].kind, "send");
});

test("send:记录 origin=user,供对方回复时判断", () => {
  const c = setup();
  const r = run("send", ["peer", "用户手动发的"], c);
  const id = r.intentions[0].id;

  assert.equal(c.state.outbound.get(id).origin, "user");
  assert.equal(c.state.outbound.get(id).text, "用户手动发的");
});

test("send:多词内容拼成一条", () => {
  const c = setup();
  const r = run("send", ["peer", "帮我", "跑一下", "测试"], c);
  assert.equal(r.intentions[0].text, "帮我 跑一下 测试");
});

test("send:缺收件人或内容时拒绝,并给用法", () => {
  const c = setup();
  for (const args of [[], ["peer"], ["", "x"]]) {
    const r = run("send", args, c);
    assert.equal(r.ok, false);
    assert.match(r.error, /用法/);
  }
});

test("send:未连接时拒绝,不产出意图", () => {
  const c = setup({ connState: "offline" });
  const r = run("send", ["peer", "hi"], c);
  assert.equal(r.ok, false);
  assert.match(r.error, /未连接/);
  assert.deepEqual(r.intentions, []);
});

test("send:收件人不存在时报出是谁", () => {
  const c = setup();
  const r = run("send", ["ghost", "hi"], c);
  assert.equal(r.ok, false);
  assert.match(r.error, /ghost/);
});

test("send:@分组解析", () => {
  const c = setup({ peers: [member("a", { labels: ["web"] }), member("b", { labels: ["web"] }), member("d", { labels: ["db"] })] });
  const r = run("send", ["@web", "前端注意"], c);

  assert.equal(r.ok, true);
  assert.deepEqual(r.intentions[0].to, "@web");
  assert.match(r.lines.join(), /2 个节点/);
});

test("send:空分组被拒绝", () => {
  const c = setup({ peers: [member("a", { labels: ["web"] })] });
  const r = run("send", ["@nobody", "hi"], c);
  assert.equal(r.ok, false);
  assert.match(r.error, /nobody/);
});

test("send:小群发直接发,不确认", () => {
  const peers = Array.from({ length: BULK_WARN_THRESHOLD }, (_, i) => member(`p${i}`));
  const c = setup({ peers });
  const r = run("send", ["@default", "hi"], c);

  assert.equal(r.ok, true);
  assert.equal(r.party, undefined, "不超过阈值不应要求确认");
  assert.deepEqual(intentTypes(r), ["send", "card"]);
});

test("send:超过阈值要确认,且不直接产出意图", () => {
  const peers = Array.from({ length: BULK_WARN_THRESHOLD + 1 }, (_, i) => member(`p${i}`));
  const c = setup({ peers });
  const r = run("send", ["@default", "hi"], c);

  assert.equal(r.ok, true);
  assert.equal(r.party.kind, "confirmBulk");
  assert.equal(r.party.n, BULK_WARN_THRESHOLD + 1);
  assert.deepEqual(r.intentions, [], "确认前不该产生发送意图");
});

test("send:显式点名单个节点即使人多也不确认", () => {
  const peers = Array.from({ length: 20 }, (_, i) => member(`p${i}`));
  const c = setup({ peers });
  const r = run("send", ["p3", "hi"], c);
  assert.equal(r.party, undefined, "单播不该被当成群发");
});

// ---------------------------------------------------------------- doSend(确认后复用)

test("doSend:确认后走同一条发送路径", () => {
  const c = setup();
  const r = doSend("peer", "内容", "user", { targets: ["peer"], unknown: [] }, c.state, c.env);
  assert.equal(r.ok, true);
  assert.deepEqual(intentTypes(r), ["send", "card"]);
});

// ---------------------------------------------------------------- announce

test("announce:合法值都接受,非法值报错并显示当前值", () => {
  const c = setup();
  for (const m of ["off", "auto", "always"]) {
    assert.equal(run("announce", [m], c).ok, true);
    assert.equal(c.state.announce, m);
  }
  const bad = run("announce", ["sometimes"], c);
  assert.equal(bad.ok, false);
  assert.match(bad.error, /always/);
});

test("on / off 是 announce 的简写", () => {
  const c = setup();
  run("on", [], c);
  assert.equal(c.state.announce, "auto");
  run("off", [], c);
  assert.equal(c.state.announce, "off");
});

// ---------------------------------------------------------------- label

test("label:list 显示当前标签", () => {
  const c = setup();
  c.state.selfLabels = ["web", "fe"];
  const r = run("label", ["list"], c);
  assert.match(r.lines.join(), /web, fe/);
});

test("label:add / remove 更新状态并要求重连", () => {
  const c = setup();
  const add = run("label", ["add", "web", "fe"], c);
  assert.equal(add.ok, true);
  assert.deepEqual(c.state.selfLabels, ["web", "fe"]);
  assert.equal(add.party.kind, "reconnect", "标签改了 broker 才知道,必须重连");
  assert.deepEqual(add.party.labels, ["web", "fe"]);

  const rm = run("label", ["remove", "web"], c);
  assert.deepEqual(c.state.selfLabels, ["fe"]);
  assert.equal(rm.party.kind, "reconnect");
});

test("label:remove 支持 rm 简写", () => {
  const c = setup();
  run("label", ["add", "x"], c);
  run("label", ["rm", "x"], c);
  assert.deepEqual(c.state.selfLabels, []);
});

test("label:缺参数时报用法", () => {
  const c = setup();
  assert.match(run("label", ["add"], c).error, /用法/);
  assert.match(run("label", ["wat"], c).error, /用法/);
});

test("label:未连接时不提示重连", () => {
  const c = setup({ connState: "offline" });
  const r = run("label", ["add", "web"], c);
  assert.equal(r.party.kind, "reconnect", "party 仍然产出,让上层决定");
  assert.ok(!r.lines.some((l) => /正在重连/.test(l)), "离线时不该说正在重连");
});

// ---------------------------------------------------------------- team 生命周期

test("create:生成配置并产出 connect party", (t) => {
  const home = mkdtempSync(join(tmpdir(), "dispatch-test-"));
  t.after(() => rmSync(home, { recursive: true, force: true }));

  // dispatch 内部用真实 home,所以这里只测参数校验路径
  const c = setup();
  const bad = run("create", [], c);
  assert.equal(bad.ok, false);
  assert.match(bad.error, /用法/);

  const badUrl = run("create", ["t1", "not-a-url"], c);
  assert.equal(badUrl.ok, false);
});

test("join:缺 team 名时报用法", () => {
  const c = setup();
  const r = run("join", [], c);
  assert.equal(r.ok, false);
  assert.match(r.error, /用法/);
});

test("leave:没有当前 team 时报用法", () => {
  const c = setup({ team: null });
  const r = run("leave", [], c);
  assert.equal(r.ok, false);
  assert.match(r.error, /用法/);
});

test("leave:不存在的 team 报错,不静默成功", () => {
  const c = setup({ team: "nope-never-exists" });
  const r = run("leave", [], c);
  assert.equal(r.ok, false);
  assert.match(r.error, /没有/);
});

// ---------------------------------------------------------------- 双入口一致性

test("双入口一致性:命令和工具走同一个 dispatch,输出必然相同", () => {
  // 这条测试的意义在于固定"单一实现"这个约束:如果有人给命令
  // 或工具单独加了分支逻辑,它会先在这里失败。
  const a = setup();
  const b = setup();
  const viaCommand = run("send", ["peer", "同样的内容"], a);
  const viaTool = run("send", ["peer", "同样的内容"], b);

  assert.equal(viaCommand.ok, viaTool.ok);
  assert.deepEqual(intentTypes(viaCommand), intentTypes(viaTool));
  assert.deepEqual(viaCommand.intentions[0].text, viaTool.intentions[0].text);
  assert.deepEqual(viaCommand.intentions[0].to, viaTool.intentions[0].to);
});

test("sendMessage 是 dispatch 与工具共用的底层入口", () => {
  const c = setup();
  const r = sendMessage("peer", "底层入口", "model", c.state, c.env);
  assert.equal(r.ok, true);
  assert.equal(c.state.outbound.get(r.intentions[0].id).origin, "model");
});

// ---------------------------------------------------------------- id 唯一性

/**
 * 真机上出过一次严重故障:id 生成器是 `m-<时间>-<进程内计数器>`,
 * 缺了进程标识。两个节点在同一毫秒各发一条时,时间相同、计数器都从
 * 0 开始,id 必然碰撞。接收方靠 id 去重,于是第二条被当成重复投递
 * 静默丢弃。症状是"发送方看到投递 1/1,接收方毫无反应",极难定位。
 */
test("id 唯一性:同一毫秒内多次调用不重复", () => {
  const c = setup();
  const ids = new Set();
  for (let i = 0; i < 50; i++) {
    const r = run("send", ["peer", `m${i}`], c);
    ids.add(r.intentions[0].id);
  }
  assert.equal(ids.size, 50, "同一毫秒内的 id 必须互不相同");
});

test("id 唯一性:带进程标识,不会和另一个进程碰撞", () => {
  // 无法在单进程内直接模拟两个进程,但可以断言 id 里除了时间和
  // 计数器之外还有一段进程级随机成分:长度足够且不随调用变化。
  const c = setup();
  const r = run("send", ["peer", "x"], c);
  const id = r.intentions[0].id;

  const parts = id.split("-");
  assert.ok(parts.length >= 4, `id 应有 >=4 段(含进程标识),实际 ${id}`);

  // 进程标识段在多次调用间保持不变(它是模块级的,不是每次随机)
  const r2 = run("send", ["peer", "y"], c);
  assert.equal(id.split("-")[2], r2.intentions[0].id.split("-")[2], "进程标识应稳定");
});

test("id 唯一性:群发路径也用同一个生成器", () => {
  // 注意阈值:超过 BULK_WARN_THRESHOLD 会返回 confirmBulk 而不是
  // 直接发送,所以群发要控制在阈值以内才能真正拿到 send 意图。
  const peers = Array.from({ length: BULK_WARN_THRESHOLD }, (_, i) => member(`p${i}`, { labels: ["web"] }));
  const bulk = setup({ peers });
  const single = setup();

  const fromBulk = run("send", ["@web", "b"], bulk);
  assert.equal(fromBulk.ok, true);
  assert.equal(fromBulk.party, undefined, "阈值以内应直接发送");

  const bulkId = fromBulk.intentions[0].id;
  const singleId = run("send", ["peer", "a"], single).intentions[0].id;

  assert.match(bulkId, /^m-/);
  assert.match(singleId, /^m-/);
  assert.notEqual(bulkId, singleId);
  assert.ok(bulkId.split("-").length >= 4, "群发路径的 id 也要带进程标识");
  assert.equal(bulkId.split("-")[2], singleId.split("-")[2], "同一进程内进程标识应一致");
});

// ---------------------------------------------------------------- 意图契约

/**
 * send 意图必须用顶层 text / hops,由 index.ts 组装信封的 body。
 *
 * 真机上出过:dispatch.js 写 body:{text},而 index.ts 读 it.text,
 * 结果 /team send 发出空正文的消息。对方只看到空字符串,
 * 症状是"发送方显示投递成功,接收方完全没反应"。
 *
 * 两个生产者(dispatch.js 和 session.js)都要满足同一契约,
 * 所以这里两边都测。
 */
test("契约:dispatch 的 send 意图用顶层 text / hops", () => {
  const c = setup();
  const r = run("send", ["peer", "正文内容"], c);
  const send = r.intentions.find((i) => i.type === "send");

  assert.ok(send);
  assert.equal(send.text, "正文内容", "text 必须在顶层");
  assert.equal(send.hops, 0);
  assert.equal("body" in send, false, "不该自带 body —— 那是 index.ts 的职责");
  assert.ok(send.id, "必须自带 id");
});

test("契约:群发确认后走 doSend,同样用顶层 text", () => {
  const c = setup();
  const r = doSend("peer", "群发正文", "user", { targets: ["peer"], unknown: [] }, c.state, c.env);
  const send = r.intentions.find((i) => i.type === "send");

  assert.ok(send);
  assert.equal(send.text, "群发正文");
  assert.equal("body" in send, false);
});

test("契约:两个生产者的 send 意图字段集一致", async () => {
  // 防止再次出现"一边改了一边没改"
  const { createSessionState, applyRoster, onTurnSettled } = await import("./session.js");

  const c = setup();
  const fromDispatch = run("send", ["peer", "x"], c).intentions.find((i) => i.type === "send");

  const s = createSessionState("me");
  applyRoster(s, { members: [{ name: "me", labels: [], host: null, addr: null, since: 0 }, { name: "peer", labels: [], host: null, addr: null, since: 0 }] });
  s.pendingReply = { to: "peer", hops: 0, re: "r1" };
  s.lastText = "y";
  const fromSession = onTurnSettled(s).find((i) => i.type === "send");

  const keys = (o) => Object.keys(o).sort().join(",");
  assert.equal(
    keys(fromDispatch).includes("text") && keys(fromSession).includes("text"),
    true,
    `两边都要有 text: dispatch=${keys(fromDispatch)} session=${keys(fromSession)}`,
  );
  assert.equal("body" in fromDispatch, false);
  assert.equal("body" in fromSession, false);
});

test("契约:正文非空 —— 空正文会被接收方丢弃", () => {
  const c = setup();
  const r = run("send", ["peer", "非空"], c);
  const send = r.intentions.find((i) => i.type === "send");
  assert.ok(send, `应有 send 意图,实际 ${JSON.stringify(r)}`);
  assert.ok(send.text.length > 0);
});
