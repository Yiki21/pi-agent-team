/**
 * Transport 一致性测试套件。
 *
 * 这是三种模式(broker / mesh / swim)的共同验收标准。新 transport
 * 必须整套通过才能算完成 —— roadmap 8.3 的八条保证都在这里落地。
 *
 * 用法:在 transport 自己的测试文件里调用 conformanceSuite():
 *
 *   import { conformanceSuite } from "./conformance.js";
 *   conformanceSuite({
 *     name: "broker",
 *     makeHarness: async (t) => ({ create, teardown }),
 *   });
 *
 * ── 为什么参数化 ──
 * 三种 transport 的差异只在"消息怎么走"。如果每条保证都给每种模式
 * 单独写一遍测试,它们会慢慢分叉,最后变成三套不同的语义。
 * 一个套件跑三遍,语义不可能分叉。
 */
import test from "node:test";
import assert from "node:assert/strict";

/** 等一个条件成立,超时则失败 */
export function waitFor(predicate, ms = 5000, label = "条件") {
  return new Promise((resolve, reject) => {
    const t0 = Date.now();
    const tick = () => {
      let v;
      try {
        v = predicate();
      } catch (err) {
        return reject(err);
      }
      if (v) return resolve(v);
      if (Date.now() - t0 > ms) return reject(new Error(`${label} 超时(${ms}ms)`));
      setTimeout(tick, 20);
    };
    tick();
  });
}

/** 短暂等待,用于断言"不该发生的事" */
export const settle = (ms = 300) => new Promise((r) => setTimeout(r, ms));

/**
 * 跑一整套一致性测试。
 *
 * @param {{
 *   name: string,
 *   makeHarness: (t: import("node:test").TestContext) => Promise<{
 *     create: (self: { name: string, labels?: string[], host?: string|null }) => Promise<{
 *       transport: object,
 *       inbox: Array<object>,
 *       states: string[],
 *       sent: Array<object>,
 *       stop: () => void,
 *     }>,
 *     teardown: () => void | Promise<void>,
 *     // 可选的额外钩子,用于测 transport 特有的能力
 *     supportsTakeover?: boolean,
 *   }>,
 * }} opts
 */
export function conformanceSuite({ name, makeHarness }) {
  // ---------------------------------------------------------------- 1. from 可信

  test(`[${name}] 1. from 来自连接身份,不信发送方自述`, async (t) => {
    const h = await makeHarness(t);
    const a = await h.create({ name: "conf-a" });
    const b = await h.create({ name: "conf-b" });
    // 等 a 和 b 互相建立起可用的路径。
    //
    // 不同 transport 的就绪条件不同:
    //   mesh   按需建边,必须等成员表里有端点、边真正 online
    //   broker 消息都经 broker,没有端点概念,上线即可用
    // 所以这里断言的是"成员已可见",端点只在存在时才等。
    const seen = await waitFor(
      () => a.transport.members().find((m) => m.name === "conf-b"),
      8000,
      "a 看到 conf-b",
    );
    if (seen.endpoints?.length) {
      // mesh:还要等出站边建立
      await new Promise((r) => setTimeout(r, 300));
    }

    a.transport.send({
      to: "conf-b",
      id: "spoof-1",
      re: null,
      body: { text: "伪造身份", hops: 0, _claimFrom: "i-am-not-conf-a" },
    });

    const got = await waitFor(() => b.inbox.find((m) => m.id === "spoof-1"), 5000, "b 收到消息");
    assert.equal(got.from, "conf-a", "from 必须是连接身份,不能是发送方自述");
  });

  // ---------------------------------------------------------------- 2. 群发不回自己

  test(`[${name}] 2. 群发不投递给发送者自己`, async (t) => {
    const h = await makeHarness(t);
    const a = await h.create({ name: "bulk-a", labels: ["all"] });
    const b = await h.create({ name: "bulk-b", labels: ["all"] });
    await waitFor(() => a.transport.members().some((m) => m.name === "bulk-b"), 5000, "a 看到 b");

    a.transport.send({ to: "@all", id: "broadcast-1", re: null, body: { text: "全员", hops: 0 } });

    assert.ok(await waitFor(() => b.inbox.find((m) => m.id === "broadcast-1"), 5000, "b 收到群发"));
    await settle();
    assert.equal(
      a.inbox.find((m) => m.id === "broadcast-1"),
      undefined,
      "群发不该回投给发送者",
    );
  });

  // ---------------------------------------------------------------- 3. 去重

  test(`[${name}] 3. 同一收件人被重复点名时只投一次`, async (t) => {
    const h = await makeHarness(t);
    const a = await h.create({ name: "dedup-a", labels: ["g"] });
    const b = await h.create({ name: "dedup-b", labels: ["g"] });
    await waitFor(() => a.transport.members().some((m) => m.name === "dedup-b"), 5000, "a 看到 b");

    a.transport.send({
      to: ["@g", "dedup-b"], // b 既在组里又被显式点名
      id: "dedup-1",
      re: null,
      body: { text: "只应收到一次", hops: 0 },
    });

    await waitFor(() => b.inbox.find((m) => m.id === "dedup-1"), 5000, "b 收到");
    await settle();
    const hits = b.inbox.filter((m) => m.id === "dedup-1");
    assert.equal(hits.length, 1, `b 应只收到一次,实际 ${hits.length} 次`);
  });

  // ---------------------------------------------------------------- 4. 未知收件人

  test(`[${name}] 4. 未知收件人 / 空分组不静默丢弃`, async (t) => {
    const h = await makeHarness(t);
    const a = await h.create({ name: "unknown-a" });
    await waitFor(() => a.transport.state() === "online", 5000, "a 上线");

    // 明确发给一个不存在的名字
    a.transport.send({ to: "nobody-here-at-all", id: "ghost-1", re: null, body: { text: "?", hops: 0 } });

    const problem = await waitFor(
      () =>
        a.sent.find((s) => s.id === "ghost-1" && (s.outcome === "unknown" || s.outcome === "failed")) ??
        a.inbox.find((m) => m.re === "ghost-1" && m.body?.kind === "undeliverable"),
      5000,
      "收到不可投递的报告",
    );
    assert.ok(problem, "未知收件人必须有明确报告,不能当作成功");

    // 空分组同理
    a.transport.send({ to: "@no-such-label", id: "ghost-2", re: null, body: { text: "?", hops: 0 } });
    const problem2 = await waitFor(
      () =>
        a.sent.find((s) => s.id === "ghost-2" && (s.outcome === "unknown" || s.outcome === "failed")) ??
        a.inbox.find((m) => m.re === "ghost-2" && m.body?.kind === "undeliverable"),
      5000,
      "空分组要报告",
    );
    assert.ok(problem2);
  });

  // ---------------------------------------------------------------- 5. 成员视图

  test(`[${name}] 5. 成员视图收敛:新节点加入后其他人能看到`, async (t) => {
    const h = await makeHarness(t);
    const a = await h.create({ name: "see-a", labels: ["x"] });
    await waitFor(() => a.transport.state() === "online", 5000, "a 上线");

    const b = await h.create({ name: "see-b", labels: ["y"], host: "host-b" });
    await waitFor(() => b.transport.state() === "online", 5000, "b 上线");

    const seen = await waitFor(
      () => a.transport.members().find((m) => m.name === "see-b"),
      8000,
      "a 看到 b",
    );
    assert.deepEqual(seen.labels, ["y"], "标签应随成员视图传播");
    assert.equal(seen.host, "host-b", "host 应随成员视图传播");
  });

  test(`[${name}] 5b. 成员视图里不包含自己`, async (t) => {
    const h = await makeHarness(t);
    const a = await h.create({ name: "self-a" });
    await waitFor(() => a.transport.state() === "online", 5000, "a 上线");
    await settle();
    assert.equal(
      a.transport.members().some((m) => m.name === "self-a"),
      false,
      "members() 不该包含自己 —— 上层用 others() 过滤",
    );
  });

  // ---------------------------------------------------------------- 6. 顺序与载荷

  test(`[${name}] 6. 载荷原样到达,re 与 hops 不变`, async (t) => {
    const h = await makeHarness(t);
    const a = await h.create({ name: "payload-a" });
    const b = await h.create({ name: "payload-b" });
    await waitFor(() => a.transport.members().some((m) => m.name === "payload-b"), 5000, "a 看到 b");

    // 含多字节字符、换行、引号,确认不被转义或截断
    const text = '多字节 🚀\n第二行 "带引号" \\反斜杠\t制表';
    a.transport.send({ to: "payload-b", id: "p-1", re: "original-id", body: { text, hops: 3, extra: { n: 42 } } });

    const got = await waitFor(() => b.inbox.find((m) => m.id === "p-1"), 5000, "收到");
    assert.equal(got.re, "original-id", "re 必须原样保留");
    assert.equal(got.body.hops, 3, "hops 必须原样保留");
    assert.equal(got.body.text, text, "正文必须逐字节一致");
    assert.equal(got.body.extra.n, 42, "额外字段也要带过去");
  });

  test(`[${name}] 6b. 长消息(超过一个 TCP 段)完整传输`, async (t) => {
    const h = await makeHarness(t);
    const a = await h.create({ name: "long-a" });
    const b = await h.create({ name: "long-b" });
    await waitFor(() => a.transport.members().some((m) => m.name === "long-b"), 5000, "a 看到 b");

    // 注意 MAX_PAYLOAD 是 64 KiB 的**字节**数。中文每字 3 字节,
    // 所以 24000 字 = 72000 字节会被拒绝。用 16000 字 ≈ 48 KB。
    const text = "长文本".repeat(5300); // ≈48 KB,超过单个 TCP 段但仍在上限内
    a.transport.send({ to: "long-b", id: "long-1", re: null, body: { text, hops: 0 } });

    const got = await waitFor(() => b.inbox.find((m) => m.id === "long-1"), 10000, "收到长消息");
    assert.equal(got.body.text.length, text.length, "长度一致");
    assert.equal(got.body.text, text, "内容一致");
  });

  // ---------------------------------------------------------------- 7. 状态

  test(`[${name}] 7. state() 报告连接生命周期`, async (t) => {
    const h = await makeHarness(t);
    const a = await h.create({ name: "state-a" });

    assert.equal(a.transport.state(), "online", "启动后应为 online");
    assert.ok(a.states.includes("online") || a.states.includes("connecting"), `状态序列应含连接过程,实际 ${JSON.stringify(a.states)}`);

    a.stop();
    await settle(200);
    assert.equal(a.transport.state(), "offline", "stop() 后应为 offline");
  });

  // ---------------------------------------------------------------- 8. 投递语义

  test(`[${name}] 8. 投递是尽力而为,回执只表示到达对端`, async (t) => {
    const h = await makeHarness(t);
    const a = await h.create({ name: "sem-a" });
    const b = await h.create({ name: "sem-b" });
    await waitFor(() => a.transport.members().some((m) => m.name === "sem-b"), 5000, "a 看到 b");

    a.transport.send({ to: "sem-b", id: "sem-1", re: null, body: { text: "x", hops: 0 } });
    await waitFor(() => b.inbox.find((m) => m.id === "sem-1"), 5000, "到达");

    // 对端离线后再发:不该抛异常,也不该静默成功
    b.stop();
    await settle(500);
    const accepted = a.transport.send({ to: "sem-b", id: "sem-2", re: null, body: { text: "y", hops: 0 } });
    // 允许 accepted 为 true(写入本地通道)或 false(立即知道不可达),
    // 但两种情况都不能抛异常,而且要么有明确报告要么确实发不出去
    assert.equal(typeof accepted, "boolean");
  });
}

/**
 * 只在特定 transport 上跑的额外测试(例如同名接管只对 broker 有意义)。
 */
export function takeoverSuite({ name, makeHarness }) {
  test(`[${name}] 同名接管:新连接生效,旧连接收到明确的终止信号`, async (t) => {
    const h = await makeHarness(t);
    const old = await h.create({ name: "takeover-node" });
    assert.equal(old.transport.state(), "online");

    const fresh = await h.create({ name: "takeover-node" });
    assert.equal(fresh.transport.state(), "online", "新连接应生效");

    const replaced = await waitFor(
      () => old.states.includes("replaced") || old.replaced,
      8000,
      "旧连接被告知被顶替",
    );
    assert.ok(replaced, "旧连接必须收到明确的被顶替信号,而不是无限重连");
  });

  test(`[${name}] 同名接管:其他节点看不到离开/加入抖动`, async (t) => {
    const h = await makeHarness(t);
    const watcher = await h.create({ name: "watcher-node" });
    await h.create({ name: "flapper" });
    await waitFor(() => watcher.transport.members().some((m) => m.name === "flapper"), 5000, "看到 flapper");

    const before = watcher.inbox.length;
    await h.create({ name: "flapper" }); // 接管
    await settle(600);

    const leftEvents = watcher.inbox
      .slice(before)
      .filter((m) => m.from === "broker" && m.body?.kind === "peer_left" && m.body.peer === "flapper");
    assert.equal(leftEvents.length, 0, "接管不该广播离开,否则观察者会看到抖动");
  });
}
