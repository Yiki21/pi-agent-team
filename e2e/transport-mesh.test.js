/**
 * Mesh transport 一致性测试。
 *
 * 每个节点监听自己的端口(内核分配),互相用 127.0.0.1 直连。
 * 第一个节点是种子,后续节点通过它加入 —— 这条路径本身就是
 * "只需认识一个节点"的验证。
 *
 * 跑:node --test e2e/
 */
import test from "node:test";
import assert from "node:assert/strict";
import { createMeshTransport, parseSeed, resolveTargets } from "../src/transport-mesh.js";
import { conformanceSuite, waitFor } from "./conformance.js";

const TOKEN = "mesh-conformance-token";

/**
 * Mesh harness。
 *
 * 注意成员发现:第一个节点没有种子可连,所以它的成员表要靠后续节点
 * 主动连上来才填满。后续节点连第一个,并通过 hello 扩散知道彼此。
 */
async function meshHarness(t) {
  const nodes = [];

  const create = async (self) => {
    // 已有节点里随便挑一个当种子
    const seed = nodes.find((n) => n.transport.port());

    const transport = createMeshTransport({
      token: TOKEN,
      seeds: seed ? [`127.0.0.1:${seed.transport.port()}`] : [],
      listenHost: "127.0.0.1",
      listenPort: 0,
      advertiseHost: "127.0.0.1",
      heartbeatMs: 3000,
    });

    const inbox = [];
    const states = [];
    const sent = [];

    transport.on("state", (s) => states.push(s));
    transport.on("envelope", (env) => {
      inbox.push(env);
      if (env.from !== "broker" && env.re) {
        const rec = sent.find((x) => x.id === env.re);
        if (rec && rec.outcome === "pending") rec.outcome = "delivered";
      }
    });

    const rawSend = transport.send.bind(transport);
    transport.send = (envelope) => {
      sent.push({ ...envelope, outcome: "pending" });
      const ok = rawSend(envelope);
      if (!ok) {
        const rec = sent[sent.length - 1];
        rec.outcome = "failed";
      }
      return ok;
    };

    transport.start(self);

    await waitFor(() => transport.state() === "online", 6000, `${self.name} 监听就绪`);

    const node = {
      transport,
      inbox,
      states,
      sent,
      self,
      stop: () => transport.stop(),
    };
    nodes.push(node);
    return node;
  };

  const teardown = () => {
    for (const n of nodes) {
      try {
        n.stop();
      } catch {}
    }
  };

  t.after(teardown);

  return { create, teardown };
}

conformanceSuite({
  name: "mesh",
  makeHarness: (t) => meshHarness(t),
});

// ---------------------------------------------------------------- mesh 特有

test("[mesh] parseSeed 接受多种写法", () => {
  assert.deepEqual(parseSeed("host:9000"), { addr: "host", port: 9000 });
  assert.deepEqual(parseSeed("ws://host:9000"), { addr: "host", port: 9000 });
  assert.deepEqual(parseSeed("wss://host:9000/path?x=1"), { addr: "host", port: 9000 });
  assert.deepEqual(parseSeed("host"), { addr: "host", port: 8788 });
  assert.deepEqual(parseSeed("[::1]:9000"), { addr: "::1", port: 9000 });
  assert.deepEqual(parseSeed("  host:9000  "), { addr: "host", port: 9000 });
  assert.equal(parseSeed(""), null);
  assert.equal(parseSeed(null), null);
});

test("[mesh] resolveTargets 语法与 session 层一致", () => {
  const members = [
    { name: "a", labels: ["web"] },
    { name: "b", labels: ["web", "db"] },
    { name: "c", labels: [] },
  ];

  assert.deepEqual(resolveTargets("a", "self", members).sort(), ["a"]);
  assert.deepEqual(resolveTargets("@web", "self", members).sort(), ["a", "b"]);
  assert.deepEqual(resolveTargets("#web", "self", members).sort(), ["a", "b"], "旧 # 语法也认");
  assert.deepEqual(resolveTargets("@default", "self", members).sort(), ["a", "b", "c"]);
  assert.deepEqual(resolveTargets("*", "self", members).sort(), ["a", "b", "c"]);
  assert.deepEqual(resolveTargets(["a", "@web"], "self", members).sort(), ["a", "b"], "并集去重");
  assert.deepEqual(resolveTargets("nope", "self", members), [], "未知名字不匹配");
  assert.deepEqual(resolveTargets("*", "a", members).sort(), ["b", "c"], "不包含自己");
});

test("[mesh] 没有种子时第一个节点仍能上线并接受连接", async (t) => {
  const h = await meshHarness(t);
  const a = await h.create({ name: "solo-a" });
  assert.equal(a.transport.state(), "online");
  assert.ok(a.transport.port() > 0, "应绑定到一个真实端口");
});

test("[mesh] 后加入的节点通过种子被第一个节点看到", async (t) => {
  const h = await meshHarness(t);
  const a = await h.create({ name: "seed-node" });
  const b = await h.create({ name: "joiner", labels: ["web"] });

  const seen = await waitFor(
    () => a.transport.members().find((m) => m.name === "joiner"),
    8000,
    "种子节点看到新加入者",
  );
  assert.deepEqual(seen.labels, ["web"], "标签应随 hello 传播");
});

test("[mesh] 两个后来者通过种子互相发现(不需要彼此配置)", async (t) => {
  const h = await meshHarness(t);
  const seed = await h.create({ name: "hub" });
  const b = await h.create({ name: "leaf-b" });
  const c = await h.create({ name: "leaf-c" });

  // b 和 c 之间没有直接配置,全靠 hub 扩散
  const bSeesC = await waitFor(
    () => b.transport.members().some((m) => m.name === "leaf-c"),
    10000,
    "b 看到 c",
  );
  const cSeesB = await waitFor(
    () => c.transport.members().some((m) => m.name === "leaf-b"),
    10000,
    "c 看到 b",
  );
  assert.ok(bSeesC && cSeesB);
});

test("[mesh] 直连投递:两个非种子节点之间能直接发消息", async (t) => {
  const h = await meshHarness(t);
  await h.create({ name: "hub2" });
  const b = await h.create({ name: "sender" });
  const c = await h.create({ name: "receiver" });

  await waitFor(() => b.transport.members().some((m) => m.name === "receiver"), 10000, "sender 看到 receiver");

  b.transport.send({ to: "receiver", id: "direct-1", re: null, body: { text: "直连消息", hops: 0 } });

  const got = await waitFor(() => c.inbox.find((m) => m.id === "direct-1"), 8000, "receiver 收到");
  assert.equal(got.from, "sender");
  assert.equal(got.body.text, "直连消息");
});

test("[mesh] 错误 token 的入站连接被拒绝", async (t) => {
  const h = await meshHarness(t);
  const good = await h.create({ name: "guard" });

  const bad = createMeshTransport({
    token: "wrong-token",
    seeds: [`127.0.0.1:${good.transport.port()}`],
    listenHost: "127.0.0.1",
    listenPort: 0,
  });
  const badStates = [];
  bad.on("state", (s) => badStates.push(s));
  bad.start({ name: "intruder", labels: [], host: null });

  // 给它足够时间尝试连接
  await new Promise((r) => setTimeout(r, 2000));

  assert.equal(
    good.transport.members().some((m) => m.name === "intruder"),
    false,
    "错误 token 的节点不该出现在成员表里",
  );
  bad.stop();
});

test("[mesh] stop() 之后端口释放", async (t) => {
  const h = await meshHarness(t);
  const a = await h.create({ name: "temp-node" });
  const port = a.transport.port();
  assert.ok(port > 0);

  a.stop();
  await new Promise((r) => setTimeout(r, 500));

  // 端口应能重新绑定
  const again = createMeshTransport({ token: TOKEN, listenHost: "127.0.0.1", listenPort: port });
  const states = [];
  again.on("state", (s) => states.push(s));
  again.start({ name: "rebind", labels: [], host: null });
  await waitFor(() => again.state() === "online", 5000, "端口可重新绑定");
  assert.equal(again.state(), "online");
  again.stop();
});
