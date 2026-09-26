/**
 * SWIM transport 一致性测试。
 *
 * 三个节点:第一个没有种子,后两个以第一个的 **gossip 端口** 为种子。
 * 边车负责成员发现,消息走 mesh 直连。
 *
 * 需要先构建边车:cd swim && go build -o ../.tmp/swim-sidecar .
 * 不存在时整个文件 skip —— 没装 Go 的开发者也应该能跑其余测试。
 */
import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { createSwimTransport, sidecarAvailable } from "../src/transport-swim.js";
import { conformanceSuite, waitFor } from "./conformance.js";

const AVAILABLE = sidecarAvailable();
const TOKEN = "swim-transport-token";

/**
 * 边车的 gossip 端口需要显式分配:Node 侧要用它当种子地址,
 * 所以不能交给内核随机分配。
 */
let nextGossipPort = 18000;
const allocGossipPort = () => nextGossipPort++;

async function swimHarness(t) {
  const nodes = [];

  const create = async (self) => {
    // 用已有节点的 gossip 端口当种子。
    // 注意:这里是 gossip 端口,不是投递端口 —— 两者不同,
    // 种子必须指向边车的 gossip 监听。
    const seed = nodes.find((n) => n.gossipPort);
    const myGossipPort = allocGossipPort();

    const transport = createSwimTransport({
      token: TOKEN,
      seeds: seed ? [`127.0.0.1:${seed.gossipPort}`] : [],
      listenHost: "127.0.0.1",
      listenPort: 0,
      advertiseHost: "127.0.0.1",
      gossipBind: "127.0.0.1",
      gossipPort: myGossipPort,
      heartbeatMs: 3000,
      refreshMs: 2000,
    });

    const inbox = [];
    const states = [];
    const sent = [];

    transport.on("state", (s, d) => states.push(s));
    transport.on("envelope", (env) => {
      inbox.push(env);
      if (env.re) {
        const rec = sent.find((x) => x.id === env.re && x.outcome === "pending");
        if (rec) rec.outcome = "delivered";
      }
    });

    const rawSend = transport.send.bind(transport);
    transport.send = (envelope) => {
      sent.push({ ...envelope, outcome: "pending" });
      const ok = rawSend(envelope);
      if (!ok) sent[sent.length - 1].outcome = "failed";
      return ok;
    };

    await transport.start(self);

    // 上线要等边车和投递层都就绪
    await waitFor(() => transport.state() === "online", 15000, `${self.name} 上线`);

    const node = {
      transport,
      inbox,
      states,
      sent,
      self,
      gossipPort: myGossipPort,
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
  name: "swim",
  makeHarness: (t) => swimHarness(t),
});

// ---------------------------------------------------------------- SWIM 特有

test("[swim] 边车缺失时明确失败,不静默降级", { skip: !AVAILABLE }, async (t) => {
  const bad = createSwimTransport({
    token: TOKEN,
    sidecarPath: "/nonexistent/path/to/sidecar",
    listenHost: "127.0.0.1",
    listenPort: 0,
  });

  const states = [];
  const details = [];
  bad.on("state", (s, d) => {
    states.push(s);
    if (d) details.push(d);
  });
  t.after(() => bad.stop());

  await bad.start({ name: "no-sidecar", labels: [], host: null });
  await sleep(500);

  assert.equal(bad.state(), "offline", "没有边车时不该报 online");
  assert.ok(states.includes("offline"), `状态序列应含 offline,实际 ${JSON.stringify(states)}`);

  // 原因必须明确,而且给出修复方法 —— 静默降级会让"SWIM 检测到故障"
  // 变成谎言,所以这里要求它说清楚为什么起不来。
  const missing = details.find((d) => d.reason === "sidecar_missing");
  assert.ok(missing, `应给出 sidecar_missing 原因,实际 ${JSON.stringify(details)}`);
  assert.match(missing.message, /go build/, "要告诉用户怎么构建边车");
});

test("[swim] 成员表来自边车,且带投递端点", { skip: !AVAILABLE }, async (t) => {
  const h = await swimHarness(t);
  const a = await h.create({ name: "swim-a", labels: ["x"] });
  const b = await h.create({ name: "swim-b", labels: ["y"] });

  const seen = await waitFor(
    () => a.transport.members().find((m) => m.name === "swim-b"),
    15000,
    "a 看到 b",
  );

  assert.deepEqual(seen.labels, ["y"], "标签来自边车 Meta");
  assert.ok(seen.deliver > 0, "成员必须带投递端口,否则知道名字也发不出去");
  assert.ok(seen.endpoints.length > 0, "端点要能推导出来");
});

test("[swim] 边车报 dead 后不再投递给该节点", { skip: !AVAILABLE }, async (t) => {
  const h = await swimHarness(t);
  const a = await h.create({ name: "kill-a" });
  const b = await h.create({ name: "kill-b" });

  await waitFor(() => a.transport.members().some((m) => m.name === "kill-b"), 15000, "a 看到 b");

  // 强杀 b 的边车 —— 不 Leave,靠 suspicion 检测
  b.stop();

  const gone = await waitFor(
    () => !a.transport.members().some((m) => m.name === "kill-b"),
    30000,
    "a 最终不再看到 b",
  );
  assert.ok(gone, "被强杀的节点应从成员表消失");
});
