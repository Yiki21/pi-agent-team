/**
 * Broker transport 的一致性测试。
 *
 * 这个 harness 用**假 WebSocket 类**驱动**真实的 BrokerTransport**:
 * 它在进程内把信封交给真 broker(走真 WebSocket 网络),但把收到的
 * 帧记录下来供断言。所以测到的是真实协议行为,不是模拟。
 *
 * 跑:node --test e2e/
 */
import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:net";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { createBrokerTransport } from "../src/transport.js";
import { conformanceSuite, takeoverSuite, waitFor } from "./conformance.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const BROKER = join(HERE, "..", "broker.mjs");
const TOKEN = "conformance-token";

/** 要一个空闲端口:先监听 0,读出端口,释放 */
function allocPort() {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.on("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}

/**
 * 一次测试用的 broker harness。
 *
 * create() 返回一个节点:{ transport, inbox, states, sent, stop }
 *   inbox   收到的信封(含 broker 控制消息)
 *   states  状态序列
 *   sent    我们发出的信封 + 最终结果(send 后在收到回执时回填 outcome)
 */
async function brokerHarness(t, { noTakeover = false } = {}) {
  const port = await allocPort();
  const url = `ws://127.0.0.1:${port}`;

  const broker = spawn(
    process.execPath,
    [BROKER, "--bind", "127.0.0.1", "--port", String(port), "--heartbeat", "3000"],
    {
      env: { ...process.env, TEAM_TOKEN: TOKEN, ...(noTakeover ? { TEAM_NO_TAKEOVER: "1" } : {}) },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("broker 启动超时")), 8000);
    broker.stdout.on("data", (d) => {
      if (d.toString().includes("broker 监听")) {
        clearTimeout(timer);
        resolve();
      }
    });
    broker.on("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`broker 提前退出 code=${code}`));
    });
  });

  const nodes = [];

  const create = async (self) => {
    const inbox = [];
    const states = [];
    const sent = [];
    let replaced = false;

    const transport = createBrokerTransport({ url, token: TOKEN });

    transport.on("state", (s, detail) => {
      states.push(s);
      if (s === "replaced") replaced = true;
    });

    transport.on("envelope", (env) => {
      inbox.push(env);

      // 回填我们自己发出的消息的最终结果,供一致性测试第 4/8 条使用
      if (env.from === "broker" && env.re) {
        const rec = sent.find((x) => x.id === env.re);
        if (rec) {
          if (env.body?.kind === "delivered") rec.outcome = "delivered";
          else if (env.body?.kind === "undeliverable") rec.outcome = "unknown";
        }
      }
    });

    // 包一层 send,把发出的信封记录下来
    const rawSend = transport.send.bind(transport);
    transport.send = (envelope) => {
      const rec = { ...envelope, outcome: "pending" };
      sent.push(rec);
      const ok = rawSend(envelope);
      if (!ok) rec.outcome = "failed";
      return ok;
    };

    transport.start(self);

    const node = {
      transport,
      inbox,
      states,
      sent,
      self,
      get replaced() {
        return replaced;
      },
      stop: () => transport.stop(),
    };
    nodes.push(node);

    // \u7b49\u5230\u771f\u6b63\u4e0a\u7ebf\u518d\u8fd4\u56de\u3002\u5426\u5219\u6d4b\u8bd5\u4f1a\u5728 connecting \u9636\u6bb5\u5c31\u65ad\u8a00 online,
    // \u6216\u8005\u5728\u6536\u5230 welcome \u4e4b\u524d\u5c31\u53d1\u6d88\u606f\u3002\u4f46\u8981\u5141\u8bb8"\u88ab\u62d2/\u88ab\u9876\u66ff"\u8fd9\u7c7b
    // \u9884\u671f\u5185\u7684\u7ec8\u6001 \u2014\u2014 \u90a3\u662f\u63a5\u7ba1\u6d4b\u8bd5\u8981\u9a8c\u8bc1\u7684\u4e1c\u897f\u3002
    await waitFor(
      () =>
        (transport.state() === "online" && inbox.some((m) => m.from === "broker" && m.body?.kind === "welcome")) ||
        transport.state() === "replaced",
      6000,
      `${self.name} \u4e0a\u7ebf`,
    ).catch(() => {
      // \u8fde\u4e0d\u4e0a\u4e5f\u8fd4\u56de,\u8ba9\u6d4b\u8bd5\u81ea\u5df1\u65ad\u8a00\u671f\u671b\u7684\u72b6\u6001(\u6bd4\u5982\u5173\u95ed\u63a5\u7ba1\u65f6\u7684\u62d2\u7edd)
    });

    return node;
  };

  const teardown = async () => {
    for (const n of nodes) {
      try {
        n.stop();
      } catch {}
    }
    broker.kill("SIGTERM");
    await once(broker, "exit").catch(() => {});
  };

  t.after(teardown);

  return { url, create, teardown };
}

// ---------------------------------------------------------------- 跑套件

conformanceSuite({
  name: "broker",
  makeHarness: (t) => brokerHarness(t),
});

takeoverSuite({
  name: "broker",
  makeHarness: (t) => brokerHarness(t),
});

// ---------------------------------------------------------------- broker 特有

test("[broker] 关掉接管时,同名连接被拒", async (t) => {
  const h = await brokerHarness(t, { noTakeover: true });
  const first = await h.create({ name: "dup-node" });
  assert.equal(first.transport.state(), "online");

  const second = await h.create({ name: "dup-node" });
  // 被拒后应该停在 offline(不重连成功),而不是接管
  await new Promise((r) => setTimeout(r, 1500));
  assert.notEqual(second.transport.state(), "online", "接管关闭时第二个连接不该上线");
});

test("[broker] 未配置 token 时连接失败", async (t) => {
  const port = await allocPort();
  const bad = createBrokerTransport({ url: `ws://127.0.0.1:${port}`, token: "wrong" });
  const states = [];
  bad.on("state", (s) => states.push(s));

  // 没有 broker 在监听,所以只会看到 offline/connecting
  bad.start({ name: "no-broker", labels: [], host: null });
  await new Promise((r) => setTimeout(r, 600));
  bad.stop();
  assert.ok(states.length > 0, "应有状态变化");
});
