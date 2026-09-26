/**
 * broker 端到端测试。用 Node 原生 WebSocket 当客户端 ——
 * 和扩展用的是同一个实现,所以测出来的是真实行为。
 *
 * 跑:node --test e2e/
 *
 * 每个测试起一个独立 broker(自己的端口)。共用 broker 会让
 * roster 互相污染 —— 而 roster 恰恰是这里要测的东西之一。
 */
import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const HERE = dirname(fileURLToPath(import.meta.url));
const BROKER = join(HERE, "..", "broker.mjs");
const TOKEN = "test-token-do-not-use-in-production";

let nextPort = 19000 + (process.pid % 500);
const allocPort = () => nextPort++;

/**
 * 起一个隔离的 broker,返回一个带 connect/close 的测试上下文。
 */
async function withBroker(t) {
  const port = allocPort();
  const base = `ws://127.0.0.1:${port}`;
  const sockets = [];

  const broker = spawn(
    process.execPath,
    [BROKER, "--bind", "127.0.0.1", "--port", String(port), "--heartbeat", "2000"],
    { env: { ...process.env, TEAM_TOKEN: TOKEN }, stdio: ["ignore", "pipe", "pipe"] },
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

 /** 连一个假节点;正确 token 时等到 welcome。 */
  const connect = async (name, token = TOKEN) => {
    const ws = new WebSocket(`${base}?name=${encodeURIComponent(name)}&token=${encodeURIComponent(token)}`);
    sockets.push(ws);
    const inbox = [];
    ws.addEventListener("message", (ev) => inbox.push(JSON.parse(ev.data)));

    if (token !== TOKEN) {
      await Promise.race([once(ws, "close"), once(ws, "error")]);
      return { ws, inbox, rejected: true };
    }

    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`${name} welcome 超时`)), 5000);
      ws.addEventListener("message", (ev) => {
        if (JSON.parse(ev.data).body?.kind === "welcome") {
          clearTimeout(timer);
          resolve();
        }
      });
      ws.addEventListener("error", () => {
        clearTimeout(timer);
        reject(new Error(`${name} 连接错误`));
      });
    });

    return { ws, inbox, rejected: false };
  };

  const cleanup = async () => {
    for (const ws of sockets) {
      try {
        ws.close();
      } catch {}
    }
    broker.kill("SIGTERM");
    await once(broker, "exit").catch(() => {});
  };

  t.after(cleanup);
  return { base, connect, port };
}

const waitFor = (predicate, ms = 3000) =>
  new Promise((resolve, reject) => {
    const t0 = Date.now();
    const tick = () => {
      const v = predicate();
      if (v) return resolve(v);
      if (Date.now() - t0 > ms) return reject(new Error("waitFor 超时"));
      setTimeout(tick, 20);
    };
    tick();
  });

test("正确 token 能连上并收到 welcome", async (t) => {
  const { connect } = await withBroker(t);
  const a = await connect("alpha");
  assert.equal(a.rejected, false);
  const welcome = a.inbox.find((m) => m.body?.kind === "welcome");
  assert.equal(welcome.body.peer, "alpha");
  assert.deepEqual(welcome.body.peers, [], "第一个连接的节点应该看不到别人");
});

test("错误 token 被拒绝", async (t) => {
  const { connect } = await withBroker(t);
  const bad = await connect("intruder", "wrong-token");
  assert.equal(bad.rejected, true, "错误的 token 必须连不上");
});

test("presence:第二个节点加入时第一个收到 peer_joined", async (t) => {
  const { connect } = await withBroker(t);
  const a = await connect("p1");
  const b = await connect("p2");

  const joined = await waitFor(() => a.inbox.find((m) => m.body?.kind === "peer_joined"));
  assert.equal(joined.body.peer, "p2");
  assert.deepEqual([...joined.body.peers].sort(), ["p1", "p2"]);

  const welcome = b.inbox.find((m) => m.body?.kind === "welcome");
  assert.deepEqual(welcome.body.peers, ["p1"], "新节点应在 welcome 里看到已有节点");
});

test("presence:节点离开时其他人收到 peer_left", async (t) => {
  const { connect } = await withBroker(t);
  const a = await connect("q1");
  const b = await connect("q2");
  await waitFor(() => a.inbox.some((m) => m.body?.kind === "peer_joined"));

  b.ws.close();
  const left = await waitFor(() => a.inbox.find((m) => m.body?.kind === "peer_left"));
  assert.equal(left.body.peer, "q2");
  assert.deepEqual(left.body.peers, ["q1"], "离开后 roster 不应再包含它");
});

test("单播投递:内容原样到达,并回到执", async (t) => {
  const { connect } = await withBroker(t);
  const a = await connect("s1");
  const b = await connect("s2");
  await waitFor(() => a.inbox.some((m) => m.body?.kind === "peer_joined"));

  const id = "msg-0001";
  a.ws.send(JSON.stringify({ from: "s1", to: "s2", id, re: null, body: { text: "你好 s2" } }));

  const got = await waitFor(() => b.inbox.find((m) => m.id === id));
  assert.equal(got.from, "s1");
  assert.equal(got.to, "s2");
  assert.deepEqual(got.body, { text: "你好 s2" });

  const ack = await waitFor(() => a.inbox.find((m) => m.body?.kind === "delivered" && m.re === id));
  assert.equal(ack.body.to, "s2");
});

test("广播:一次发给所有其他节点,不回声给自己", async (t) => {
  const { connect } = await withBroker(t);
  const a = await connect("b1");
  const b = await connect("b2");
  const c = await connect("b3");
  await waitFor(() => a.inbox.filter((m) => m.body?.kind === "peer_joined").length >= 2);

  const id = "bc-0001";
  a.ws.send(JSON.stringify({ from: "b1", to: "*", id, re: null, body: { text: "全员" } }));

  assert.ok(await waitFor(() => b.inbox.find((m) => m.id === id)));
  assert.ok(await waitFor(() => c.inbox.find((m) => m.id === id)));
  assert.equal(a.inbox.find((m) => m.id === id), undefined, "广播不应回声给自己");
});

test("离线对端:立刻回执 undeliverable,不静默排队", async (t) => {
  const { connect } = await withBroker(t);
  const a = await connect("d1");
  const id = "gone-1";
  a.ws.send(JSON.stringify({ from: "d1", to: "nobody-here", id, re: null, body: { text: "?" } }));

  const reply = await waitFor(() => a.inbox.find((m) => m.re === id));
  assert.equal(reply.body.kind, "undeliverable");
  assert.equal(reply.body.reason, "offline");
});

test("from 伪造被覆盖为真实连接身份", async (t) => {
  const { connect } = await withBroker(t);
  const a = await connect("f1");
  const b = await connect("f2");
  await waitFor(() => a.inbox.some((m) => m.body?.kind === "peer_joined"));

  a.ws.send(JSON.stringify({ from: "i-am-someone-else", to: "f2", id: "spoof-1", re: null, body: {} }));

  const got = await waitFor(() => b.inbox.find((m) => m.id === "spoof-1"));
  assert.equal(got.from, "f1", "broker 必须用连接身份覆盖自报的 from");
});

test("畸形信封被丢弃,不导致崩溃", async (t) => {
  const { connect } = await withBroker(t);
  const a = await connect("m1");
  a.ws.send("这不是 JSON");
  a.ws.send(JSON.stringify({ from: "m1" })); // 缺字段
  a.ws.send(JSON.stringify({ from: "m1", to: "x", id: "y", re: null })); // 缺 body

  // broker 应该还活着并继续服务
  const b = await connect("m2");
  assert.equal(b.rejected, false, "畸形输入不应让 broker 失去服务能力");
});

test("名字冲突被拒绝", async (t) => {
  const { base, connect } = await withBroker(t);
  await connect("dup");

  const second = new WebSocket(`${base}?name=dup&token=${encodeURIComponent(TOKEN)}`);
  await Promise.race([once(second, "close"), once(second, "error")]);
  assert.ok(
    second.readyState === WebSocket.CLOSED || second.readyState === WebSocket.CLOSING,
    "重复名字必须被拒绝",
  );
});

test("非法名字被拒绝", async (t) => {
  const { base } = await withBroker(t);
  const bad = new WebSocket(`${base}?name=${encodeURIComponent("BAD NAME!")}&token=${encodeURIComponent(TOKEN)}`);
  await Promise.race([once(bad, "close"), once(bad, "error")]);
  assert.ok(bad.readyState === WebSocket.CLOSED || bad.readyState === WebSocket.CLOSING);
});

// 这条是回归测试:名字正则曾经只允许小写,导致 "poetA" 这种驼峰名
// 被拒,客户端表现为无限重连(409 + 指数退避)。
test("大写/驼峰名字被接受", async (t) => {
  const { connect } = await withBroker(t);
  const a = await connect("poetA");
  assert.equal(a.rejected, false, "驼峰名应该合法");
  const b = await connect("Server-01");
  assert.equal(b.rejected, false, "首字母大写 + 连字符应该合法");
});

test("健康端点反映在线节点", async (t) => {
  const { connect, port } = await withBroker(t);
  await connect("h1");

  const res = await fetch(`http://127.0.0.1:${port}/health`);
  assert.equal(res.status, 200);
  const body = await res.json();
  assert.equal(body.ok, true);
  assert.deepEqual(body.peers, ["h1"]);
});

test("多字节 UTF-8 消息跨 WebSocket 完整传输", async (t) => {
  const { connect } = await withBroker(t);
  const a = await connect("u1");
  const b = await connect("u2");
  await waitFor(() => a.inbox.some((m) => m.body?.kind === "peer_joined"));

  // 超过 125 字节 → 走 16 位长度分支
  const text = "中文测试🚀".repeat(30);
  a.ws.send(JSON.stringify({ from: "u1", to: "u2", id: "utf8-1", re: null, body: { text } }));

  const got = await waitFor(() => b.inbox.find((m) => m.id === "utf8-1"));
  assert.equal(got.body.text, text);
});
