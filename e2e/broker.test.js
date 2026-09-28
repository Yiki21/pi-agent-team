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

import { createServer } from "node:net";

/**
 * 要一个空闲端口。
 *
 * 之前是 `19000 + (pid % 500)`,在多个测试文件并行时(各自一个
 * Node 进程)会碰撞,表现为随机的"welcome 超时"。让内核分配
 * 才是可靠的:先监听 0,读出端口,然后释放。
 *
 * 这里有 TOCTOU 窗口(释放到 broker 绑定之间),但窗口极小,
 * 而且比基于 pid 的确定性碰撞好得多。
 */
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
 * 起一个隔离的 broker,返回一个带 connect/close 的测试上下文。
 */
async function withBroker(t, { noTakeover = false, heartbeat = 2000 } = {}) {
  const port = await allocPort();
  const base = `ws://127.0.0.1:${port}`;
  const sockets = [];

  const broker = spawn(
    process.execPath,
    [BROKER, "--bind", "127.0.0.1", "--port", String(port), "--heartbeat", String(heartbeat)],
    {
      env: {
        ...process.env,
        TEAM_TOKEN: TOKEN,
        ...(noTakeover ? { TEAM_NO_TAKEOVER: "1" } : {}),
      },
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

  /**
   * 连一个节点。
   * @param name 节点名
   * @param opts.token 覆盖 token
   * @param opts.tags tag 列表(逗号分隔)
   * @param opts.host 机器名
   * @param opts.expectClose 预期被关闭(接管/拒绝场景),等 close 再返回
   */
  const connect = async (name, opts = {}) => {
    const { token = TOKEN, tags = "", host = "", expectClose = false } = opts;
    const qs = new URLSearchParams({ name, token });
    if (tags) qs.set("tags", tags);
    if (host) qs.set("host", host);

    const ws = new WebSocket(`${base}?${qs}`);
    sockets.push(ws);
    const inbox = [];
    const closes = [];
    ws.addEventListener("message", (ev) => inbox.push(JSON.parse(ev.data)));
    ws.addEventListener("close", (ev) => closes.push({ code: ev.code }));

    if (token !== TOKEN || expectClose) {
      await Promise.race([once(ws, "close"), once(ws, "error")]);
      return { ws, inbox, closes, rejected: true };
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

    return { ws, inbox, closes, rejected: false };
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
  const bad = await connect("intruder", { token: "wrong-token" });
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

// 注意语义变化:早期版本把"收件人不在线"回执为 reason:"offline",
// 但 broker 不维护任何持久成员表,它无法区分"下线了"和"从未存在过"。
// 现在统一用 unknown_recipient —— 这是诚实的那一个。友好措辞
// ("不在线或不存在")由扩展层负责。
test("离线对端:立刻回执,不静默排队", async (t) => {
  const { connect } = await withBroker(t);
  const a = await connect("d1");
  const id = "gone-1";
  a.ws.send(JSON.stringify({ from: "d1", to: "nobody-here", id, re: null, body: { text: "?" } }));

  const reply = await waitFor(() => a.inbox.find((m) => m.re === id));
  assert.equal(reply.body.kind, "undeliverable");
  assert.equal(reply.body.reason, "unknown_recipient");
  assert.deepEqual(reply.body.unknown, ["nobody-here"]);
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

// 默认是接管语义;"拒绝冲突"只在 TEAM_NO_TAKEOVER=1 时生效。
// 保留两条路径的测试,因为拒绝路径是接管逻辑的对照组。
test("名字冲突:关闭接管时返回 409", async (t) => {
  const { base, connect } = await withBroker(t, { noTakeover: true });
  await connect("dup");

  const second = new WebSocket(`${base}?name=dup&token=${encodeURIComponent(TOKEN)}`);
  await Promise.race([once(second, "close"), once(second, "error")]);
  assert.ok(
    second.readyState === WebSocket.CLOSED || second.readyState === WebSocket.CLOSING,
    "接管关闭时,重复名字必须被拒绝",
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

// ================================================================ 同名接管

test("同名接管:新连接踢掉旧连接,旧连接收到 4001", async (t) => {
  const { connect } = await withBroker(t);
  const old = await connect("worker");

  const fresh = await connect("worker");
  assert.equal(fresh.rejected, false, "新连接必须被接受");

  // 旧连接应被 broker 用 4001 关闭 —— 客户端靠这个码判断"不要重连"
  await waitFor(() => old.closes.length > 0);
  assert.equal(old.closes[0].code, 4001, "被顶掉的连接应收到 4001 replaced");
});

test("同名接管:其他节点不会看到 peer_left 抖动", async (t) => {
  const { connect } = await withBroker(t);
  const watcher = await connect("watcher");
  await connect("worker");
  await waitFor(() => watcher.inbox.some((m) => m.body?.kind === "peer_joined" && m.body.peer === "worker"));

  const before = watcher.inbox.length;
  await connect("worker"); // 接管
  // 给 broker 时间把任何假事件发出来
  await new Promise((r) => setTimeout(r, 300));

  const after = watcher.inbox.slice(before);
  const left = after.filter((m) => m.body?.kind === "peer_left" && m.body.peer === "worker");
  assert.equal(left.length, 0, "接管不应广播 peer_left —— 对观察者来说节点一直在线");
});

test("同名接管:消息投递到新连接,不投旧连接", async (t) => {
  const { connect } = await withBroker(t);
  const sender = await connect("sender");
  const old = await connect("target");
  const fresh = await connect("target");
  await waitFor(() => old.closes.length > 0);

  sender.ws.send(JSON.stringify({ from: "sender", to: "target", id: "after-takeover", re: null, body: { text: "hi" } }));

  assert.ok(await waitFor(() => fresh.inbox.find((m) => m.id === "after-takeover")), "新连接必须收到");
  assert.equal(old.inbox.find((m) => m.id === "after-takeover"), undefined, "旧连接不应收到");
});

// ================================================================ 群发

test("群发:显式收件人数组", async (t) => {
  const { connect } = await withBroker(t);
  const a = await connect("a");
  const b = await connect("b");
  const c = await connect("c");
  const d = await connect("d");
  await waitFor(() => a.inbox.filter((m) => m.body?.kind === "peer_joined").length >= 3);

  a.ws.send(JSON.stringify({ from: "a", to: ["b", "c"], id: "multi-1", re: null, body: { text: "b 和 c" } }));

  assert.ok(await waitFor(() => b.inbox.find((m) => m.id === "multi-1")));
  assert.ok(await waitFor(() => c.inbox.find((m) => m.id === "multi-1")));
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(d.inbox.find((m) => m.id === "multi-1"), undefined, "d 不在收件人里");
  assert.equal(a.inbox.find((m) => m.id === "multi-1"), undefined, "发送者不应收到自己的消息");

  const ack = await waitFor(() => a.inbox.find((m) => m.re === "multi-1"));
  assert.equal(ack.body.kind, "delivered");
  assert.equal(ack.body.delivered, 2);
  assert.equal(ack.body.total, 2);
});

test("群发:#tag 分组", async (t) => {
  const { connect } = await withBroker(t);
  const lead = await connect("lead");
  const w1 = await connect("web1", { tags: "web,frontend" });
  const w2 = await connect("web2", { tags: "web" });
  const db = await connect("db1", { tags: "db" });
  await waitFor(() => lead.inbox.filter((m) => m.body?.kind === "peer_joined").length >= 3);

  lead.ws.send(JSON.stringify({ from: "lead", to: "#web", id: "tag-1", re: null, body: { text: "web 组" } }));

  assert.ok(await waitFor(() => w1.inbox.find((m) => m.id === "tag-1")));
  assert.ok(await waitFor(() => w2.inbox.find((m) => m.id === "tag-1")));
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(db.inbox.find((m) => m.id === "tag-1"), undefined, "db 组不该收到 #web");
});

test("群发:tag 和名字混合且去重", async (t) => {
  const { connect } = await withBroker(t);
  const lead = await connect("lead");
  const w1 = await connect("web1", { tags: "web" });
  await connect("db1", { tags: "db" });
  await waitFor(() => lead.inbox.filter((m) => m.body?.kind === "peer_joined").length >= 2);

  // web1 既在 #web 里又被显式点名 —— 只应收到一次
  lead.ws.send(JSON.stringify({ from: "lead", to: ["#web", "web1"], id: "dedupe-1", re: null, body: { text: "x" } }));
  await waitFor(() => w1.inbox.find((m) => m.id === "dedupe-1"));
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(w1.inbox.filter((m) => m.id === "dedupe-1").length, 1, "同一收件人只应收到一次");

  const ack = await waitFor(() => lead.inbox.find((m) => m.re === "dedupe-1"));
  assert.equal(ack.body.total, 1);
});

test("群发:空分组要报出来,不能假装成功", async (t) => {
  const { connect } = await withBroker(t);
  const lead = await connect("lead");
  await connect("web1", { tags: "web" });
  await waitFor(() => lead.inbox.some((m) => m.body?.kind === "peer_joined"));

  lead.ws.send(JSON.stringify({ from: "lead", to: "#nobody", id: "empty-1", re: null, body: { text: "?" } }));
  const ack = await waitFor(() => lead.inbox.find((m) => m.re === "empty-1"));
  assert.equal(ack.body.kind, "undeliverable");
  assert.deepEqual(ack.body.unknown, ["#nobody"]);
});

test("群发:部分收件人未知时,已知的照投,并报告未知的", async (t) => {
  const { connect } = await withBroker(t);
  const a = await connect("a");
  const b = await connect("b");
  await waitFor(() => a.inbox.some((m) => m.body?.kind === "peer_joined"));

  a.ws.send(JSON.stringify({ from: "a", to: ["b", "ghost"], id: "partial-1", re: null, body: { text: "x" } }));
  assert.ok(await waitFor(() => b.inbox.find((m) => m.id === "partial-1")));

  const ack = await waitFor(() => a.inbox.find((m) => m.re === "partial-1"));
  assert.equal(ack.body.kind, "delivered");
  assert.equal(ack.body.delivered, 1);
  assert.deepEqual(ack.body.unknown, ["ghost"]);
});

// ================================================================ 成员元数据

test("成员元数据:welcome 带 host / tags,同一机器可有多个节点", async (t) => {
  const { connect } = await withBroker(t);
  // 同一台机器上两个 agent —— 身份是名字,不是 IP
  await connect("dev01-frontend", { host: "dev01", tags: "web" });
  await connect("dev01-backend", { host: "dev01", tags: "api" });
  const laptop = await connect("laptop", { host: "laptop" });

  const welcome = laptop.inbox.find((m) => m.body?.kind === "welcome");
  const byName = Object.fromEntries(welcome.body.members.map((m) => [m.name, m]));

  assert.equal(byName["dev01-frontend"].host, "dev01");
  assert.equal(byName["dev01-backend"].host, "dev01");
  assert.deepEqual(byName["dev01-frontend"].tags, ["web"]);
  assert.deepEqual(byName["dev01-backend"].tags, ["api"]);
  assert.equal(byName["dev01-frontend"].addr, byName["dev01-backend"].addr, "同机两个节点来源地址相同");
});

test("成员元数据:非法 tag 被丢弃,数量有上限", async (t) => {
  const { connect } = await withBroker(t);
  const many = Array.from({ length: 20 }, (_, i) => `t${i}`).join(",");
  await connect("tagger", { tags: `ok,bad tag!,${many}` });
  const obs = await connect("obs");

  const welcome = obs.inbox.find((m) => m.body?.kind === "welcome");
  const tagger = welcome.body.members.find((m) => m.name === "tagger");
  assert.ok(tagger.tags.includes("ok"));
  assert.ok(!tagger.tags.includes("bad tag!"), "含空格/感叹号的 tag 应被丢弃");
  assert.ok(tagger.tags.length <= 8, `tag 数量应有上限,实际 ${tagger.tags.length}`);
});

test("群发:#tag 不把自己算进去(发送者也属于该组时)", async (t) => {
  const { connect } = await withBroker(t);
  // 发送者自己就是 web —— 在线两个 web 节点,但它只应发给另一个
  const me = await connect("web-sender", { tags: "web" });
  const peer = await connect("web-other", { tags: "web" });
  await waitFor(() => me.inbox.some((m) => m.body?.kind === "peer_joined"));

  me.ws.send(JSON.stringify({ from: "web-sender", to: "#web", id: "self-1", re: null, body: { text: "x" } }));

  assert.ok(await waitFor(() => peer.inbox.find((m) => m.id === "self-1")), "同组其他节点应收到");
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(me.inbox.find((m) => m.id === "self-1"), undefined, "发送者不应收到自己发的");

  const ack = await waitFor(() => me.inbox.find((m) => m.re === "self-1"));
  assert.equal(ack.body.total, 1, "总数应排除发送者自己");
});

test("群发:@label 语法(和客户端的用户面向写法一致)", async (t) => {
  const { connect } = await withBroker(t);
  const lead = await connect("lead");
  const w1 = await connect("web1", { tags: "web" });
  const db = await connect("db1", { tags: "db" });
  await waitFor(() => lead.inbox.filter((m) => m.body?.kind === "peer_joined").length >= 2);

  lead.ws.send(JSON.stringify({ from: "lead", to: "@web", id: "at-1", re: null, body: { text: "x" } }));

  assert.ok(await waitFor(() => w1.inbox.find((m) => m.id === "at-1")), "@web 应命中 web1");
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(db.inbox.find((m) => m.id === "at-1"), undefined, "db1 不该收到");

  const ack = await waitFor(() => lead.inbox.find((m) => m.re === "at-1"));
  assert.equal(ack.body.kind, "delivered", "不该报 unknown —— 两侧语法必须一致");
  assert.equal(ack.body.delivered, 1);
});

test("群发:@default 指全员", async (t) => {
  const { connect } = await withBroker(t);
  const lead = await connect("lead");
  const a = await connect("a");
  const b = await connect("b");
  await waitFor(() => lead.inbox.filter((m) => m.body?.kind === "peer_joined").length >= 2);

  lead.ws.send(JSON.stringify({ from: "lead", to: "@default", id: "def-1", re: null, body: { text: "x" } }));

  assert.ok(await waitFor(() => a.inbox.find((m) => m.id === "def-1")));
  assert.ok(await waitFor(() => b.inbox.find((m) => m.id === "def-1")));
  assert.equal(lead.inbox.find((m) => m.id === "def-1"), undefined, "发送者不该收到");

  const ack = await waitFor(() => lead.inbox.find((m) => m.re === "def-1"));
  assert.equal(ack.body.delivered, 2);
});

test("群发:@default 不被当成 label default", async (t) => {
  const { connect } = await withBroker(t);
  const lead = await connect("lead");
  // 故意造一个真叫 default 的 tag,确认 @default 走全员而不是这个组
  const named = await connect("marker", { tags: "default" });
  const plain = await connect("plain");
  await waitFor(() => lead.inbox.filter((m) => m.body?.kind === "peer_joined").length >= 2);

  lead.ws.send(JSON.stringify({ from: "lead", to: "@default", id: "amb-1", re: null, body: { text: "x" } }));

  assert.ok(await waitFor(() => named.inbox.find((m) => m.id === "amb-1")));
  assert.ok(await waitFor(() => plain.inbox.find((m) => m.id === "amb-1")), "全员应包含没有该 tag 的节点");
});

// ---------------------------------------------------------------- /auth 诊断端点
//
// 为什么需要它:Node 内置 WebSocket 把握手阶段的 HTTP 状态完全藏起来了。
// broker 返回 401 时,客户端只能看到 error(消息为空)+ close 1006 ——
// 和网线被拔一模一样。实测过,不是推测。
//
// 所以客户端握手失败后,用普通 HTTP 调一次 /auth 来区分"token 错"和"连不上"。

test("/auth:正确 token 返回 200", async (t) => {
  const { port } = await withBroker(t);
  const r = await fetch(`http://127.0.0.1:${port}/auth`, {
    headers: { authorization: `Bearer ${TOKEN}` },
  });
  assert.equal(r.status, 200);
  assert.deepEqual(await r.json(), { ok: true });
});

test("/auth:错误 token 返回 401,并带上 broker 的 token 指纹", async (t) => {
  const { port } = await withBroker(t);
  const r = await fetch(`http://127.0.0.1:${port}/auth`, {
    headers: { authorization: "Bearer wrong-token-value-here" },
  });
  assert.equal(r.status, 401);
  const body = await r.json();
  assert.equal(body.ok, false);
  assert.equal(body.reason, "token_mismatch");
  // 指纹让用户能核对"服务器用的是哪个 token",而不必把 token 本身发出去
  assert.match(body.fingerprint, /^[0-9a-f]{8}$/);
});

test("/auth:不带 token 返回 401", async (t) => {
  const { port } = await withBroker(t);
  const r = await fetch(`http://127.0.0.1:${port}/auth`);
  assert.equal(r.status, 401);
  assert.equal((await r.json()).reason, "token_missing");
});

test("/auth:不接受 query 里的 token(避免进 access log)", async (t) => {
  const { port } = await withBroker(t);
  const r = await fetch(`http://127.0.0.1:${port}/auth?token=${TOKEN}`);
  assert.equal(r.status, 401, "query 里的 token 不该被采纳");
});

test("/auth:错误 token 的响应里不含正确 token", async (t) => {
  const { port } = await withBroker(t);
  const r = await fetch(`http://127.0.0.1:${port}/auth`, {
    headers: { authorization: "Bearer nope" },
  });
  const text = await r.text();
  assert.equal(text.includes(TOKEN), false, "诊断信息绝不能泄露真实 token");
});

// ---------------------------------------------------------------- 启动失败
//
// 之前 server.listen 没有 error 处理,端口被占时抛出 Node 的原始堆栈:
// 不说是谁占着,也不提最常见的原因 —— 已经有一个 broker 在跑了。
// 真实用户在 dev01 上撞到过:一个 systemd 服务占着 8787,又手动起了一个。

import { createServer as createNetServer } from "node:net";

/** 起 broker,等它退出,返回 { code, stdout, stderr } */
function runBrokerToExit(args, env = {}) {
  return new Promise((resolve) => {
    const p = spawn(process.execPath, [BROKER, ...args], {
      env: { ...process.env, TEAM_TOKEN: TOKEN, ...env },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    p.stdout.on("data", (d) => (stdout += d));
    p.stderr.on("data", (d) => (stderr += d));
    const timer = setTimeout(() => {
      p.kill("SIGKILL");
      resolve({ code: "timeout", stdout, stderr });
    }, 8000);
    p.on("exit", (code) => {
      clearTimeout(timer);
      resolve({ code, stdout, stderr });
    });
  });
}

test("端口被占:退出码 78,并说明原因和排查方法", async (t) => {
  // 先占住一个端口
  const holder = createNetServer();
  await new Promise((r) => holder.listen(0, "127.0.0.1", r));
  const port = holder.address().port;
  t.after(() => holder.close());

  const r = await runBrokerToExit(["--bind", "127.0.0.1", "--port", String(port)]);

  assert.equal(
    r.code,
    78,
    "应以 78(EX_CONFIG)退出 —— systemd 可以用 RestartPreventExitStatus=78 停止无意义的重启",
  );

  const out = r.stdout + r.stderr;
  assert.match(out, new RegExp(`127\\.0\\.0\\.1:${port}`), "要说出是哪个地址端口");
  assert.match(out, /已被占用|already in use/, "要明说端口被占");
  assert.match(out, /ss -ltnp|lsof/, "要给出查占用者的命令");
  assert.match(out, /systemctl|已经.*在跑|another broker/, "要提示最常见的原因:已有实例在跑");
  assert.equal(/at Server\.setupListenHandle/.test(out), false, "不该再打印 Node 的原始堆栈");
});

test("没有 TOKEN:仍然明确拒绝启动", async () => {
  const r = await runBrokerToExit(["--bind", "127.0.0.1", "--port", "0"], { TEAM_TOKEN: "" });
  assert.notEqual(r.code, 0);
  assert.match(r.stderr, /TEAM_TOKEN/);
});

test("绑一个本机没有的地址:退出码 78,提示用 tailscale IP", async () => {
  // 192.0.2.0/24 是 RFC 5737 的文档专用网段,保证不会配在任何真实接口上。
  // 这对应 $(tailscale ip -4) 在 tailscale 没起来时展开成的情形。
  const r = await runBrokerToExit(["--bind", "192.0.2.1", "--port", "0"]);
  assert.equal(r.code, 78);
  const out = r.stdout + r.stderr;
  assert.match(out, /192\.0\.2\.1/, "要说出是哪个地址");
  assert.match(out, /tailscale ip/, "要提示最常见的正确写法");
  assert.equal(/at Server\.setupListenHandle/.test(out), false, "不该有原始堆栈");
});

// ---------------------------------------------------------------- 消息过大
//
// 以前 broker 直接 socket.destroy():发送方只看到 close 1006,和网线被拔
// 一模一样。用户会去查网络和防火墙,而实际原因是文本太长。

test("消息过大:broker 以 1009 关闭并说明原因,不是裸断开", async (t) => {
  const h = await withBroker(t);
  // 收件人必须在线,否则消息会在地址解析那步就被判 undeliverable、
  // 根本走不到帧读取 —— 第一版测试就栽在这儿(它实际测的是"收件人不存在")。
  await h.connect("big-target");

  const env = JSON.stringify({
    from: "big-sender",
    to: "big-target",
    id: "big-1",
    re: null,
    body: { text: "字".repeat(22000), hops: 0 },
  });
  assert.ok(Buffer.byteLength(env) > 65536, `前置条件:信封必须真的超限,实际 ${Buffer.byteLength(env)} 字节`);

  const closed = await new Promise((resolve, reject) => {
    const ws = new WebSocket(`${h.base}/?name=big-sender&token=${TOKEN}&host=h`);
    const timer = setTimeout(() => reject(new Error("等关闭超时")), 10000);
    ws.onopen = () => ws.send(env);
    ws.onclose = (ev) => {
      clearTimeout(timer);
      resolve({ code: ev.code, reason: ev.reason });
    };
    ws.onerror = () => {};
  });

  assert.equal(closed.code, 1009, "应使用 1009(消息过大),而不是 1006");
  assert.match(closed.reason, /too large|payload/i, "要带上原因,否则对端无从判断");
});

test("消息过大:只断那一条,重连后照常可用", async (t) => {
  const h = await withBroker(t);
  await h.connect("recover-target");

  await new Promise((resolve) => {
    const ws = new WebSocket(`${h.base}/?name=recover&token=${TOKEN}&host=h`);
    ws.onopen = () =>
      ws.send(
        JSON.stringify({ from: "recover", to: "recover-target", id: "x", re: null, body: { text: "字".repeat(22000), hops: 0 } }),
      );
    ws.onclose = () => resolve();
    ws.onerror = () => {};
  });

  // 同一个名字重连,后面的正常消息应照常送达
  const got = await new Promise((resolve) => {
    const ws = new WebSocket(`${h.base}/?name=recover&token=${TOKEN}&host=h`);
    const t2 = setTimeout(() => resolve(false), 8000);
    ws.onopen = () => ws.send(JSON.stringify({ from: "recover", to: "recover-target", id: "ok-1", re: null, body: { text: "小消息", hops: 0 } }));
    ws.onmessage = (ev) => {
      if (JSON.parse(ev.data).body?.kind === "delivered") {
        clearTimeout(t2);
        resolve(true);
      }
    };
    ws.onerror = () => {};
  });
  assert.equal(got, true, "超限只该断那一次,不该影响后续连接");
});
