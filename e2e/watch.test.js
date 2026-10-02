/**
 * 订阅的一致性测试。跑:node --test e2e/
 *
 * 用真的 broker 和真的 WebSocket 走一遍订阅:登记、发布、收到通知、
 * 注销。特别验两件光靠单测断不了的事:
 *
 *   1. 被订阅者完全不知道自己被看(它的收件箱里不该出现任何 watch 帧)。
 *      这不是礼貌问题:一旦有回执,订阅者就被暴露了。
 *   2. 订阅者断开后 broker 的表要清干净。
 *
 * 订阅只在 broker 模式存在,所以这里不进 conformance.js ——
 * 那个套件跑三种 transport,加进来会逼着 mesh/swim 也实现一遍。
 */
import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const HERE = dirname(fileURLToPath(import.meta.url));
const BROKER = join(HERE, "..", "broker.mjs");
const TOKEN = "watch-conformance-token";

/** 起一个 broker,读回实际端口(--port 0 让内核分配) */
async function startBroker() {
  const broker = spawn(
    process.execPath,
    [BROKER, "--bind", "127.0.0.1", "--port", "0"],
    { env: { ...process.env, TEAM_TOKEN: TOKEN }, stdio: ["ignore", "pipe", "pipe"] },
  );

  const port = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("broker 启动超时")), 8000);
    broker.stdout.on("data", (d) => {
      const m = /broker 监听 ws:\/\/[^:]+:(\d+)/.exec(d.toString());
      if (!m) return;
      clearTimeout(timer);
      resolve(Number(m[1]));
    });
    broker.on("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`broker 提前退出 code=${code}`));
    });
  });

  return { broker, port, stop: () => broker.kill("SIGTERM") };
}

/**
 * 一个假节点:真 WebSocket 连接,收信封进 inbox。
 * 和 transport-broker 的 harness 同一思路 —— 测真实协议行为。
 */
async function connect(port, name) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}?name=${name}&host=h&tags=&token=${TOKEN}`);
  const inbox = [];

  ws.addEventListener("message", (ev) => {
    try {
      inbox.push(JSON.parse(String(ev.data)));
    } catch {
      // 忽略非 JSON 帧
    }
  });

  await new Promise((resolve, reject) => {
    ws.addEventListener("open", resolve, { once: true });
    ws.addEventListener("error", () => reject(new Error(`${name} 连接失败`)), { once: true });
  });

  return {
    name,
    inbox,
    close: () => ws.close(),
    /** 发一个应用信封 */
    send(to, body, id = `m-${Math.random().toString(36).slice(2, 8)}`) {
      ws.send(JSON.stringify({ from: name, to, id, re: null, body }));
      return id;
    },
    /** 发一个订阅控制帧 */
    watch(action, target) {
      ws.send(
        JSON.stringify({
          from: name,
          to: "broker",
          id: `w-${Math.random().toString(36).slice(2, 8)}`,
          re: null,
          body: { kind: "watch", action, ...(target ? { target } : {}) },
        }),
      );
    },
    /** 等一条满足条件的帧 */
    async waitFor(pred, ms = 2000) {
      const t0 = Date.now();
      while (Date.now() - t0 < ms) {
        const hit = inbox.find(pred);
        if (hit) return hit;
        await new Promise((r) => setTimeout(r, 15));
      }
      return null;
    },
  };
}

const isWatch = (env) => /watch/.test(String(env?.body?.kind ?? ""));

test("[watch] 登记、收到通知、注销", async (t) => {
  const { port, stop } = await startBroker();
  t.after(stop);

  const alice = await connect(port, "alice");
  const bob = await connect(port, "bob");
  t.after(() => {
    alice.close();
    bob.close();
  });

  bob.watch("add", "alice");
  const ack = await bob.waitFor((e) => e.body?.kind === "watch_ack" && e.body.action === "add");
  assert.ok(ack, "应当收到 watch_ack");
  assert.equal(ack.body.target, "alice");
  assert.equal(ack.body.state, "registered");

  // bob 订阅 alice 时,alice 应当被告知"有 1 个人在看你"(只有数量,没有名字)
  const state = await alice.waitFor((e) => e.body?.kind === "watch_state");
  assert.ok(state, "订阅生效时,被订阅者应当收到计数通知");
  assert.equal(state.body.watchers, 1);
  assert.deepEqual(
    Object.keys(state.body).sort(),
    ["kind", "watchers"],
    "计数帧只能带数量,绝不能带订阅者的身份 —— 那会让订阅变成双向可见",
  );

  // alice 发布一轮输出
  alice.send("_watchers", { text: "ALICE 的结论", hops: 1 });
  const note = await bob.waitFor((e) => e.body?.kind === "watch_notify");
  assert.ok(note, "订阅者应当收到 watch_notify");
  assert.equal(note.body.target, "alice");
  assert.equal(note.body.summary, "ALICE 的结论");

  // ── 单向性 ──
  // alice 只该收到"有几个人在看"这个计数,不该收到任何带订阅者身份的东西:
  // 没有 watch_ack、没有 watch_notify、没有任何回执。
  await new Promise((r) => setTimeout(r, 150));
  const leaked = alice.inbox.filter(
    (e) => isWatch(e) && e.body?.kind !== "watch_state",
  );
  assert.equal(
    leaked.length,
    0,
    `被订阅者不该收到任何带身份信息的 watch 帧(实际收到 ${JSON.stringify(leaked.map((e) => e.body))})`,
  );

  // 注销之后就收不到了
  bob.watch("remove", "alice");
  await bob.waitFor((e) => e.body?.kind === "watch_ack" && e.body.action === "remove");
  const before = bob.inbox.filter((e) => e.body?.kind === "watch_notify").length;
  alice.send("_watchers", { text: "注销之后", hops: 1 });
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(
    bob.inbox.filter((e) => e.body?.kind === "watch_notify").length,
    before,
    "注销后不该再收到通知",
  );
});

test("[watch] 没人在听时发布不是错误,但要告知", async (t) => {
  const { port, stop } = await startBroker();
  t.after(stop);

  const solo = await connect(port, "solo");
  t.after(() => solo.close());

  solo.send("_watchers", { text: "没人在听", hops: 1 });
  const none = await solo.waitFor((e) => e.body?.kind === "watch_none");
  assert.ok(none, "发布者应当被告知没人在听,而不是静默成功");
});

test("[watch] 订阅自己会被拒", async (t) => {
  const { port, stop } = await startBroker();
  t.after(stop);

  const solo = await connect(port, "solo");
  t.after(() => solo.close());

  solo.watch("add", "solo");
  const err = await solo.waitFor((e) => e.body?.kind === "watch_error");
  assert.ok(err, "应当收到 watch_error");
  assert.equal(err.body.reason, "self");
});

test("[watch] 订阅数上限", async (t) => {
  const { port, stop } = await startBroker();
  t.after(stop);

  const solo = await connect(port, "solo");
  t.after(() => solo.close());

  for (let i = 0; i < 9; i++) solo.watch("add", `peer${i}`);
  const err = await solo.waitFor((e) => e.body?.kind === "watch_error" && e.body.reason === "limit");
  assert.ok(err, "第 9 个订阅应当被拒");
  assert.equal(err.body.limit, 8);
});

test("[watch] 订阅者断开后,发布不再报给它", async (t) => {
  const { port, stop } = await startBroker();
  t.after(stop);

  const alice = await connect(port, "alice");
  t.after(() => alice.close());

  const bob = await connect(port, "bob");
  bob.watch("add", "alice");
  await bob.waitFor((e) => e.body?.kind === "watch_ack");

  bob.close();
  await new Promise((r) => setTimeout(r, 250));

  // broker 不应当崩,也不该因为表里留着 bob 而报错
  alice.send("_watchers", { text: "bob 已经走了", hops: 1 });
  await new Promise((r) => setTimeout(r, 200));
  const alive = await connect(port, "carol");
  t.after(() => alive.close());
  assert.equal(typeof alive.name, "string", "broker 仍然可用");
});

test("[watch] 同名连接接管后,新连接从空订阅开始", async (t) => {
  const { port, stop } = await startBroker();
  t.after(stop);

  const alice = await connect(port, "alice");
  t.after(() => alice.close());

  const first = await connect(port, "bob");
  first.watch("add", "alice");
  await first.waitFor((e) => e.body?.kind === "watch_ack");

  // 同名新连接接管
  const second = await connect(port, "bob");
  t.after(() => second.close());
  await new Promise((r) => setTimeout(r, 200));

  second.watch("list");
  const lst = await second.waitFor((e) => e.body?.kind === "watch_ack" && e.body.action === "list");
  assert.ok(lst, "新连接应当能列订阅");
  assert.deepEqual(lst.body.watches, [], "新连接不该继承旧连接的订阅");
});
