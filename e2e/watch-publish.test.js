/**
 * 订阅发布的端到端测试。跑:node --test e2e/
 *
 * watch.test.js(单元)验到"产出 publish 动作",watch.test.js(e2e)验到
 * "broker 会展开 _watchers"。这里把两端接上,用真实的 transport 和真实的
 * session 状态机跑一遍完整链路:
 *
 *   订阅者订阅 → 发布者被告知计数 → 发布者跑完一轮 → publish 意图
 *   → transport 发出真实帧 → 订阅者收到通知
 *
 * 单独写一个文件是因为它同时依赖 transport 和 session 两层,
 * 放在任何一边都不合适。
 */
import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { createBrokerTransport } from "../src/transport.js";
import { createSessionState, onTurnSettled, observeMessage } from "../src/session.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const BROKER = join(HERE, "..", "broker.mjs");
const TOKEN = "watch-publish-token";

async function startBroker() {
  const broker = spawn(process.execPath, [BROKER, "--bind", "127.0.0.1", "--port", "0"], {
    env: { ...process.env, TEAM_TOKEN: TOKEN },
    stdio: ["ignore", "pipe", "pipe"],
  });
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
  return { port, stop: () => broker.kill("SIGTERM") };
}

const settle = (ms = 3000) => new Promise((r) => setTimeout(r, ms));

async function waitFor(pred, ms = 3000) {
  const t0 = Date.now();
  while (Date.now() - t0 < ms) {
    if (pred()) return true;
    await new Promise((r) => setTimeout(r, 20));
  }
  return false;
}

test("[watch] 不再有订阅者时,发布是静默的", () => {
  const s = createSessionState("publisher");
  observeMessage(s, "assistant", "没人看的输出");
  assert.deepEqual(
    onTurnSettled(s),
    [],
    "没有订阅者时不该产生 publish —— 未使用该特性的部署不该因为升级多出流量",
  );
});

test("[watch] 从订阅到收到通知的完整链路", async (t) => {
  const { port, stop } = await startBroker();
  t.after(stop);
  const url = `ws://127.0.0.1:${port}`;

  // 订阅者:一个裸 WebSocket 客户端
  const watcherInbox = [];
  const watcher = new WebSocket(`${url}?name=watcher&host=h&tags=&token=${TOKEN}`);
  watcher.addEventListener("message", (e) => {
    try {
      watcherInbox.push(JSON.parse(String(e.data)));
    } catch {}
  });
  await new Promise((r) => watcher.addEventListener("open", r, { once: true }));
  t.after(() => watcher.close());

  // 发布者:真实 transport + 真实 session 状态机
  const transport = createBrokerTransport({ url, token: TOKEN });
  const inbox = [];
  transport.on("envelope", (env) => inbox.push(env));
  t.after(() => transport.stop());

  transport.start({ name: "publisher", labels: [], host: "h" });
  assert.equal(await waitFor(() => transport.state() === "online"), true, "应当连上 broker");
  assert.equal(
    await waitFor(() => inbox.some((e) => e.body?.kind === "welcome")),
    true,
    "应当收到 welcome",
  );

  // 订阅者订阅发布者
  watcher.send(
    JSON.stringify({
      from: "watcher",
      to: "broker",
      id: "w1",
      re: null,
      body: { kind: "watch", action: "add", target: "publisher" },
    }),
  );

  // 发布者被告知有人在看(只有数量)
  const told = await waitFor(() =>
    inbox.some((e) => e.body?.kind === "watch_state" && e.body.watchers === 1),
  );
  assert.equal(told, true, "订阅生效时,被订阅者应当收到计数通知");
  const stateFrame = inbox.find((e) => e.body?.kind === "watch_state");
  // 只允许 kind 和 watchers 两个字段。任何多出来的字段都可能带身份信息:
  // 一旦计数帧里出现订阅者的名字,订阅就变成双向可见了。
  assert.deepEqual(
    Object.keys(stateFrame.body).sort(),
    ["kind", "watchers"],
    "计数帧只能带数量,不能带订阅者的身份",
  );

  // 发布者跑完一轮
  const s = createSessionState("publisher");
  s.watcherCount = stateFrame.body.watchers;
  observeMessage(s, "assistant", "PUBLISHER 的最终结论");

  const actions = onTurnSettled(s);
  assert.equal(actions.length, 1);
  assert.equal(actions[0].type, "publish");

  // 按 index.ts 的方式执行 publish 意图
  for (const it of actions) {
    if (it.type === "publish") transport.publishToWatchers(it.text);
  }

  const delivered = await waitFor(() => watcherInbox.some((e) => e.body?.kind === "watch_notify"), 2000);
  assert.equal(delivered, true, "订阅者应当收到通知");

  const note = watcherInbox.find((e) => e.body?.kind === "watch_notify");
  assert.equal(note.body.target, "publisher");
  assert.equal(note.body.summary, "PUBLISHER 的最终结论");
});

test("[watch] 没人订阅时发布,发布者会被告知", async (t) => {
  const { port, stop } = await startBroker();
  t.after(stop);

  const transport = createBrokerTransport({ url: `ws://127.0.0.1:${port}`, token: TOKEN });
  const inbox = [];
  transport.on("envelope", (env) => inbox.push(env));
  t.after(() => transport.stop());

  transport.start({ name: "lonely", labels: [], host: "h" });
  assert.equal(await waitFor(() => transport.state() === "online"), true);
  await waitFor(() => inbox.some((e) => e.body?.kind === "welcome"));

  transport.publishToWatchers("没人在听");
  const told = await waitFor(() => inbox.some((e) => e.body?.kind === "watch_none"));
  assert.equal(told, true, "无人订阅时要明确告知,不静默成功");
});
