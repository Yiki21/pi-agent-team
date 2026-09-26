/**
 * SWIM 边车的行为验证。
 *
 * 这个测试直接驱动 Go 边车(不经 Node 适配层),验证它自己是对的:
 * 成员发现、标签传播、故障检测。适配层的问题在 transport-swim.test.js 里查。
 *
 * 需要先构建边车:
 *   cd swim && go build -o ../.tmp/swim-sidecar .
 *
 * 边车不存在时整个文件 skip,而不是失败 —— 这样没装 Go 的开发者也
 * 能跑其余测试。
 */
import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const HERE = dirname(fileURLToPath(import.meta.url));
const SIDECAR = join(HERE, "..", ".tmp", "swim-sidecar");
const TOKEN = "sidecar-test-token";

const available = existsSync(SIDECAR);

/** 起一个边车节点,等到它打印 LISTEN 行 */
function startNode({ name, host, labels, port, seeds = [] }) {
  const args = [
    "-name", name,
    "-token", TOKEN,
    "-bind", "127.0.0.1",
    "-port", String(port),
    "-http", "127.0.0.1:0",
    "-host", host,
    "-labels", labels,
  ];
  if (seeds.length) args.push("-seeds", seeds.join(","));

  const proc = spawn(SIDECAR, args, { stdio: ["ignore", "pipe", "pipe"] });
  let out = "";
  let err = "";
  proc.stdout.on("data", (d) => (out += d));
  proc.stderr.on("data", (d) => (err += d));

  return {
    proc,
    stdout: () => out,
    stderr: () => err,
    async url(ms = 10000) {
      const t0 = Date.now();
      while (Date.now() - t0 < ms) {
        const m = out.match(/LISTEN (http:\/\/\S+)/);
        if (m) return m[1];
        if (proc.exitCode !== null) throw new Error(`边车退出 code=${proc.exitCode}: ${err.slice(0, 300)}`);
        await sleep(40);
      }
      throw new Error(`等 LISTEN 超时。stderr: ${err.slice(0, 300)}`);
    },
    kill: (sig = "SIGTERM") => {
      try {
        proc.kill(sig);
      } catch {}
    },
  };
}

async function getJson(base, path) {
  const r = await fetch(base + path, { signal: AbortSignal.timeout(5000) });
  assert.equal(r.status, 200, `${path} 应返回 200`);
  return r.json();
}

const names = (snap) => snap.members.map((m) => m.name).sort();
const byName = (snap, n) => snap.members.find((m) => m.name === n);

/** 所有边车节点在测试结束时清理 */
function track(t) {
  const nodes = [];
  t.after(() => {
    for (const n of nodes) n.kill("SIGKILL");
  });
  return (node) => {
    nodes.push(node);
    return node;
  };
}

test("SWIM 边车:成员发现 + 标签传播 + 故障检测", { skip: !available }, async (t) => {
  const trackNode = track(t);

  // --- 单节点起步
  const a1 = trackNode(startNode({ name: "a1", host: "laptop", labels: "web,fe", port: 17946 }));
  const a1url = await a1.url();

  const solo = await getJson(a1url, "/members");
  assert.equal(solo.self, "a1");
  assert.deepEqual(names(solo), ["a1"], "只有自己");

  const self = byName(solo, "a1");
  assert.deepEqual(self.labels, ["web", "fe"], "标签应出现在自己的成员信息里");
  assert.equal(self.host, "laptop", "host 应出现在自己的成员信息里");
  assert.equal(self.state, "alive");

  // --- 第二个节点通过种子加入
  const a2 = trackNode(startNode({ name: "a2", host: "dev01", labels: "api", port: 17947, seeds: ["127.0.0.1:17946"] }));
  const a2url = await a2.url();

  // 等双向可见
  const t0 = Date.now();
  let m1 = await getJson(a1url, "/members");
  while (names(m1).length < 2 && Date.now() - t0 < 10000) {
    await sleep(150);
    m1 = await getJson(a1url, "/members");
  }
  assert.deepEqual(names(m1), ["a1", "a2"], "a1 应看到 a2");

  const m2 = await getJson(a2url, "/members");
  assert.deepEqual(names(m2), ["a1", "a2"], "a2 应看到 a1");

  // --- 标签通过 Meta 传播(不额外查询)
  const a2seenFromA1 = byName(m1, "a2");
  assert.deepEqual(a2seenFromA1.labels, ["api"], "a2 的标签应传播到 a1");
  assert.equal(a2seenFromA1.host, "dev01", "a2 的 host 应传播到 a1");

  // --- 第三个节点也以 a1 为种子:全互联发现
  const a3 = trackNode(startNode({ name: "a3", host: "dev01", labels: "db", port: 17948, seeds: ["127.0.0.1:17946"] }));
  const a3url = await a3.url();

  const t1 = Date.now();
  let m3 = await getJson(a3url, "/members");
  while (names(m3).length < 3 && Date.now() - t1 < 10000) {
    await sleep(150);
    m3 = await getJson(a3url, "/members");
  }
  assert.deepEqual(names(m3), ["a1", "a2", "a3"], "a3 应看到全部三个节点");

  // --- 优雅退出:立刻被感知
  a3.kill("SIGTERM");
  const t2 = Date.now();
  let afterLeave = await getJson(a1url, "/members");
  while (afterLeave.members.some((m) => m.name === "a3" && m.state === "alive") && Date.now() - t2 < 8000) {
    await sleep(150);
    afterLeave = await getJson(a1url, "/members");
  }
  const a3state = byName(afterLeave, "a3");
  assert.ok(!a3state || a3state.state !== "alive", `a3 不该还是 alive,实际 ${JSON.stringify(a3state)}`);

  // --- 强杀:靠 suspicion 超时被检测出来
  a2.kill("SIGKILL");
  const t3 = Date.now();
  let afterKill = await getJson(a1url, "/members");
  while (afterKill.members.some((m) => m.name === "a2" && m.state === "alive") && Date.now() - t3 < 20000) {
    await sleep(250);
    afterKill = await getJson(a1url, "/members");
  }
  const a2state = byName(afterKill, "a2");
  assert.ok(
    !a2state || a2state.state !== "alive",
    `强杀的 a2 最终应被判为非 alive,实际 ${JSON.stringify(a2state)};耗时 ${Date.now() - t3}ms`,
  );
});

test("SWIM 边车:缺参数时拒绝启动", { skip: !available }, async (t) => {
  // 没给 token
  const bad = spawn(SIDECAR, ["-name", "x", "-bind", "127.0.0.1", "-port", "0"], { stdio: ["ignore", "pipe", "pipe"] });
  t.after(() => bad.kill("SIGKILL"));

  const code = await new Promise((resolve) => {
    bad.on("exit", resolve);
    setTimeout(() => resolve("timeout"), 4000);
  });
  assert.notEqual(code, "timeout", "缺参数时应立刻退出,不是挂着");
  assert.notEqual(code, 0, "退出码应非零");
});

test("SWIM 边车:token 不同则 gossip 密钥不同,互相连不上", { skip: !available }, async (t) => {
  const trackNode = track(t);

  const a = trackNode(startNode({ name: "tok-a", host: "h", labels: "", port: 17950 }));
  const aurl = await a.url();

  // 手工起一个用别的 token 的节点,种子指向 a
  const other = spawn(
    SIDECAR,
    ["-name", "tok-b", "-token", "different-token", "-bind", "127.0.0.1", "-port", "17951",
     "-http", "127.0.0.1:0", "-host", "h", "-labels", "", "-seeds", "127.0.0.1:17950"],
    { stdio: ["ignore", "pipe", "pipe"] },
  );
  t.after(() => other.kill("SIGKILL"));

  await sleep(2500);
  const snap = await getJson(aurl, "/members");
  assert.equal(
    names(snap).includes("tok-b"),
    false,
    "token 不同的节点不该互相可见 —— gossip 密钥应把它挡在外面",
  );
});
