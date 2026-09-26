/**
 * SWIM transport:成员与故障检测用 SWIM,消息投递用直连。
 *
 * ── 为什么这样分 ──
 *   SWIM 是成员协议,不是消息总线。它回答"谁活着",不负责"把字节
 *   送到对端"。所以:
 *     成员表 ← Go 边车(memberlist)
 *     消息   ← 节点间直连(复用 mesh 的连接层)
 *   这符合 roadmap 9.3 定的边界。
 *
 * ── 为什么边车是 Go ──
 *   memberlist 是 SWIM 最成熟的实现,而 SWIM 最难的三块(incarnation
 *   number、suspicion 超时、ping-req 间接探测)只在故障时才暴露。
 *
 * ── 成员信息需要携带投递端点 ──
 *   SWIM 的通告里是 gossip 端口,而消息要走直连端口,两者不同。所以
 *   边车的 Meta 里带 deliver 字段,Node 侧据此知道往哪连。
 *   少了这一步的症状是:收件人名字已知,但发不出去。
 *
 * ── 边车不可用时明确失败,不静默降级 ──
 *   降级到静态种子会让"SWIM 检测到故障"变成谎言 —— 成员表说什么
 *   都不影响消息怎么走。所以 state() 变 offline 并给出原因。
 */

import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { createMeshTransport, resolveTargets } from "./transport-mesh.js";
import { createEmitter } from "./transport.js";

const HERE = dirname(fileURLToPath(import.meta.url));

/** 边车可执行文件的常见位置 */
function findSidecar(explicit) {
  if (explicit) return existsSync(explicit) ? explicit : null;
  if (process.env.PI_TEAM_SWIM_SIDECAR) {
    return existsSync(process.env.PI_TEAM_SWIM_SIDECAR) ? process.env.PI_TEAM_SWIM_SIDECAR : null;
  }
  for (const p of [join(HERE, "..", ".tmp", "swim-sidecar"), join(HERE, "..", "swim", "swim-sidecar")]) {
    if (existsSync(p)) return p;
  }
  return null;
}

/** 诊断用:边车是否存在 */
export const sidecarAvailable = (explicit = null) => findSidecar(explicit) !== null;

export function createSwimTransport({
  token,
  seeds = [],
  sidecarPath = null,
  listenHost = "0.0.0.0",
  listenPort = 0,
  advertiseHost = null,
  gossipBind = "127.0.0.1",
  gossipPort = 0,
  heartbeatMs = 15_000,
  /** 成员刷新间隔(兜底,SSE 丢事件时靠它) */
  refreshMs = 30_000,
} = {}) {
  const bus = createEmitter();
  const binary = findSidecar(sidecarPath);

  let sidecar = null;
  let sidecarUrl = null;
  /** SWIM 给的成员(权威) */
  let swimMembers = [];
  /** 合并了投递端点的最终视图 */
  let members = [];
  let state = "offline";
  let self = null;
  let mesh = null;
  let eventsAbort = null;
  let pollTimer = null;
  let stopped = false;

  const setState = (next, detail) => {
    if (state === next) return;
    state = next;
    bus.emit("state", next, detail);
  };

  // ---------------------------------------------------------------- 边车

  async function startSidecar() {
    if (!binary) {
      setState("offline", {
        reason: "sidecar_missing",
        message:
          "找不到 SWIM 边车。构建:cd swim && go build -o ../.tmp/swim-sidecar . " +
          "或用 PI_TEAM_SWIM_SIDECAR 指定路径。",
      });
      return false;
    }

    // 注意:-deliver 需要在 mesh 监听端口确定后才能给。
    // 所以先起 mesh,再起边车。
    const args = [
      "-name", self.name,
      "-token", token,
      "-bind", gossipBind,
      "-port", String(gossipPort),
      "-http", "127.0.0.1:0",
    ];
    if (self.host) args.push("-host", self.host);
    if (self.labels?.length) args.push("-labels", self.labels.join(","));
    if (seeds.length) args.push("-seeds", seeds.join(","));
    if (mesh?.port()) args.push("-deliver", String(mesh.port()));

    sidecar = spawn(binary, args, { stdio: ["ignore", "pipe", "pipe"] });

    let stdout = "";
    let stderr = "";
    sidecar.stdout.on("data", (d) => (stdout += d));
    sidecar.stderr.on("data", (d) => (stderr += d));

    sidecar.on("exit", (code) => {
      if (stopped) return;
      setState("offline", { reason: "sidecar_exited", code, message: stderr.slice(-300) });
    });

    const t0 = Date.now();
    while (Date.now() - t0 < 10_000) {
      const m = stdout.match(/LISTEN (http:\/\/\S+)/);
      if (m) {
        sidecarUrl = m[1];
        return true;
      }
      if (sidecar.exitCode !== null) {
        setState("offline", { reason: "sidecar_exit_early", code: sidecar.exitCode, message: stderr.slice(-300) });
        return false;
      }
      await sleep(40);
    }

    setState("offline", { reason: "sidecar_timeout", message: stderr.slice(-300) });
    return false;
  }

  /** 拉全量快照。事件可能丢,快照不会,所以它是权威来源。 */
  async function refreshMembers() {
    if (!sidecarUrl || stopped) return;
    try {
      const r = await fetch(`${sidecarUrl}/members`, { signal: AbortSignal.timeout(4000) });
      if (!r.ok) return;
      const data = await r.json();
      applySwimMembers(Array.isArray(data?.members) ? data.members : []);
    } catch {
      // 边车暂时不可达不影响既有视图,下次事件或轮询会修正
    }
  }

  function applySwimMembers(list) {
    // dead / left 不该保留:memberlist 稍后会彻底移除,期间发给它是浪费
    swimMembers = list
      .filter((m) => m?.name && m.name !== self?.name)
      .filter((m) => m.state === "alive" || m.state === "suspect")
      .map((m) => ({
        name: m.name,
        host: m.host ?? null,
        addr: m.addr ?? null,
        labels: Array.isArray(m.labels) ? m.labels : [],
        deliver: typeof m.deliver === "number" ? m.deliver : null,
        since: 0,
        endpoints: m.deliver && m.addr ? [`${m.addr}:${m.deliver}`] : [],
      }));

    // 把成员和端点注入 mesh 的投递层
    mesh?.setExternalMembers?.(swimMembers);

    members = swimMembers;
    bus.emit("membership", members);
  }

  async function subscribeEvents() {
    if (!sidecarUrl) return;
    const ac = new AbortController();
    eventsAbort = ac;

    try {
      const r = await fetch(`${sidecarUrl}/events`, {
        signal: ac.signal,
        headers: { accept: "text/event-stream" },
      });
      if (!r.ok || !r.body) return;

      const reader = r.body.getReader();
      const decoder = new TextDecoder();
      let buf = "";

      while (!stopped) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });

        let idx;
        while ((idx = buf.indexOf("\n\n")) >= 0) {
          const block = buf.slice(0, idx);
          buf = buf.slice(idx + 2);
          for (const line of block.split("\n")) {
            if (!line.startsWith("data: ")) continue;
            try {
              const ev = JSON.parse(line.slice(6));
              // 快照直接用,其它事件重拉 —— 重拉能顺带清掉彻底离开的节点
              if (ev.kind === "snapshot") applySwimMembers(ev.all ?? []);
              else void refreshMembers();
            } catch {}
          }
        }
      }
    } catch (err) {
      if (!stopped && err?.name !== "AbortError") {
        // SSE 断了不致命:轮询会兜底
      }
    }
  }

  return {
    mode: "swim",

    async start(nextSelf) {
      self = nextSelf;
      stopped = false;
      setState("connecting");

      // 1. 先起投递层,拿到监听端口 —— 边车要把它通告出去
      mesh = createMeshTransport({
        token,
        seeds: [], // 成员发现完全交给边车
        listenHost,
        listenPort,
        advertiseHost,
        heartbeatMs,
        // 关键:不要 mesh 自己那套发现,否则两套机制会互相干扰,
        // 表现为成员表里出现边车已经判死的节点
        externalMembership: true,
      });
      mesh.on("envelope", (env) => bus.emit("envelope", env));

      await new Promise((resolve) => {
        const off = mesh.on("state", (s, detail) => {
          if (s === "online") {
            off();
            resolve();
          }
          if (s === "offline" && detail?.reason === "listen_failed") {
            off();
            setState("offline", detail);
            resolve();
          }
        });
        mesh.start(nextSelf);
        setTimeout(resolve, 5000);
      });

      if (state === "offline") return;

      // 2. 起边车(带 deliver 端口)
      const ok = await startSidecar();
      if (!ok) return;

      // 3. 拉成员 + 订阅变更
      await refreshMembers();
      void subscribeEvents();

      setState("online");

      pollTimer = setInterval(() => void refreshMembers(), refreshMs);
      pollTimer.unref?.();
    },

    stop() {
      stopped = true;
      if (pollTimer) clearInterval(pollTimer);
      pollTimer = null;
      eventsAbort?.abort();
      eventsAbort = null;
      try {
        mesh?.stop();
      } catch {}
      mesh = null;
      if (sidecar) {
        try {
          sidecar.kill("SIGTERM");
        } catch {}
      }
      sidecar = null;
      sidecarUrl = null;
      swimMembers = [];
      members = [];
      setState("offline", { reason: "stopped" });
      bus.emit("membership", []);
    },

    send({ to, id, re = null, body }) {
      if (!mesh) return false;

      // 在 SWIM 成员表上解析 —— 它比 mesh 自己的视图权威,
      // 因为故障检测由它负责。
      const targets = resolveTargets(to, self?.name ?? "", members);
      if (!targets.length) return false;

      return mesh.send({ to: targets.length === 1 ? targets[0] : targets, id, re, body });
    },

    state: () => state,
    members: () => members,
    on: bus.on,

    /** 诊断用 */
    sidecarUrl: () => sidecarUrl,
    deliverPort: () => mesh?.port() ?? null,
  };
}
