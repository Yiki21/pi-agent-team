/**
 * Transport 工厂:按 TEAM_MODE 选择投递方式。
 *
 * 三种模式共用同一套 Team API(roadmap 8.2)。调用方只看到 start/
 * stop/send/members/on,看不到底下的 WebSocket、gossip、重连这些。
 *
 * 三种模式都必须通过 e2e/conformance.js 的同一套保证。语义不会在
 * 模式之间分叉,因为只有一套测试。
 *
 * ── 需要什么配置 ──
 *   broker  url(broker 地址)        必填
 *   mesh    seeds(至少一个)         可选但强烈建议;没有种子的节点
 *                                   只能等别人连它
 *   swim    seeds + 边车可执行文件   边车缺失是硬失败,不静默降级
 *
 * 所有模式都要 token。
 */

import { createBrokerTransport } from "./transport.js";
import { createMeshTransport } from "./transport-mesh.js";
import { createSwimTransport, sidecarAvailable } from "./transport-swim.js";

export const MODES = ["broker", "mesh", "swim"];

/** 从配置和环境变量解析出模式 */
export function resolveMode({ config = {}, env = process.env } = {}) {
  const raw = env.TEAM_MODE ?? config.mode ?? "broker";
  if (!MODES.includes(raw)) {
    return { ok: false, reason: `TEAM_MODE 只能是 ${MODES.join(" / ")},实际 "${raw}"` };
  }
  return { ok: true, mode: raw };
}

/**
 * 创建 transport。
 *
 * @param {{
 *   mode: "broker"|"mesh"|"swim",
 *   config: { url?: string, token: string, seeds?: string[], labels?: string[] },
 *   listenHost?: string, listenPort?: number, advertiseHost?: string|null,
 *   sidecarPath?: string|null,
 * }} opts
 */
export function createTransport({
  mode,
  config,
  listenHost = "0.0.0.0",
  listenPort = 0,
  advertiseHost = null,
  sidecarPath = null,
}) {
  const token = config?.token;
  if (!token) return { ok: false, reason: "缺少 token" };

  switch (mode) {
    case "broker": {
      if (!config.url) {
        return { ok: false, reason: "broker 模式需要 url。用 /team create <team> <url> 或 /team join <team> <url> <token>" };
      }
      return {
        ok: true,
        transport: createBrokerTransport({ url: toSocketUrl(config.url), token }),
      };
    }

    case "mesh": {
      const seeds = normalizeSeeds(config.seeds);
      if (!seeds.length) {
        // 不是致命错误,但要说清楚后果:没有种子的节点只能被动等待,
        // 自己发现不了任何人。
        return {
          ok: true,
          transport: createMeshTransport({ token, seeds, listenHost, listenPort, advertiseHost }),
          warning:
            "mesh 模式没有配置 seeds:本节点要等别人主动连你才会被看到。" +
            "配一个 seeds 就能双向发现。",
        };
      }
      return { ok: true, transport: createMeshTransport({ token, seeds, listenHost, listenPort, advertiseHost }) };
    }

    case "swim": {
      if (!sidecarAvailable(sidecarPath)) {
        return {
          ok: false,
          reason:
            "swim 模式需要边车可执行文件,但没找到。构建方式:" +
            "cd swim && go build -o ../.tmp/swim-sidecar . " +
            "或设置 PI_TEAM_SWIM_SIDECAR 指向它。",
        };
      }
      return {
        ok: true,
        transport: createSwimTransport({
          token,
          seeds: normalizeSeeds(config.seeds),
          sidecarPath,
          listenHost,
          listenPort,
          advertiseHost,
        }),
      };
    }

    default:
      return { ok: false, reason: `未知模式 "${mode}"` };
  }
}

/** seeds 可以写成字符串(逗号分隔)或数组 */
export function normalizeSeeds(raw) {
  if (!raw) return [];
  const list = Array.isArray(raw) ? raw : String(raw).split(",");
  return list.map((s) => String(s).trim()).filter(Boolean);
}

/** http(s) → ws(s),broker transport 内部需要 */
export function toSocketUrl(url) {
  return String(url).replace(/^https:\/\//, "wss://").replace(/^http:\/\//, "ws://");
}

/**
 * 该模式的配置是否完整。给 /team 命令做提示用,不抛异常。
 */
export function modeReadiness(mode, config = {}, sidecarPath = null) {
  if (!config?.token) return { ready: false, reason: "缺少 token" };
  switch (mode) {
    case "broker":
      return config.url
        ? { ready: true }
        : { ready: false, reason: "broker 模式需要 url" };
    case "mesh":
      return normalizeSeeds(config.seeds).length
        ? { ready: true }
        : { ready: true, warning: "没有 seeds,只能被动等待别人连你" };
    case "swim":
      return sidecarAvailable(sidecarPath)
        ? { ready: true }
        : { ready: false, reason: "找不到 SWIM 边车可执行文件" };
    default:
      return { ready: false, reason: `未知模式 ${mode}` };
  }
}
