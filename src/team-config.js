/**
 * Team 配置的读写。create / join / leave team 的落地层。
 *
 * ── 授权模型(用户选定:轻量,token 即团队)──
 *   一个 broker 地址 + 一个 token = 一个 team,就是全部凭证。
 *   没有成员白名单、没有加入审批、没有"谁能改 label"的规则。
 *
 *   这带来一个必须说清楚的后果:任何拿到 token 的进程都能用任意空闲
 *   名字加入,也可以顶掉一个在线节点。token 只在 tailnet 内传递,
 *   而且在 Tailscale 建立连接时机器身份已经验证过,所以这个模型在
 *   当前威胁模型下是可接受的。改这个模型要连着改 roadmap 第 11 节。
 *
 * ── 为什么不写进项目目录 ──
 *   token 是凭据。放 `<project>/.pi/...` 会被 git add 进去。
 *   统一放用户级目录,并且 0600。
 */

import { randomBytes } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

import { MODES } from "./mode.js";

/** 名字必须和 broker 的校验一致,否则能写下来却连不上 */
const NAME_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$/;

const TEAM_RE = /^[a-z0-9][a-z0-9._-]{0,31}$/;

/** 生成 token:32 字节 = 64 hex,和文档建议的 `openssl rand -hex 32` 一致 */
export const generateToken = () => randomBytes(32).toString("hex");

export const configDir = (home = homedir()) => join(home, ".pi", "agent", "pi-agent-team");

const teamFile = (team, home) => join(configDir(home), `${team}.json`);

/**
 * 校验 team 名。和节点名一样,不允许大写,避免和文件名大小写敏感
 * 交互出奇怪结果(macOS 上是大小写不敏感的,Linux 上不是)。
 */
export function validateTeamName(name) {
  if (typeof name !== "string" || !TEAM_RE.test(name)) {
    return { ok: false, reason: "team 名只能用小写字母、数字、. _ -,1-32 字符,且以字母数字开头" };
  }
  return { ok: true };
}

export function validateAgentName(name) {
  if (typeof name !== "string" || !NAME_RE.test(name)) {
    return { ok: false, reason: "节点名只能用字母、数字、. _ -,1-32 字符,且以字母或数字开头" };
  }
  return { ok: true };
}

/** 列出本机已有的 team */
export function listTeams(home = homedir()) {
  const dir = configDir(home);
  if (!existsSync(dir)) return [];
  try {
    return readdirSync(dir)
      .filter((f) => f.endsWith(".json"))
      .map((f) => f.slice(0, -5))
      .sort();
  } catch {
    return [];
  }
}

/** 读一个 team 的配置。不存在或损坏都返回 null,由调用方决定怎么报。 */
export function readTeam(team, home = homedir()) {
  const path = teamFile(team, home);
  if (!existsSync(path)) return null;
  try {
    const data = JSON.parse(readFileSync(path, "utf8"));
    if (!data || typeof data.token !== "string") return null;
    // 旧配置没有 mode 字段 → 视为 broker(那是当时唯一的模式)
    if (!data.mode) data.mode = "broker";
    return data;
  } catch {
    return null;
  }
}

/**
 * 写一个 team 的配置。
 *
 * 目录 0700、文件 0600 —— 文件里是凭据。touch 之前先 mkdir,
 * 避免 rename 式写入在目录不存在时失败。
 */
export function writeTeam(team, config, home = homedir()) {
  const dir = configDir(home);
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  const path = teamFile(team, home);
  writeFileSync(path, `${JSON.stringify({ ...config, team }, null, 2)}\n`, { mode: 0o600 });
  chmodSync(path, 0o600); // 已存在的文件不会被 mode 参数收紧,必须显式 chmod
  return path;
}

export function removeTeam(team, home = homedir()) {
  const path = teamFile(team, home);
  if (!existsSync(path)) return false;
  rmSync(path);
  return true;
}

// ---------------------------------------------------------------- 高层操作

/**
 * create_team:生成配置。
 *
 * 注意它**不启动任何东西**,也不验证 broker 是否可达 —— 创建是本地动作。
 * join 才负责连上。
 */
export function createTeam({ team, url, token, labels = [], mode = "broker", seeds = [], home = homedir() }) {
  const t = validateTeamName(team);
  if (!t.ok) return { ok: false, reason: t.reason };

  if (!MODES.includes(mode)) {
    return { ok: false, reason: `mode 只能是 ${MODES.join(" / ")},实际 "${mode}"` };
  }

  // broker 模式必须有 url;mesh/swim 用 seeds,url 可以不填。
  // 这放宽是必要的:多模式支持之前,url 是唯一入口,现在不是了。
  let normalized = null;
  if (url) {
    // 先规范化再校验。反过来的话 ws:// 会在校验那步被拒,
    // 而用户只是写了个等价的写法。
    normalized = normalizeUrl(url);
    if (typeof normalized !== "string" || !/^https?:\/\//.test(normalized)) {
      return { ok: false, reason: "url 必须以 http:// 或 https:// 开头(ws:// 和 wss:// 也接受,会转换)" };
    }
  } else if (mode === "broker") {
    return { ok: false, reason: "broker 模式需要 url(mesh/swim 用 seeds)" };
  }
  if (readTeam(team, home)) {
    return { ok: false, reason: `team "${team}" 已存在。用 /team join ${team} 加入,或先 /team leave ${team}` };
  }
  if (token && !/^[0-9a-fA-F]{16,}$/.test(token)) {
    // 不强制,只是提醒。用 openssl rand -hex 32 生成的正好符合。
    return { ok: false, reason: "token 看起来不像 hex(建议 openssl rand -hex 32 生成 64 位 hex)" };
  }

  const finalToken = token || generateToken();
  // 不写空字段 —— 空数组是 truthy,读回来会覆盖调用方给的值
  const config = { mode, token: finalToken, createdAt: new Date().toISOString() };
  if (normalized) config.url = normalized;
  if (labels?.length) config.labels = labels;
  const cleanSeeds = (Array.isArray(seeds) ? seeds : String(seeds ?? "").split(","))
    .map((s) => String(s).trim())
    .filter(Boolean);
  if (cleanSeeds.length) config.seeds = cleanSeeds;
  const path = writeTeam(team, config, home);

  return { ok: true, path, created: !token, token: finalToken, config };
}

/**
 * join_team:读配置,返回连接参数。
 *
 * 找不到本地配置时,允许用 url + token 现场加入并记下来 —— 否则
 * 每台机器都要先手工建配置文件,那就不像"join"了。
 */
export function joinTeam({ team, url, token, mode, seeds, save = true, home = homedir() }) {
  const t = validateTeamName(team);
  if (!t.ok) return { ok: false, reason: t.reason };

  const existing = readTeam(team, home);

  if (!existing) {
    if (!token) {
      const known = listTeams(home);
      return {
        ok: false,
        reason: known.length
          ? `本地没有 team "${team}"。已有的:${known.join(", ")}`
          : `本地没有 team "${team}",首次加入需要 token`,
      };
    }
    const created = createTeam({ team, url, token, mode, seeds, home });
    if (!created.ok) return created;
    return { ok: true, config: created.config, path: created.path, adopted: true };
  }

  // 已有配置又被显式给了 url/token:更新它
  if (url || token) {
    const config = { ...existing, url: url ? normalizeUrl(url) : existing.url, token: token ?? existing.token };
    const path = writeTeam(team, config, home);
    return { ok: true, config, path, updated: true };
  }

  return { ok: true, config: existing, path: teamFile(team, home) };
}

/**
 * leave_team:清掉本地配置。
 *
 * 只删本地记录,不动远端 —— broker 没有任何持久状态,节点断开
 * 就等于离开了。
 */
export function leaveTeam({ team, home = homedir() }) {
  const t = validateTeamName(team);
  if (!t.ok) return { ok: false, reason: t.reason };

  const existed = removeTeam(team, home);
  return existed
    ? { ok: true, team }
    : { ok: false, reason: `本地没有 team "${team}"` };
}

/** 把 ws:// 统一成 http://,因为命令行和文档里两种都有人写 */
export function normalizeUrl(url) {
  return String(url).replace(/^ws:\/\//, "http://").replace(/^wss:\/\//, "https://");
}

/** 内部用:http(s) → ws(s) */
export const toSocketUrl = (url) =>
  String(url).replace(/^https:\/\//, "wss://").replace(/^http:\/\//, "ws://");
