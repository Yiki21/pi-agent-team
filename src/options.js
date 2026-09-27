/**
 * 连接选项:解析、合并、校验。
 *
 * ── 为什么要单独一个模块 ──
 *   同一组选项有三个入口(环境变量 / /team 命令 / team_join 工具),
 *   如果各写一份解析,它们迟早会分叉 —— 命令能设的选项工具设不了,
 *   或者两处对同一个值的校验不一致。所以解析和校验只在这里做一次。
 *
 * ── 两类选项,生命周期不同 ──
 *
 *   team 级(写进 ~/.pi/agent/pi-agent-team/<team>.json):
 *     url      broker 地址
 *     token    认证
 *     mode     broker | mesh | swim
 *     seeds    mesh/swim 的种子地址
 *
 *   session 级(只在本次运行有效,不落盘):
 *     name     节点名
 *     labels   本节点标签
 *     port     mesh/swim 的监听端口
 *     listen   mesh/swim 的监听地址
 *
 *   为什么这些不落盘:一台机器上可以同时跑几个 Pi,每个是一个独立
 *   节点,它们共用同一个 team 配置文件。
 *     name  存进去 → 第二个 agent 启动时覆盖第一个的名字,而名字就是身份
 *     port  存进去 → 第二个 agent 监听同一个端口,EADDRINUSE 起不来
 */

import { MODES } from "./mode.js";

/** team 级选项 → 落盘字段名 */
const TEAM_KEYS = new Set(["url", "token", "mode", "seeds"]);
/** session 级选项 → 只影响本次运行 */
const SESSION_KEYS = new Set(["name", "labels", "port", "listen"]);

export const ALL_KEYS = [...TEAM_KEYS, ...SESSION_KEYS];

/**
 * 解析 `--key value` / `--key=value` / `key=value` 混排的参数。
 *
 * 位置参数(不以 -- 开头且不带 =)原样留在 rest 里,让调用方按位置
 * 语义处理 —— /team join 的第一个位置参数是 team 名。
 *
 * @returns {{ values: Record<string,string>, rest: string[], unknown: string[] }}
 */
export function parseOptionArgs(args = []) {
  const values = {};
  const rest = [];
  const unknown = [];

  for (let i = 0; i < args.length; i++) {
    const raw = String(args[i] ?? "");
    if (!raw) continue;

    let key = null;
    let val = null;

    if (raw.startsWith("--")) {
      const body = raw.slice(2);
      const eq = body.indexOf("=");
      if (eq >= 0) {
        key = body.slice(0, eq);
        val = body.slice(eq + 1);
      } else {
        key = body;
        // 下一个参数是值,除非它本身是另一个选项
        const next = args[i + 1];
        if (next !== undefined && !String(next).startsWith("--")) {
          val = String(next);
          i++;
        } else {
          val = "";
        }
      }
    } else if (raw.includes("=") && !raw.startsWith("@")) {
      const eq = raw.indexOf("=");
      key = raw.slice(0, eq);
      val = raw.slice(eq + 1);
    } else {
      rest.push(raw);
      continue;
    }

    key = key.trim().toLowerCase();
    if (!ALL_KEYS.includes(key)) {
      unknown.push(key);
      continue;
    }
    values[key] = String(val).trim();
  }

  return { values, rest, unknown };
}

/** seeds 接受 "a:1,b:2" 或数组 */
export function parseSeeds(raw) {
  if (!raw) return [];
  const list = Array.isArray(raw) ? raw : String(raw).split(",");
  return list.map((s) => String(s).trim()).filter(Boolean);
}

/** labels 同理 */
export function parseLabels(raw) {
  if (!raw) return [];
  const list = Array.isArray(raw) ? raw : String(raw).split(",");
  return list.map((s) => String(s).trim()).filter(Boolean);
}

/**
 * 校验并规范化解析出来的选项。
 *
 * @returns {{ ok: true, team: object, session: object } | { ok: false, reason: string }}
 */
export function validateOptions(values = {}) {
  const team = {};
  const session = {};

  for (const [k, v] of Object.entries(values)) {
    if (v === "") continue; // 空值 = 没给,不要清掉已有配置

    if (k === "url") {
      if (!/^(https?|wss?):\/\//.test(v)) {
        return { ok: false, reason: `url 需要以 http:// 或 https:// 开头,实际 "${v}"` };
      }
      team.url = v;
    } else if (k === "token") {
      if (v.length < 16) return { ok: false, reason: "token 太短(至少 16 位)" };
      team.token = v;
    } else if (k === "mode") {
      if (!MODES.includes(v)) {
        return { ok: false, reason: `mode 只能是 ${MODES.join(" / ")},实际 "${v}"` };
      }
      team.mode = v;
    } else if (k === "seeds") {
      const seeds = parseSeeds(v);
      if (!seeds.length) return { ok: false, reason: "seeds 不能为空" };
      for (const s of seeds) {
        if (!/^[^\s:]+:\d{1,5}$/.test(s)) {
          return { ok: false, reason: `seed "${s}" 应该写成 host:port,例如 100.64.0.1:7946` };
        }
      }
      team.seeds = seeds;
    } else if (k === "name") {
      if (!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/.test(v)) {
        return { ok: false, reason: `节点名 "${v}" 不合法(字母数字开头,可用 _ . -,最长 64)` };
      }
      session.name = v;
    } else if (k === "port") {
      const port = Number(v);
      // 0 合法:交给内核分配。/team status 会报出实际端口。
      if (!Number.isInteger(port) || port < 0 || port > 65535) {
        return { ok: false, reason: `port 应该是 0-65535 的整数,实际 "${v}"(0 = 自动分配)` };
      }
      session.port = port;
    } else if (k === "listen") {
      // 不做严格 IP 校验:主机名和 IPv6 都合法,交给 listen() 去报错更准
      if (/\s/.test(v)) return { ok: false, reason: `listen 地址不能有空格,实际 "${v}"` };
      session.listen = v;
    } else if (k === "labels") {
      const labels = parseLabels(v);
      // 上限和 broker / 成员表一致,免得本机接受了而别人看不到
      if (labels.length > 8) return { ok: false, reason: `标签最多 8 个,实际 ${labels.length} 个` };
      session.labels = labels;
    }
  }

  return { ok: true, team, session };
}

/**
 * 模式相关的必填项检查。
 *
 * 放在这里而不是各个入口里,原因和解析一样:三个入口必须给出
 * 完全相同的判断,否则"命令能连上、工具连不上"这种问题会反复出现。
 */
export function checkModeRequirements({ mode, url, seeds, token }) {
  if (!token) return { ok: false, reason: "缺少 token(用 --token 或 TEAM_TOKEN 提供)" };

  if (mode === "broker") {
    if (!url) {
      return { ok: false, reason: "broker 模式需要 --url,例如 --url http://100.64.0.1:8787" };
    }
    return { ok: true };
  }

  if (!seeds?.length) {
    // 不是错误,但后果要说清楚:没有种子的节点只能等别人连它
    return {
      ok: true,
      warning: `${mode} 模式没有 --seeds:本节点只能等别人主动连你。给它一个已在线节点的地址即可双向发现。`,
    };
  }
  return { ok: true };
}

/** 给 /team status 和帮助信息用的一行说明 */
export const OPTION_HELP = [
  "  --url <http://host:port>    broker 地址(broker 模式必需)",
  "  --token <hex>               team token(必需)",
  "  --mode <broker|mesh|swim>   投递模式(默认 broker)",
  "  --seeds <host:port,...>     mesh/swim 的种子地址",
  "  --name <名字>               本节点名(仅本次运行)",
  "  --labels <a,b>              本节点标签(仅本次运行)",
  "  --port <n>                  mesh/swim 监听端口(仅本次运行,0 = 自动)",
  "  --listen <地址>             mesh/swim 监听地址(仅本次运行)",
].join("\n");
