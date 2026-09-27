#!/usr/bin/env node
/**
 * pi-agent-team broker
 *
 * 常驻的发现 + 路由节点。不认识消息内容,不落盘,无状态。
 *
 * 设计取舍:
 *  - 所有节点主动 dial out 到 broker,完全对称,没有 leader 选举。
 *    代价是 broker 是单点 —— 但它挂了两边 Pi 各自照常工作,
 *    只是跨机消息不通。这个失败模式可以接受。
 *  - 不做 TLS:只监听 tailnet 接口,链路已被 WireGuard 加密。
 *  - 不做离线队列:对端离线立刻回执 undeliverable。
 *    静默排队会让"对方到底收没收到"变成不可知。
 *  - 同名接管:同名新连接踢掉旧连接。同名几乎总是意味着旧进程
 *    已经死了或被 tmux kill 掉而心跳还没超时 —— 让新实例干等
 *    30 秒是更差的行为。代价是持 token 者可顶掉任意节点;
 *    token 本身就是唯一凭证,而且只在 tailnet 内,可接受。
 *
 * 用法:
 *   TEAM_TOKEN=xxx node broker.mjs --bind $(tailscale ip -4)
 */

import { createServer } from "node:http";
import { createHash, timingSafeEqual } from "node:crypto";
import { FrameReader, encodeFrame, closeFrame, pingFrame, pongFrame } from "./src/ws.js";

// ------------------------------------------------------------------ 参数

function parseArgs(argv) {
  const out = { port: 8787, bind: null, heartbeat: 30_000 };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--port") out.port = Number(argv[++i]);
    else if (a === "--bind") out.bind = argv[++i];
    else if (a === "--heartbeat") out.heartbeat = Number(argv[++i]);
    else if (a === "--help" || a === "-h") out.help = true;
  }
  return out;
}

const args = parseArgs(process.argv.slice(2));

if (args.help) {
  console.log(`pi-agent-team broker

  --bind <ip>        监听地址(默认 127.0.0.1;跨机请填 tailscale IP)
  --port <n>         监听端口(默认 8787)
  --heartbeat <ms>   心跳探测间隔(默认 30000)

环境变量:
  TEAM_TOKEN         必需。所有节点连接时校验。
`);
  process.exit(0);
}

const TOKEN = process.env.TEAM_TOKEN;
const NAME_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$/;

/** 一个节点最多声明多少个 tag,防止滥用 */
const MAX_TAGS = 8;
const TAG_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$/;

/** 关闭码:被同名新连接顶掉。客户端收到它不应该重连。 */
const CLOSE_REPLACED = 4001;

if (!TOKEN) {
  console.error("拒绝启动:必须设置 TEAM_TOKEN。没有 token 的 broker 是敞开的。");
  process.exit(1);
}
if (args.bind === null) {
  console.warn("warn: 未指定 --bind,默认只听 127.0.0.1(仅本机可连)");
  console.warn("warn: 跨机部署请用  --bind $(tailscale ip -4)");
  args.bind = "127.0.0.1";
}
if (args.bind === "0.0.0.0" || args.bind === "::") {
  console.error("拒绝启动:不要绑 0.0.0.0,绑 tailscale 接口。");
  process.exit(1);
}

// ------------------------------------------------------------------ 状态

/**
 * @type {Map<string, {
 *   name: string, socket: import('node:net').Socket, since: number,
 *   alive: boolean, tags: string[], host: string | null, addr: string | null
 * }>}
 */
const peers = new Map();

const log = (...a) => console.log(new Date().toISOString(), ...a);
const fingerprint = createHash("sha256").update(TOKEN).digest("hex").slice(0, 8);

function tokenOk(presented) {
  const a = Buffer.from(presented ?? "");
  const b = Buffer.from(TOKEN);
  if (a.length !== b.length) return false;
  return timingSafeEqual(a, b);
}

function write(socket, obj) {
  if (socket.destroyed || !socket.writable) return false;
  try {
    return socket.write(encodeFrame(JSON.stringify(obj)));
  } catch (err) {
    log("write failed:", err.message);
    return false;
  }
}

/**
 * 成员快照。
 *
 * 返回对象而不是名字数组 —— 因为客户端需要 host 和 tags 才能
 * 分组显示和做 #tag 群发。名字数组是这套信息的退化形式。
 */
function members() {
  return [...peers.values()].map((p) => ({
    name: p.name,
    host: p.host,
    addr: p.addr,
    tags: p.tags,
    since: p.since,
  }));
}

function nameList() {
  return [...peers.keys()];
}

let sysSeq = 0;
function sys(body, to = "*", re = null) {
  return {
    from: "broker",
    to,
    id: `sys-${Date.now().toString(36)}-${(sysSeq++).toString(36)}`,
    re,
    body,
  };
}

function broadcast(obj, exceptName) {
  for (const [name, peer] of peers) {
    if (name === exceptName) continue;
    write(peer.socket, obj);
  }
}

// ------------------------------------------------------------------ 收件人解析

/**
 * 把信封的 `to` 解析成实际收件人名单。
 *
 * 支持四种形态:
 *   "name"                   单个节点
 *   "*"                      除发送者外的所有人
 *   ["a","b"]                显式多收件人
 *   "#tag" / ["#tag","a"]    按 tag 分组(会和显式名字去重合并)
 *
 * @returns {{ targets: string[], unknown: string[], empty: string[] }}
 *   targets  实际能投递的节点名
 *   unknown  指了但不存在的名字或空分组
 *   empty    [] —— 保留字段,语义上等于 unknown 里的分组
 */
function resolveTargets(to, senderName) {
  const requested = Array.isArray(to) ? to : [to];
  const targets = new Set();
  const unknown = [];

  for (const raw of requested) {
    if (typeof raw !== "string") continue;
    const t = raw.trim();
    if (!t) continue;

    if (t === "*") {
      for (const name of peers.keys()) if (name !== senderName) targets.add(name);
      continue;
    }

    // 默认组:不指定收件人时等同于全员。这里的语义和客户端的
    // parseRecipients 必须一致,否则"默认组"在不同层指向不同集合。
    // 必须在下面的 @ 分支之前判,否则 @default 会被当成 label "default"。
    if (t === "@default" || t === "default") {
      for (const name of peers.keys()) if (name !== senderName) targets.add(name);
      continue;
    }

    if (t.startsWith("#") || t.startsWith("@")) {
      // 两种前缀都认:# 是早期语法,@ 是现在的用户面向语法。
      // 只认一种会让客户端"本地算出命中 1 个"但 broker 报 unknown ——
      // 两侧语法不同步时的典型症状。
      const tag = t.slice(1);
      let matched = 0;
      for (const p of peers.values()) {
        if (p.name === senderName) continue;
        if (p.tags.includes(tag)) {
          targets.add(p.name);
          matched++;
        }
      }
      // 空分组要报出来,否则发送方以为"发给了全部 web 节点",
      // 实际一个都没匹配到却显示成功。
      if (matched === 0) unknown.push(t);
      continue;
    }

    if (!peers.has(t)) {
      unknown.push(t);
      continue;
    }
    targets.add(t);
  }

  return { targets: [...targets], unknown };
}

// ------------------------------------------------------------------ 握手

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

function reject(socket, code, reason) {
  socket.write(`HTTP/1.1 ${code} ${reason}\r\nConnection: close\r\n\r\n`);
  socket.destroy();
  return null;
}

function parseTags(raw) {
  if (!raw) return [];
  const seen = new Set();
  for (const part of raw.split(",")) {
    const t = part.trim();
    if (!t || t.length > 32) continue;
    if (!TAG_RE.test(t)) continue;
    seen.add(t);
    if (seen.size >= MAX_TAGS) break;
  }
  return [...seen];
}

/**
 * 握手。
 *
 * @param {boolean} allowTakeover 是否允许顶掉同名旧连接。
 *   e2e 里可以关掉来测"冲突被拒"这条路径,但生产默认开。
 * @returns {{ name: string, tags: string[], host: string | null } | null}
 */
function handshake(req, socket, allowTakeover) {
  if ((req.headers.upgrade ?? "").toLowerCase() !== "websocket") {
    socket.destroy();
    return null;
  }

  const url = new URL(req.url ?? "/", "http://placeholder");
  const name = (url.searchParams.get("name") ?? "").trim();

  if (!tokenOk(url.searchParams.get("token"))) {
    log(`拒绝:token 错误 (fp=${fingerprint})`);
    return reject(socket, 401, "Unauthorized");
  }
  if (!NAME_RE.test(name)) {
    log(`拒绝:非法名字 ${JSON.stringify(name)}`);
    return reject(socket, 400, "Bad Request");
  }

  // 同名接管:踢掉旧连接,让新连接接管这个名字。
  const existing = peers.get(name);
  if (existing) {
    if (!allowTakeover) {
      log(`拒绝:名字占用 ${name}`);
      return reject(socket, 409, "Conflict");
    }
    log(`接管:${name}(踢掉旧连接,它已在线 ${Math.round((Date.now() - existing.since) / 1000)}s)`);
    // 先从表里摘掉,再关 socket —— 顺序很重要。
    // 反过来的话,旧 socket 的 close 处理器会看到 peers.get(name)
    // 仍是自己,于是广播一条假的 peer_left。
    peers.delete(name);
    try {
      existing.socket.write(closeFrame(CLOSE_REPLACED));
      existing.socket.end();
    } catch {}
    // 不广播 peer_left:对其他人来说这个节点一直在,只是换了条连接。
  }

  const key = req.headers["sec-websocket-key"];
  if (typeof key !== "string") return reject(socket, 400, "Bad Request");

  const accept = createHash("sha1").update(key + WS_GUID).digest("base64");
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
      "Upgrade: websocket\r\n" +
      "Connection: Upgrade\r\n" +
      `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
  );
  socket.setNoDelay?.(true);
  socket.setTimeout?.(0);

  const host = url.searchParams.get("host");
  return {
    name,
    tags: parseTags(url.searchParams.get("tags")),
    host: host && host.length <= 64 ? host : null,
  };
}

// ------------------------------------------------------------------ 消息

function handleMessage(name, socket, text) {
  let env;
  try {
    env = JSON.parse(text);
  } catch {
    log(`丢弃:${name} 发来非 JSON`);
    return;
  }

  if (
    !env ||
    typeof env !== "object" ||
    typeof env.from !== "string" ||
    typeof env.id !== "string" ||
    !("to" in env) ||
    !("re" in env) ||
    !("body" in env)
  ) {
    log(`丢弃:${name} 发来畸形信封`);
    return;
  }

  // 不信任自报的 from:用连接身份覆盖
  if (env.from !== name) {
    log(`注意:${name} 自报 from=${env.from},已覆盖为连接身份`);
    env.from = name;
  }

  const toDesc = Array.isArray(env.to) ? env.to.join(",") : String(env.to);
  if (typeof env.to !== "string" && !Array.isArray(env.to)) {
    log(`丢弃:${name} 的 to 类型非法`);
    return;
  }

  const { targets, unknown } = resolveTargets(env.to, name);

  if (targets.length === 0) {
    write(
      socket,
      sys({ kind: "undeliverable", reason: unknown.length ? "unknown_recipient" : "no_recipients", to: toDesc, unknown }, name, env.id),
    );
    log(`${name} → ${toDesc}:无可投递对象${unknown.length ? `(未知:${unknown.join(",")})` : ""}`);
    return;
  }

  let delivered = 0;
  const failed = [];
  for (const target of targets) {
    const peer = peers.get(target);
    if (!peer || !write(peer.socket, env)) {
      failed.push(target);
      continue;
    }
    delivered++;
  }

  // 单播保持和以前一样:delivered 直接对应该收件人。
  // 群发时给出汇总,并列出没成功的。
  write(
    socket,
    sys(
      {
        kind: delivered === 0 ? "undeliverable" : "delivered",
        to: toDesc,
        delivered,
        total: targets.length,
        ...(failed.length ? { failed } : {}),
        ...(unknown.length ? { unknown } : {}),
        ...(delivered === 0 ? { reason: "write_failed" } : {}),
      },
      name,
      env.id,
    ),
  );

  log(
    `${name} → ${toDesc}:投递 ${delivered}/${targets.length}` +
      (failed.length ? ` 失败:${failed.join(",")}` : "") +
      (unknown.length ? ` 未知:${unknown.join(",")}` : ""),
  );
}

// ------------------------------------------------------------------ HTTP

const server = createServer((req, res) => {
  const path = (req.url ?? "").split("?")[0];
  if (path === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    // peers 保留为名字数组(简单、稳定);members 是完整快照。
    // 两个都给,免得改一处就破一个监控脚本。
    res.end(JSON.stringify({ ok: true, count: peers.size, peers: nameList(), members: members() }));
    return;
  }
  // 握手前的 token 诊断。
  //
  // 存在的理由:Node 内置 WebSocket 会把握手阶段的 HTTP 状态藏起来。
  // 401 到了客户端只剩 error(空消息)+ close 1006,和网络断开无法区分。
  // 客户端连接失败后调一次这个端点,才能说清"是 token 不对"。
  //
  // 只接受 Authorization 头,不接受 query —— query 会进访问日志和
  // 浏览器历史,token 不该出现在那儿。
  if (path === "/auth") {
    const auth = req.headers.authorization ?? "";
    const presented = auth.startsWith("Bearer ") ? auth.slice(7) : null;

    if (!presented) {
      res.writeHead(401, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: false, reason: "token_missing" }));
      return;
    }
    if (!tokenOk(presented)) {
      // 只给指纹,不给 token:用户能核对"服务器是哪个 token",
      // 而信息本身泄露不了什么。
      res.writeHead(401, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: false, reason: "token_mismatch", fingerprint }));
      return;
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  res.writeHead(404, { "content-type": "text/plain" });
  res.end("pi-agent-team broker\n");
});

// ------------------------------------------------------------------ 连接

// 允许通过环境变量关闭接管,只为测试"冲突被拒"这条路径存在。
const ALLOW_TAKEOVER = process.env.TEAM_NO_TAKEOVER !== "1";

server.on("upgrade", (req, socket) => {
  const info = handshake(req, socket, ALLOW_TAKEOVER);
  if (!info) return;

  const { name, tags, host } = info;

  const touch = () => {
    const p = peers.get(name);
    if (p && p.socket === socket) p.alive = true;
  };

  const reader = new FrameReader({
    onText: (text) => {
      touch();
      handleMessage(name, socket, text);
    },
    onPing: () => {
      touch();
      socket.write(pongFrame());
    },
    onPong: touch,
    onClose: () => socket.end(closeFrame(1000)),
  });

  socket.on("data", (chunk) => {
    try {
      reader.feed(chunk);
    } catch (err) {
      log(`${name} 帧错误:${err.message} —— 断开`);
      socket.destroy();
    }
  });
  socket.on("error", () => {});
  socket.on("close", () => {
    // 只有"当前占用这个名字的 socket"才有资格宣布离开。
    // 被接管时 handshake 已经把名字摘走了,所以这里会直接返回 ——
    // 这正是防止假 peer_left 抖动的那个判断。
    if (peers.get(name)?.socket !== socket) return;
    peers.delete(name);
    log(`离开:${name}(在线 ${peers.size})`);
    broadcast(sys({ kind: "peer_left", peer: name, peers: nameList(), members: members() }));
  });

  const remote = socket.remoteAddress ?? null;
  peers.set(name, {
    name,
    socket,
    since: Date.now(),
    alive: true,
    tags,
    host,
    addr: remote,
  });
  log(`加入:${name}(在线 ${peers.size})${tags.length ? ` tags=${tags.join(",")}` : ""}${host ? ` host=${host}` : ""}`);

  write(socket, sys({ kind: "welcome", peer: name, peers: nameList().filter((n) => n !== name), members: members() }));
  broadcast(sys({ kind: "peer_joined", peer: name, peers: nameList(), members: members() }), name);
});

// 心跳:探活并踢掉半死连接。没有它,拔网线的客户端会永远留在
// peers 里,后续发给它的消息全部写进黑洞。
const heartbeat = setInterval(() => {
  for (const [name, peer] of peers) {
    if (!peer.alive) {
      log(`心跳超时:${name} —— 断开`);
      peer.socket.destroy();
      continue;
    }
    peer.alive = false;
    if (!peer.socket.writable || !socketWritePing(peer.socket)) {
      peer.socket.destroy();
    }
  }
}, args.heartbeat);
heartbeat.unref?.();

function socketWritePing(socket) {
  try {
    return socket.write(pingFrame());
  } catch {
    return false;
  }
}

// ------------------------------------------------------------------ 启动

/**
 * 绑定失败时的退出码。
 *
 * 78 是 sysexits(3) 的 EX_CONFIG —— "配置错了,重试没用"。
 * 选一个有意义的码是为了让 systemd 能写:
 *   RestartPreventExitStatus=78
 * 否则它会按 Restart=on-failure 无限重启一个永远起不来的进程,
 * 日志里刷满同样的错误。
 */
const EXIT_CONFIG = 78;

server.on("error", (err) => {
  if (err?.code === "EADDRINUSE") {
    // 以前这里是裸的 server.listen,端口被占时抛的是 Node 的原始堆栈:
    // 不说是谁占着,也不提最常见的原因 —— 已经有一个 broker 在跑了。
    // 真实场景就撞到过:一个 systemd 服务占着 8787,又手动起了一个。
    console.error(`\n拒绝启动:${args.bind}:${args.port} 已被占用。\n`);
    console.error("查是谁占着:");
    console.error(`  ss -ltnp | grep ${args.port}          # Linux`);
    console.error(`  lsof -nP -iTCP:${args.port} -sTCP:LISTEN   # macOS`);
    console.error("");
    console.error("最常见的原因:这台机器上已经有一个 broker 在跑了 ——");
    console.error("可能是先前手动启动的,也可能装成了服务:");
    console.error("  systemctl status pi-agent-team-broker");
    console.error("  systemctl --user status pi-agent-team-broker");
    console.error("");
    console.error("确认之后,要么停掉那个,要么给这个换一个端口:--port <其他端口>");
    process.exit(EXIT_CONFIG);
  }

  if (err?.code === "EACCES") {
    console.error(`\n拒绝启动:无权绑定 ${args.bind}:${args.port}。`);
    console.error("1024 以下的端口需要特权。换一个高位端口,或改绑地址。\n");
    process.exit(EXIT_CONFIG);
  }

  if (err?.code === "EADDRNOTAVAIL") {
    console.error(`\n拒绝启动:本机没有地址 ${args.bind}。`);
    console.error("这个地址必须已经配在本机某个接口上。常见做法是绑 tailscale IP:");
    console.error("  --bind \"$(tailscale ip -4)\"");
    console.error("或者只给本机用:--bind 127.0.0.1\n");
    process.exit(EXIT_CONFIG);
  }

  console.error(`\n拒绝启动:绑定 ${args.bind}:${args.port} 失败:${err?.code ?? err?.message}\n`);
  process.exit(EXIT_CONFIG);
});

server.listen(args.port, args.bind, () => {
  log(`broker 监听 ws://${args.bind}:${args.port}`);
  log(`token 指纹 ${fingerprint}(日志核对用,不是 token 本身)`);
  log(`接管模式 ${ALLOW_TAKEOVER ? "开(同名新连接踢旧连接)" : "关(同名返回 409)"}`);
});

function shutdown(signal) {
  log(`${signal} —— 关闭`);
  clearInterval(heartbeat);
  for (const { socket } of peers.values()) {
    try {
      socket.write(closeFrame(1001));
      socket.end();
    } catch {}
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 1500).unref();
}
for (const sig of ["SIGINT", "SIGTERM"]) process.on(sig, () => shutdown(sig));
