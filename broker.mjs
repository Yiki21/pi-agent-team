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
 *  - 不做 TLS:只监听 tailscale 接口,链路已被 WireGuard 加密。
 *    再叠一层只是多一份证书要管。
 *  - 不做离线队列:对端离线立刻回执 undeliverable。
 *    静默排队会让"对方到底收没收到"变成不可知。
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

/** @type {Map<string, {name: string, socket: import('node:net').Socket, since: number, alive: boolean}>} */
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

// roster 是"当前所有在线节点",不排除任何人。
// 排除谁应该由接收方自己决定 —— 之前我在广播时排除了新加入者,
// 导致每个接收方拿到的名单都不一样(测试抓到的就是这个)。
const roster = () => [...peers.keys()];

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

// ------------------------------------------------------------------ 握手

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/** @returns {string | null} 通过校验的节点名 */
function handshake(req, socket) {
  if ((req.headers.upgrade ?? "").toLowerCase() !== "websocket") {
    socket.destroy();
    return null;
  }

  const url = new URL(req.url ?? "/", "http://placeholder");
  const name = (url.searchParams.get("name") ?? "").trim();

  const reject = (code, reason) => {
    socket.write(`HTTP/1.1 ${code} ${reason}\r\nConnection: close\r\n\r\n`);
    socket.destroy();
    return null;
  };

  if (!tokenOk(url.searchParams.get("token"))) {
    log(`拒绝:token 错误 (fp=${fingerprint})`);
    return reject(401, "Unauthorized");
  }
  if (!NAME_RE.test(name)) {
    log(`拒绝:非法名字 ${JSON.stringify(name)}`);
    return reject(400, "Bad Request");
  }
  if (peers.has(name)) {
    log(`拒绝:名字占用 ${name}`);
    return reject(409, "Conflict");
  }

  const key = req.headers["sec-websocket-key"];
  if (typeof key !== "string") return reject(400, "Bad Request");

  const accept = createHash("sha1").update(key + WS_GUID).digest("base64");
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
      "Upgrade: websocket\r\n" +
      "Connection: Upgrade\r\n" +
      `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
  );
  socket.setNoDelay?.(true);
  socket.setTimeout?.(0);
  return name;
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

  if (env.to === "*") {
    let n = 0;
    for (const [peerName, peer] of peers) {
      if (peerName === name) continue;
      if (write(peer.socket, env)) n++;
    }
    log(`${name} 广播 → ${n} 个对端`);
    return;
  }

  if (typeof env.to !== "string" || env.to.length === 0) {
    log(`丢弃:${name} 的 to 非法`);
    return;
  }

  const target = peers.get(env.to);
  if (!target) {
    write(socket, sys({ kind: "undeliverable", reason: "offline", to: env.to }, name, env.id));
    log(`${name} → ${env.to}:离线`);
    return;
  }
  if (!write(target.socket, env)) {
    write(socket, sys({ kind: "undeliverable", reason: "write_failed", to: env.to }, name, env.id));
    return;
  }

  // 回执只证明"写进了对端 socket",不证明对端 LLM 处理了。
  // 命名上刻意区分,避免调用方误读。
  write(socket, sys({ kind: "delivered", to: env.to }, name, env.id));
  log(`${name} → ${env.to} ok (id=${env.id.slice(0, 8)})`);
}

// ------------------------------------------------------------------ HTTP

const server = createServer((req, res) => {
  if ((req.url ?? "").split("?")[0] === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, count: peers.size, peers: [...peers.keys()] }));
    return;
  }
  res.writeHead(404, { "content-type": "text/plain" });
  res.end("pi-agent-team broker\n");
});

// ------------------------------------------------------------------ 连接

server.on("upgrade", (req, socket) => {
  const name = handshake(req, socket);
  if (!name) return;

  const touch = () => {
    const p = peers.get(name);
    if (p) p.alive = true;
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
    if (peers.get(name)?.socket !== socket) return;
    peers.delete(name);
    log(`离开:${name}(在线 ${peers.size})`);
    broadcast(sys({ kind: "peer_left", peer: name, peers: roster() }));
  });

  peers.set(name, { name, socket, since: Date.now(), alive: true });
  log(`加入:${name}(在线 ${peers.size})`);

  write(socket, sys({ kind: "welcome", peer: name, peers: roster().filter((n) => n !== name) }));
  broadcast(sys({ kind: "peer_joined", peer: name, peers: roster() }), name);
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

server.listen(args.port, args.bind, () => {
  log(`broker 监听 ws://${args.bind}:${args.port}`);
  log(`token 指纹 ${fingerprint}(日志核对用,不是 token 本身)`);
  log(`节点连接:ws://${args.bind}:${args.port}?name=<name>&token=<TEAM_TOKEN>`);
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
