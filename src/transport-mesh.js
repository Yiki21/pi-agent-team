/**
 * Mesh transport:全互联直连,没有中心节点。
 *
 * ── 拓扑 ──
 * 每个节点同时监听和服务:一个本地端口接受入站连接,同时主动向
 * 已知节点发起连接。消息不过第三方。
 *
 * ── 成员发现 ──
 *   1. 每个节点配一组静态种子(seeds),可以只写一个。
 *   2. 每条边建立后**双向交换 hello**:名字、标签、主机名、监听端口。
 *   3. hello 里带完整的成员表(含地址)。收到后按表补齐自己的视图。
 *   4. 学到新成员时,**主动把新 hello 广播给所有现存边** —— 这一步
 *      不能省,否则会出现不对称视图:hub 知道 b 和 c,而 b 不知道 c。
 *   5. 需要给某人发消息而还没有出站边时,按已知端点建一条。
 *
 * ── 出站边与入站边分开 ──
 *   peers   只存放**出站**边(我们连出去的),发送走这里。
 *   inbound 存放别人连过来的边,只用于接收和识别身份。
 *
 *   分开的原因:一对节点可能双向建边。如果混在一个表里,发送时要
 *   判断用哪条,还会重复。分开后发送路径永远只有出站边。
 *   结果:每对节点最终一条出站边,总量 n(n-1)/2。
 *
 * ── 与 broker 的取舍 ──
 *   收益:没有单点;没有第三方进程看到明文;broker 停机不影响。
 *   代价:每个节点开一个端口;n 个节点 n(n-1)/2 条连接(25 个 = 300 条);
 *        收件人解析用自己可能略旧的成员视图。
 */

import { createServer as createHttpServer, request as httpRequest } from "node:http";
import { createHash } from "node:crypto";
import { FrameReader, encodeFrame, closeFrame, pingFrame, pongFrame } from "./ws.js";
import { createEmitter } from "./transport.js";

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const RECONNECT_BASE_MS = 800;
const RECONNECT_MAX_MS = 15_000;
const HELLO_TIMEOUT_MS = 5_000;
const HEARTBEAT_MS = 15_000;

/**
 * 解析种子。"host:port"、"ws://host:port"、"host"、"[::1]:9000" 都接受。
 */
export function parseSeed(seed, defaultPort = 8788) {
  let s = String(seed ?? "").trim();
  if (!s) return null;
  s = s.replace(/^wss?:\/\//, "").replace(/^https?:\/\//, "").split("/")[0].trim();
  if (!s) return null;

  const v6 = s.match(/^\[(.+)\](?::(\d+))?$/);
  if (v6) return { addr: v6[1], port: v6[2] ? Number(v6[2]) : defaultPort };

  const idx = s.lastIndexOf(":");
  if (idx < 0) return { addr: s, port: defaultPort };
  const port = Number(s.slice(idx + 1));
  if (!Number.isFinite(port) || port <= 0) return { addr: s, port: defaultPort };
  return { addr: s.slice(0, idx), port };
}

/**
 * 在本地成员视图上解析收件人。语法与 session.js 一致
 * (@label / #label / * / @default / 名字 / 数组)。
 */
export function resolveTargets(to, selfName, memberList) {
  const requested = Array.isArray(to) ? to : [to];
  const targets = new Set();
  const byName = new Set(memberList.map((m) => m.name));

  for (const raw of requested) {
    if (typeof raw !== "string") continue;
    const t = raw.trim();
    if (!t) continue;

    if (t === "*" || t === "@default") {
      for (const m of memberList) targets.add(m.name);
      continue;
    }
    if (t.startsWith("@") || t.startsWith("#")) {
      const label = t.slice(1);
      for (const m of memberList) if ((m.labels ?? []).includes(label)) targets.add(m.name);
      continue;
    }
    if (byName.has(t)) targets.add(t);
  }

  targets.delete(selfName);
  return [...targets];
}

export function createMeshTransport({
  token,
  seeds = [],
  listenHost = "0.0.0.0",
  listenPort = 0,
  /**
   * 保留参数但不使用,只是为了让调用方不必按模式分支传参。
   *
   * 地址是**从入站连接的来源地址学来的**,不是对端自述的。这样对端
   * 无法让我们去连一个它编出来的地址,也就不存在"通告错了连不上"
   * 这类问题;代价是两端都在 NAT 后面时需要别的办法(见 docs)。
   */
  advertiseHost = null,
  heartbeatMs = HEARTBEAT_MS,
  helloTimeoutMs = HELLO_TIMEOUT_MS,
  /**
   * 由外部提供成员来源时置 true。
   *
   * SWIM 模式用它:成员表由 Go 边车(memberlist)负责,mesh 只提供
   * 连接与投递。此时 mesh 自己**不**做成员发现也不扩散 hello,
   * 成员视图完全由外部注入 —— 否则两套发现机制会互相干扰,
   * 表现为成员表里出现边车已经判死的节点。
   */
  externalMembership = false,
} = {}) {
  const bus = createEmitter();

  let self = null;
  let server = null;
  let state = "offline";
  let boundPort = null;
  let stopped = false;

  /** 出站边:name → link(provisional key → link,直到 hello 确定名字) */
  const peers = new Map();
  /** 入站边:name → { socket } */
  const inbound = new Map();
  /** 成员元数据:name → { host, labels, since } */
  const known = new Map();
  /** 端点:name → { addr, port } */
  const endpoints = new Map();

  const setState = (next, detail) => {
    if (state === next) return;
    state = next;
    bus.emit("state", next, detail);
  };

  function members() {
    const out = [];
    for (const [name, meta] of known) {
      if (name === self?.name) continue;
      const ep = endpoints.get(name);
      out.push({
        name,
        host: meta.host ?? null,
        addr: ep?.addr ?? null,
        labels: meta.labels ?? [],
        since: meta.since ?? 0,
        endpoints: ep?.port ? [`${ep.addr}:${ep.port}`] : [],
      });
    }
    return out;
  }

  /** 外部注入的成员(仅 externalMembership 模式使用) */
  let injectedMembers = [];

  const emitMembership = () =>
    bus.emit("membership", externalMembership ? injectedMembers : members());

  /** hello 载荷:自己 + 完整成员表(含地址,便于对端回连) */
  function helloBody() {
    return {
      kind: "mesh-hello",
      name: self?.name ?? null,
      host: self?.host ?? null,
      labels: self?.labels ?? [],
      listen: boundPort,
      members: members().map((m) => ({
        name: m.name,
        host: m.host,
        labels: m.labels,
        addr: m.addr,
        port: m.endpoints?.[0] ? Number(String(m.endpoints[0]).split(":").pop()) : null,
      })),
    };
  }

  function registerMember(name, { host = null, labels = [], addr = null, port = null } = {}) {
    if (!name || name === self?.name) return false;

    const had = known.has(name);
    const prev = known.get(name);
    known.set(name, {
      name,
      host: host || prev?.host || null,
      labels: labels?.length ? labels : (prev?.labels ?? []),
      since: prev?.since ?? Date.now(),
    });

    // 端口为 0/null 时不能当成有效端点,否则会把好地址覆盖成坏的
    if (addr && port) {
      const cur = endpoints.get(name);
      if (!cur || cur.addr !== addr || cur.port !== port) endpoints.set(name, { addr, port });
    }

    return !had;
  }

  function applyHelloMembers(list) {
    if (!Array.isArray(list)) return false;
    let changed = false;
    for (const m of list) {
      if (registerMember(m?.name, { host: m?.host, labels: m?.labels, addr: m?.addr, port: m?.port })) changed = true;
    }
    return changed;
  }

  /**
   * 把当前 hello 广播给所有现存边。
   *
   * 成员表变化时必须调它。hello 只在边建立时交换一次,那时如果还没有
   * 新成员,对端就永远学不到它 —— 表现为不对称成员表。
   */
  function broadcastHello() {
    const payload = JSON.stringify(helloBody());
    let sent = 0;
    for (const link of peers.values()) {
      if (link.online && link.send(payload)) sent++;
    }
    for (const { socket } of inbound.values()) {
      try {
        socket.write(encodeFrame(payload));
        sent++;
      } catch {}
    }
    return sent;
  }

  function noteChange(mutation) {
    if (mutation) {
      emitMembership();
    }
  }

  /** 确保有一条通往 name 的出站边 */
  function ensureLink(name) {
    if (name === self?.name) return null;
    const existing = peers.get(name);
    if (existing && existing.online !== undefined) return existing;

    const ep = endpoints.get(name);
    if (!ep?.addr || !ep?.port) return null;

    const link = createLink({ addr: ep.addr, port: ep.port, provisionalName: name });
    peers.set(name, link);
    link.start();
    return link;
  }

  /** 一条对等边的公共处理:hello 解析 + 业务信封转发 */
  function makeTextHandler({ getLinkName, setLinkName, onHello, remoteAddr }) {
    return (text) => {
      let env;
      try {
        env = JSON.parse(text);
      } catch {
        return;
      }

      if (env?.kind === "mesh-hello") {
        setLinkName(env.name);

        const learnedNew = registerMember(env.name, {
          host: env.host,
          labels: env.labels,
          addr: remoteAddr,
          port: env.listen ?? null,
        });
        const learnedFromList = applyHelloMembers(env.members);
        noteChange(learnedNew || learnedFromList);

        onHello?.(env);
        return;
      }

      bus.emit("envelope", env);
    };
  }

  /** 建立一条出站边 */
  function createLink({ addr, port, provisionalName }) {
    let socket = null;
    let linkState = "connecting";
    let delay = RECONNECT_BASE_MS;
    let timer = null;
    let linkStopped = false;
    let resolvedName = provisionalName;
    /** 边还没 online 时积压的消息,上线后按序发出 */
    const backlog = [];

    const link = {
      get name() {
        return resolvedName;
      },
      set name(n) {
        resolvedName = n;
      },
      get address() {
        return addr;
      },
      get remotePort() {
        return port;
      },
      get state() {
        return linkState;
      },
      get online() {
        return linkState === "online";
      },
      get backlogSize() {
        return backlog.length;
      },
      start: connect,
      stop() {
        linkStopped = true;
        if (timer) clearTimeout(timer);
        timer = null;
        try {
          socket?.destroy();
        } catch {}
        socket = null;
        linkState = "offline";
        backlog.length = 0;
      },
      send(text) {
        if (!socket || socket.destroyed) return false;
        try {
          socket.write(encodeFrame(text));
          return true;
        } catch {
          return false;
        }
      },
      /** 排入待发队列;已有连接就直接发 */
      queue(text) {
        if (link.send(text)) return true;
        // 队列入有上限,防止对端永远连不上时内存无限增长
        if (backlog.length < 200) backlog.push(text);
        return false;
      },
    };

    function flushBacklog() {
      if (!backlog.length) return;
      const pending = backlog.splice(0, backlog.length);
      for (const text of pending) {
        if (!link.send(text)) {
          // 又断了,剩下的放回去等下次
          backlog.unshift(...pending.slice(pending.indexOf(text)));
          return;
        }
      }
    }

    function scheduleReconnect() {
      if (linkStopped || timer) return;
      timer = setTimeout(() => {
        timer = null;
        connect();
      }, delay);
      delay = Math.min(delay * 2, RECONNECT_MAX_MS);
    }

    function connect() {
      if (linkStopped) return;
      if (socket && !socket.destroyed) return;

      linkState = "connecting";

      const req = httpRequest({
        host: addr,
        port,
        path: "/",
        headers: {
          Connection: "Upgrade",
          Upgrade: "websocket",
          "Sec-WebSocket-Version": "13",
          "Sec-WebSocket-Key": Buffer.from(String(Math.random())).toString("base64").slice(0, 24),
          "x-team-name": self?.name ?? "",
          "x-team-token": token,
        },
        timeout: helloTimeoutMs,
      });

      req.on("upgrade", (_res, sock, head) => {
        socket = sock;
        linkState = "online";
        delay = RECONNECT_BASE_MS;

        // 上线即自我介绍,并把积压的消息发出去
        link.send(JSON.stringify(helloBody()));
        flushBacklog();

        const reader = new FrameReader({
          onText: makeTextHandler({
            getLinkName: () => link.name,
            setLinkName: (n) => {
              // hello 确定了真名:把 key 从临时名换成真名
              if (n && n !== link.name) {
                peers.delete(link.name);
                link.name = n;
                peers.set(n, link);
              }
            },
            remoteAddr: addr,
            onHello: () => {
              // 对端告诉我们它是谁、它认识谁 —— 我们已经处理完了,
              // 再回一个 hello 让它也拿到最新视图
              link.send(JSON.stringify(helloBody()));
              emitMembership();
            },
          }),
          onPing: () => {
            try {
              sock.write(pongFrame());
            } catch {}
          },
          onClose: () => sock.end(closeFrame(1000)),
        });

        if (head?.length) reader.feed(head);
        sock.on("data", (chunk) => {
          try {
            reader.feed(chunk);
          } catch {
            sock.destroy();
          }
        });
        sock.on("close", () => {
          socket = null;
          linkState = "offline";
          emitMembership();
          scheduleReconnect();
        });
        sock.on("error", () => {});

        const hb = setInterval(() => {
          try {
            socket?.write(pingFrame());
          } catch {}
        }, heartbeatMs);
        hb.unref?.();
        sock.on("close", () => clearInterval(hb));
      });

      req.on("timeout", () => req.destroy());
      req.on("error", () => {
        linkState = "offline";
        scheduleReconnect();
      });
      req.end();
    }

    return link;
  }

  function handleUpgrade(req, socket) {
    const reject = (code, reason) => {
      socket.write(`HTTP/1.1 ${code} ${reason}\r\nConnection: close\r\n\r\n`);
      socket.destroy();
    };

    if ((req.headers.upgrade ?? "").toLowerCase() !== "websocket") return reject(400, "Bad Request");
    if (req.headers["x-team-token"] !== token) return reject(401, "Unauthorized");

    const key = req.headers["sec-websocket-key"];
    if (typeof key !== "string") return reject(400, "Bad Request");

    const remoteAddr = socket.remoteAddress?.replace(/^::ffff:/, "") ?? null;

    socket.write(
      "HTTP/1.1 101 Switching Protocols\r\n" +
        "Upgrade: websocket\r\n" +
        "Connection: Upgrade\r\n" +
        `Sec-WebSocket-Accept: ${createHash("sha1").update(key + WS_GUID).digest("base64")}\r\n\r\n`,
    );
    socket.setNoDelay?.(true);

    let realName = null;

    const reader = new FrameReader({
      onText: makeTextHandler({
        getLinkName: () => realName,
        setLinkName: (n) => {
          realName = n;
        },
        remoteAddr,
        onHello: (env) => {
          // 记录入站边
          if (realName) inbound.set(realName, { socket });

          // 回 hello:告诉它我们是谁、认识谁
          try {
            socket.write(encodeFrame(JSON.stringify(helloBody())));
          } catch {}

          // 为它建一条出站边,这样我们也能主动发给它。
          // 入站边的 socket 只用于接收,不拿来做发送通道。
          if (realName) ensureLink(realName);

          emitMembership();
        },
      }),
      onPing: () => {
        try {
          socket.write(pongFrame());
        } catch {}
      },
      onClose: () => socket.end(closeFrame(1000)),
    });

    socket.on("data", (chunk) => {
      try {
        reader.feed(chunk);
      } catch {
        socket.destroy();
      }
    });
    socket.on("error", () => {});
    socket.on("close", () => {
      if (realName && inbound.get(realName)?.socket === socket) inbound.delete(realName);
      emitMembership();
    });

    const hb = setInterval(() => {
      try {
        socket.write(pingFrame());
      } catch {}
    }, heartbeatMs);
    hb.unref?.();
    socket.on("close", () => clearInterval(hb));
  }

  return {
    mode: "mesh",

    start(nextSelf) {
      self = nextSelf;
      stopped = false;
      setState("connecting");

      server = createHttpServer((_req, res) => res.writeHead(404).end("pi-agent-team mesh\n"));
      server.on("upgrade", handleUpgrade);
      server.on("error", (err) => setState("offline", { reason: "listen_failed", message: err.message }));

      server.listen(listenPort, listenHost, () => {
        boundPort = server.address().port;
        setState("online");

        for (const seed of seeds) {
          const p = parseSeed(seed);
          if (!p) continue;
          const key = `seed:${p.addr}:${p.port}`;
          if (peers.has(key)) continue;
          const link = createLink({ addr: p.addr, port: p.port, provisionalName: key });
          peers.set(key, link);
          link.start();
        }
      });
    },

    stop() {
      stopped = true;
      for (const link of peers.values()) {
        try {
          link.stop();
        } catch {}
      }
      for (const { socket } of inbound.values()) {
        try {
          socket.destroy();
        } catch {}
      }
      inbound.clear();
      peers.clear();
      known.clear();
      endpoints.clear();
      try {
        server?.close();
      } catch {}
      server = null;
      boundPort = null;
      setState("offline", { reason: "stopped" });
      bus.emit("membership", []);
    },

    send({ to, id, re = null, body }) {
      if (!self) return false;
      const envelope = { from: self.name, id, re, body };

      const targets = resolveTargets(to, self.name, members());
      if (!targets.length) return false;

      let anyOk = false;
      for (const target of targets) {
        let link = peers.get(target);
        if (!link || !link.online) {
          const created = ensureLink(target);
          if (created) link = created;
        }

        const ok = link?.send(JSON.stringify(envelope));
        if (ok) {
          anyOk = true;
        } else if (link) {
          // 边存在但还没 online:排队,等它上线再发。
          // 丢了会让"刚加入就发消息"神秘失败 —— 这在真实使用里
          // 很常见(用户看到上线就立刻说话)。
          link.queue(JSON.stringify(envelope));
          anyOk = true; // 已接管,不当作失败
        }
      }
      return anyOk;
    },

    state: () => state,
    // 外部提供成员时,members() 返回注入的视图 —— 它才是权威的
    members: () => (externalMembership ? injectedMembers : members()),
    on: bus.on,
    port: () => boundPort,

    /**
     * 外部成员来源注入(仅 externalMembership 模式)。
     *
     * 两条信息都要:成员名单,以及每个成员的投递端点。
     * 端点告诉 mesh 该往哪连;没有端点的成员无法投递。
     *
     * 注意这里会**主动建边**,不等 send 时才建。原因:SWIM 模式下
     * mesh 不做成员发现,所以两边都不会主动连对方 —— 如果只在
     * send 时按需建边,那条边第一次发消息时还在握手,消息就丢了
     * (或要等排队)。提前建边让首次发送就能成功。
     */
    setExternalMembers(list) {
      if (!externalMembership) return;
      injectedMembers = Array.isArray(list) ? list : [];

      const wanted = new Set();

      for (const m of injectedMembers) {
        if (!m?.name || m.name === self?.name) continue;
        wanted.add(m.name);

        const port = m.deliver ?? (m.endpoints?.[0] ? Number(String(m.endpoints[0]).split(":").pop()) : null);
        const addr = m.addr ?? null;
        if (!addr || !port) continue;

        const cur = endpoints.get(m.name);
        if (!cur || cur.addr !== addr || cur.port !== port) {
          endpoints.set(m.name, { addr, port });
        }
      }

      // 已经不在成员表里的边要拆掉 —— 否则 SWIM 判死的节点仍然
      // 占着一条重连中的边,我们会反复尝试连一个死节点。
      for (const [key, link] of peers) {
        const name = link.name;
        if (name.startsWith("seed:")) continue;
        if (!wanted.has(name) && name !== self?.name) {
          try {
            link.stop();
          } catch {}
          peers.delete(key);
          endpoints.delete(name);
        }
      }

      // 为每个成员确保有边(提前握手,首次发送即命中)
      for (const name of wanted) ensureLink(name);

      emitMembership();
    },
  };
}
