/**
 * Transport 接口与两种实现的基础设施。
 *
 * 三种模式(broker / mesh / swim)共用同一个接口。调用方只看到事件
 * 和 send(),看不到 WebSocket、重连、心跳这些东西。
 *
 * ── 接口 ──
 *   start(self)            连接并加入
 *   stop()                 离开并断开
 *   send(envelope)         投递,返回是否进入了发送路径
 *   state()                "offline" | "connecting" | "online" | "replaced"
 *   members()              当前成员视图 [{name, host, labels, endpoints}]
 *   on("envelope", fn)     收到应用消息或控制消息
 *   on("state", fn)        连接状态变化
 *   on("membership", fn)   成员视图变化
 *
 * ── 每种 transport 必须满足的保证 ──
 * 见 e2e/conformance.test.js。新 transport 必须整套通过才能算完成。
 *
 * ── 为什么决策不在这里 ──
 * 这个文件只管"把字节送到对端"和"报告状态"。收到消息之后该怎么办
 * (注入?回信?只显示卡片?)属于会话状态机,在 session.js。
 */

/** broker 用这个关闭码表示"你被同名的新连接顶掉了"。 */
export const CLOSE_REPLACED = 4001;

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30_000;

/**
 * 把 host/labels 参数编码进查询串。broker 和 mesh 都用它。
 * 字段名用 tags 是历史原因(broker 先写好了),这里集中收口。
 */
function identityQuery(self) {
  const qs = new URLSearchParams();
  qs.set("name", self.name);
  if (self.labels?.length) qs.set("tags", self.labels.join(","));
  if (self.host) qs.set("host", self.host);
  return qs;
}

/**
 * 一个极简事件发射器,带订阅与退订。
 * 单个订阅者抛异常不能影响其它订阅者,也不能断掉连接。
 */
function createEmitter() {
  const handlers = new Map();

  return {
    on(event, handler) {
      if (!handlers.has(event)) handlers.set(event, new Set());
      handlers.get(event).add(handler);
      return () => handlers.get(event)?.delete(handler);
    },
    emit(event, ...args) {
      for (const h of handlers.get(event) ?? []) {
        try {
          h(...args);
        } catch {
          // 订阅者的错误不该影响 transport
        }
      }
    },
  };
}

// ================================================================ Broker

/**
 * Broker transport:所有节点主动 dial out 到一个中心 broker。
 *
 * 单点,但它挂了只有跨机消息不通,本地工作继续 —— 这个失败模式
 * 是可接受的,也是默认选它的原因。
 */
export function createBrokerTransport({ url, token, WebSocketImpl = WebSocket }) {
  const bus = createEmitter();
  /** @type {WebSocket|null} */
  let socket = null;
  let state = "offline";
  let self = null;
  let members = [];
  let reconnectDelay = RECONNECT_BASE_MS;
  let reconnectTimer = null;
  let stopped = false;

  const setState = (next, detail) => {
    if (state === next) return;
    state = next;
    bus.emit("state", next, detail);
  };

  const setMembers = (body) => {
    // 兼容两种字段名:broker 的 tags 和内部的 labels
    const list = Array.isArray(body?.members)
      ? body.members
      : Array.isArray(body?.peers)
        ? body.peers.map((name) => ({ name, host: null, labels: [] }))
        : null;
    if (!list) return;

    members = list
      .filter((m) => m?.name && m.name !== self?.name)
      .map((m) => ({
        name: m.name,
        host: m.host ?? null,
        addr: m.addr ?? null,
        labels: m.labels ?? m.tags ?? [],
        since: m.since ?? 0,
        endpoints: [], // broker 模式不需要端点,消息都经 broker
      }));
    bus.emit("membership", members);
  };

  function scheduleReconnect() {
    if (stopped || reconnectTimer) return;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      open();
    }, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_MS);
  }

  function open() {
    if (stopped || !self) return;
    if (socket && (socket.readyState === WebSocketImpl.OPEN || socket.readyState === WebSocketImpl.CONNECTING)) {
      return;
    }

    setState("connecting");

    const qs = identityQuery(self);
    qs.set("token", token);

    let ws;
    try {
      ws = new WebSocketImpl(`${url}?${qs}`);
    } catch (err) {
      setState("offline", { reason: "construct_failed", message: String(err?.message ?? err) });
      scheduleReconnect();
      return;
    }
    socket = ws;

    ws.addEventListener("open", () => {
      reconnectDelay = RECONNECT_BASE_MS;
      setState("online");
    });

    ws.addEventListener("message", (ev) => {
      let env;
      try {
        env = JSON.parse(String(ev.data));
      } catch {
        return;
      }
      // 成员视图由控制消息维护
      if (env.from === "broker") setMembers(env.body);
      bus.emit("envelope", env);
    });

    ws.addEventListener("close", (ev) => {
      socket = null;
      members = [];
      bus.emit("membership", members);

      if (ev.code === CLOSE_REPLACED) {
        // 另一个同名节点接管了。继续重连只会互相顶来顶去。
        stopped = true;
        setState("replaced", { name: self?.name });
        return;
      }

      setState("offline", { code: ev.code });
      scheduleReconnect();
    });

    // error 之后必然跟 close,重连统一放在 close 里处理
    ws.addEventListener("error", () => {});
  }

  return {
    mode: "broker",

    start(nextSelf) {
      self = nextSelf;
      stopped = false;
      open();
    },

    stop() {
      stopped = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      reconnectTimer = null;
      try {
        socket?.close();
      } catch {}
      socket = null;
      members = [];
      setState("offline", { reason: "stopped" });
    },

    /**
     * 投递。broker 模式返回 true 只表示"写进了 socket",
     * 真正的回执(delivered / undeliverable)随后由控制消息到达。
     */
    send({ to, id, re = null, body }) {
      if (!socket || socket.readyState !== WebSocketImpl.OPEN) return false;
      try {
        socket.send(JSON.stringify({ from: self?.name ?? "", to, id, re, body }));
        return true;
      } catch {
        return false;
      }
    },

    state: () => state,
    members: () => members,
    on: bus.on,
  };
}

// ================================================================ 导出

export { createEmitter, identityQuery };
