/**
 * Transport 接口与 Broker 实现。
 *
 * 三种模式(broker / mesh / swim)共用同一个接口。调用方只看到事件
 * 和 send(),看不到 WebSocket、重连、心跳这些东西。
 *
 * 接口设计与 roadmap 8.2/8.3 一致。每个 transport 都要满足的保证见
 * roadmap 8.3,测试在 e2e/ 里。
 *
 * ── 为什么决策不在这里 ──
 * 这个文件只管"把字节送到对端"和"报告连接状态"。收到消息之后
 * 该怎么办(注入?回信?只显示卡片?)属于会话状态机,在 session.js。
 * 混在一起会让两者都无法单独测试。
 */

/** broker 用这个关闭码表示"你被同名的新连接顶掉了"。 */
export const CLOSE_REPLACED = 4001;

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30_000;

/**
 * @typedef {{
 *   // 生命周期
 *   start(self: { name: string, labels?: string[], host?: string|null }): void,
 *   stop(): void,
 *
 *   // 发送。to 支持 :name / @label / * / @default / 数组
 *   send(envelope: { to: string|string[], id: string, re: string|null,
 *                    body: Record<string, unknown> }): boolean,
 *
 *   // 只读状态
 *   state(): "offline"|"connecting"|"online"|"replaced",
 *   members(): Array<{ name: string, host: string|null, addr: string|null,
 *                      labels: string[], since: number }>,
 *
 *   // 事件订阅,返回取消订阅函数
 *   on(event: "envelope", handler: (env: object) => void): () => void,
 *   on(event: "state", handler: (state: string, detail?: object) => void): () => void,
 * }} Transport
 */

/**
 * Broker transport:所有节点主动 dial out 到一个中心 broker。
 *
 * 单点,但它挂了只有跨机消息不通,本地工作继续 —— 这个失败模式
 * 是可接受的,也是默认选它的原因。
 *
 * @returns {Transport}
 */
export function createBrokerTransport({ url, token, WebSocketImpl = WebSocket }) {
  /** @type {WebSocket|null} */
  let socket = null;
  /** @type {"offline"|"connecting"|"online"|"replaced"} */
  let state = "offline";
  let self = null;
  let reconnectDelay = RECONNECT_BASE_MS;
  let reconnectTimer = null;
  let stopped = false;

  const envelopeHandlers = new Set();
  const stateHandlers = new Set();

  const emit = (event, ...args) => {
    for (const h of event === "envelope" ? envelopeHandlers : stateHandlers) {
      try {
        h(...args);
      } catch {
        // 单个订阅者出错不该影响其它订阅者,也不该断掉连接
      }
    }
  };

  const setState = (next, detail) => {
    if (state === next) return;
    state = next;
    emit("state", next, detail);
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

    const qs = new URLSearchParams({ name: self.name, token });
    if (self.labels?.length) qs.set("tags", self.labels.join(",")); // broker 侧字段名仍是 tags
    if (self.host) qs.set("host", self.host);

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
        return; // 协议外的垃圾,忽略
      }
      emit("envelope", env);
    });

    ws.addEventListener("close", (ev) => {
      socket = null;

      if (ev.code === CLOSE_REPLACED) {
        // 另一个同名节点接管了。继续重连只会和它互相顶来顶去,
        // 两边都连不上。停下来并让调用方告诉用户。
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
      setState("offline", { reason: "stopped" });
    },

    send({ to, id, re = null, body }) {
      if (!socket || socket.readyState !== WebSocketImpl.OPEN) return false;
      try {
        socket.send(JSON.stringify({ from: self?.name ?? "", to, id, re, body }));
        return true;
      } catch {
        return false;
      }
    },

    /** 当前连接状态。成员视图由会话状态机从 welcome/peer_* 事件维护。 */
    state: () => state,

    on(event, handler) {
      const set = event === "envelope" ? envelopeHandlers : stateHandlers;
      set.add(handler);
      return () => set.delete(handler);
    },
  };
}
