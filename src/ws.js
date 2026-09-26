/**
 * 最小 RFC 6455 服务端/客户端帧编解码。
 *
 * 只做我们需要的:文本帧、分片、掩码、ping/pong/close。
 * 不做扩展协商、不做 permessage-deflate —— 我们的消息都很小,
 * 压不压缩不值得那层复杂度和依赖。
 *
 * 服务端发帧不加掩码(RFC 强制),收帧必须解掩码(客户端强制)。
 */

const OP_CONT = 0x0;
const OP_TEXT = 0x1;
const OP_BIN = 0x2;
const OP_CLOSE = 0x8;
const OP_PING = 0x9;
const OP_PONG = 0xa;

/** 上限,防止对端用超大长度头耗内存。64 KiB 足够任何一条消息。 */
export const MAX_PAYLOAD = 64 * 1024;

/**
 * 把一段文本编码成一个未掩码的 WebSocket 帧。
 * @param {string} text
 * @returns {Buffer}
 */
export function encodeFrame(text) {
  const data = Buffer.from(text, "utf8");
  if (data.length > MAX_PAYLOAD) {
    throw new Error(`payload too large: ${data.length} > ${MAX_PAYLOAD}`);
  }

  const len = data.length;
  let header;

  if (len < 126) {
    header = Buffer.from([0x80 | OP_TEXT, len]);
  } else if (len < 65536) {
    // 126..65535 —— 16 位长度
    header = Buffer.alloc(4);
    header[0] = 0x80 | OP_TEXT;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    // ≥65536 —— 64 位长度。我们的 MAX_PAYLOAD 是 64 KiB,这条分支
    // 在 len === 65536 时正好命中,少了它就是上次那个 RangeError。
    header = Buffer.alloc(10);
    header[0] = 0x80 | OP_TEXT;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }

  return Buffer.concat([header, data]);
}

/** 控制帧(close/ping/pong),载荷 ≤ 125 字节,永不使用扩展长度。 */
export function encodeControl(opcode, payload = Buffer.alloc(0)) {
  if (payload.length > 125) throw new Error("control frame payload > 125");
  return Buffer.concat([Buffer.from([0x80 | opcode, payload.length]), payload]);
}

export const closeFrame = (code) => {
  const p = Buffer.alloc(2);
  p.writeUInt16BE(code ?? 1000, 0);
  return encodeControl(OP_CLOSE, p);
};
export const pingFrame = () => encodeControl(OP_PING);
export const pongFrame = () => encodeControl(OP_PONG);

/**
 * 给一个 socket 挂上帧读取器。
 *
 * 返回一个 { feed, onMessage, onClose } 对象 —— 但更常用的用法是
 * 直接构造它并订阅:
 *
 *   const reader = new FrameReader({
 *     onText: (s) => ...,      // 完整文本消息
 *     onClose: () => ...,      // 收到 close 帧
 *     onPing: () => socket.write(pongFrame()),
 *   });
 *   socket.on("data", (c) => reader.feed(c));
 *
 * 分片消息会被累积到最后一片才交付。这样可以正确处理
 * 被 TCP 切开的帧,以及合法的分片发送方。
 */
export class FrameReader {
  #buf = Buffer.alloc(0);
  #fragments = null;
  #handlers;

  constructor({ onText, onClose, onPing, onPong } = {}) {
    this.#handlers = { onText, onClose, onPing, onPong };
  }

  feed(chunk) {
    this.#buf = this.#buf.length === 0 ? chunk : Buffer.concat([this.#buf, chunk]);

    // 循环到缓冲区不足以构成一个完整帧为止
    while (this.#tryReadOne()) {
      /* 继续 */
    }
  }

  /** @returns {boolean} 是否消费掉了一个完整帧 */
  #tryReadOne() {
    const buf = this.#buf;
    if (buf.length < 2) return false;

    const b0 = buf[0];
    const b1 = buf[1];

    const fin = (b0 & 0x80) !== 0;
    const opcode = b0 & 0x0f;
    const masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f;
    let off = 2;

    if (len === 126) {
      if (buf.length < 4) return false;
      len = buf.readUInt16BE(2);
      off = 4;
    } else if (len === 127) {
      if (buf.length < 10) return false;
      const big = buf.readBigUInt64BE(2);
      if (big > BigInt(MAX_PAYLOAD)) {
        throw new Error(`declared payload ${big} exceeds cap`);
      }
      len = Number(big);
      off = 10;
    }

    // 控制帧不能分片,且长度 ≤ 125
    if (opcode >= 0x8) {
      if (!fin) throw new Error("fragmented control frame");
      if (len > 125) throw new Error("oversized control frame");
    }

    let maskKey = null;
    if (masked) {
      if (buf.length < off + 4) return false;
      maskKey = buf.subarray(off, off + 4);
      off += 4;
    }

    if (buf.length < off + len) return false;

    const payload = Buffer.from(buf.subarray(off, off + len));
    if (maskKey) {
      for (let i = 0; i < payload.length; i++) payload[i] ^= maskKey[i & 3];
    }
    this.#buf = buf.subarray(off + len);

    switch (opcode) {
      case OP_CLOSE:
        this.#handlers.onClose?.();
        return false;

      case OP_PING:
        this.#handlers.onPing?.(payload);
        return true;

      case OP_PONG:
        this.#handlers.onPong?.(payload);
        return true;

      case OP_TEXT:
      case OP_BIN:
        if (fin) {
          this.#handlers.onText?.(payload.toString("utf8"));
        } else {
          this.#fragments = [payload];
        }
        return true;

      case OP_CONT:
        if (!this.#fragments) {
          // 没有起始帧的续帧 —— 协议错误,直接丢
          return true;
        }
        this.#fragments.push(payload);
        if (fin) {
          const whole = Buffer.concat(this.#fragments);
          this.#fragments = null;
          if (whole.length > MAX_PAYLOAD) {
            throw new Error("reassembled payload exceeds cap");
          }
          this.#handlers.onText?.(whole.toString("utf8"));
        }
        return true;

      default:
        // 未知 opcode —— 忽略
        return true;
    }
  }
}

/**
 * 客户端侧:构造一个带掩码的文本帧。
 * 浏览器/Node 原生 WebSocket 会自己处理这些,所以这个函数
 * 只在我们要自己实现客户端时用。当前 broker 是服务端,
 * 扩展用 Node 原生 WebSocket —— 所以两边都不需要它。
 *
 * 留着是因为端到端测试里要用原始 socket 当客户端。
 */
export function encodeMaskedFrame(text) {
  const data = Buffer.from(text, "utf8");
  const mask = Buffer.from([
    (Math.random() * 256) | 0,
    (Math.random() * 256) | 0,
    (Math.random() * 256) | 0,
    (Math.random() * 256) | 0,
  ]);

  const len = data.length;
  let header;
  if (len < 126) {
    header = Buffer.from([0x80 | OP_TEXT, 0x80 | len]);
  } else {
    header = Buffer.alloc(4);
    header[0] = 0x80 | OP_TEXT;
    header[1] = 0x80 | 126;
    header.writeUInt16BE(len, 2);
  }

  const masked = Buffer.from(data);
  for (let i = 0; i < masked.length; i++) masked[i] ^= mask[i & 3];

  return Buffer.concat([header, mask, masked]);
}

export const OPCODES = { OP_CONT, OP_TEXT, OP_BIN, OP_CLOSE, OP_PING, OP_PONG };
