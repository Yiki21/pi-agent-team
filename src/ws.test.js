/**
 * 帧编解码单测。跑:node --test src/
 *
 * 重点测"被 TCP 切开"和"分片"这两种情况 —— 真机上第一次出问题
 * 就是这两种形态,而不是教科书上的完整帧。
 */
import test from "node:test";
import assert from "node:assert/strict";
import { FrameReader, encodeFrame, encodeMaskedFrame, closeFrame, pingFrame, OPCODES } from "./ws.js";

/** 把一段 Buffer 逐字节喂给 reader —— 模拟最恶劣的 TCP 切分 */
function feedByteByByte(reader, buf) {
  for (const byte of buf) reader.feed(Buffer.from([byte]));
}

function collect() {
  const got = { text: [], closed: 0, ping: 0 };
  const reader = new FrameReader({
    onText: (s) => got.text.push(s),
    onClose: () => got.closed++,
    onPing: () => got.ping++,
  });
  return { reader, got };
}

test("短文本:一次喂完整帧", () => {
  const { reader, got } = collect();
  reader.feed(encodeFrame("hello"));
  assert.deepEqual(got.text, ["hello"]);
});

test("短文本:逐字节喂(最恶劣切分)", () => {
  const { reader, got } = collect();
  feedByteByByte(reader, encodeFrame("hello"));
  assert.deepEqual(got.text, ["hello"]);
});

test("同一次 feed 里塞两帧", () => {
  const { reader, got } = collect();
  reader.feed(Buffer.concat([encodeFrame("one"), encodeFrame("two")]));
  assert.deepEqual(got.text, ["one", "two"]);
});

test("16 位长度边界:125 / 126 / 65535 / 65536 都不能错", () => {
  for (const n of [0, 1, 124, 125, 126, 127, 4096, 65535, 65536]) {
    const { reader, got } = collect();
    const msg = "a".repeat(n);
    feedByteByByte(reader, encodeFrame(msg)); // 边界长度 + 逐字节双重压力
    assert.deepEqual(got.text, [msg], `len=${n} 失败`);
  }
});

test("超过 MAX_PAYLOAD 的载荷被拒绝", () => {
  assert.throws(() => encodeFrame("x".repeat(64 * 1024 + 1)), /payload too large/);
});

test("掩码帧能正确解码(客户端→服务端方向)", () => {
  const { reader, got } = collect();
  const masked = encodeMaskedFrame("masked message");
  assert.equal(masked[1] & 0x80, 0x80, "客户端帧必须置掩码位");
  feedByteByByte(reader, masked);
  assert.deepEqual(got.text, ["masked message"]);
});

test("分片文本消息:累积到最后一片才交付", () => {
  // 手工构造:第一片 fin=0 带文本,第二片 fin=1 续帧
  const p1 = Buffer.from("Hel");
  const p2 = Buffer.from("lo!");
  const f1 = Buffer.concat([Buffer.from([0x01]), Buffer.from([p1.length]), p1]); // fin=0, TEXT
  const f2 = Buffer.concat([Buffer.from([0x80 | OPCODES.OP_CONT]), Buffer.from([p2.length]), p2]); // fin=1, CONT

  const { reader, got } = collect();
  reader.feed(Buffer.concat([f1, f2]));
  assert.deepEqual(got.text, ["Hello!"]);
});

test("分片且被逐字节喂", () => {
  const p1 = Buffer.from("frag");
  const p2 = Buffer.from("mented");
  const f1 = Buffer.concat([Buffer.from([0x01, p1.length]), p1]);
  const f2 = Buffer.concat([Buffer.from([0x80 | OPCODES.OP_CONT, p2.length]), p2]);

  const { reader, got } = collect();
  feedByteByByte(reader, Buffer.concat([f1, f2]));
  assert.deepEqual(got.text, ["fragmented"]);
});

test("ping 被识别,close 被识别", () => {
  const { reader, got } = collect();
  reader.feed(pingFrame());
  assert.equal(got.ping, 1);

  reader.feed(closeFrame(1000));
  assert.equal(got.closed, 1);
});

test("控制帧不允许分片", () => {
  const { reader } = collect();
  // fin=0 的 ping —— 非法
  const bad = Buffer.from([0x09, 0x00]);
  assert.throws(() => reader.feed(bad), /fragmented control frame/);
});

test("控制帧载荷不得超过 125", () => {
  const { reader } = collect();
  const bad = Buffer.concat([Buffer.from([0x89, 126]), Buffer.from([0x00, 0x80])]);
  assert.throws(() => reader.feed(bad), /oversized control frame/);
});

test("没有起始帧的续帧被安全丢弃,不抛异常", () => {
  const { reader, got } = collect();
  const orphan = Buffer.from([0x80 | OPCODES.OP_CONT, 0x03, 0x61, 0x62, 0x63]);
  reader.feed(orphan);
  assert.deepEqual(got.text, []);
});

test("声称超大长度时立刻拒绝,不等待数据", () => {
  const { reader } = collect();
  const hdr = Buffer.alloc(10);
  hdr[0] = 0x81;
  hdr[1] = 127;
  hdr.writeBigUInt64BE(BigInt(1024 * 1024 * 1024), 2);
  assert.throws(() => reader.feed(hdr), /exceeds cap/);
});

test("UTF-8 多字节字符跨帧边界不损坏", () => {
  const { reader, got } = collect();
  const msg = "中文测试 emoji 🚀 ok";
  // 逐字节喂,让多字节字符的字节被切断
  feedByteByByte(reader, encodeFrame(msg));
  assert.deepEqual(got.text, [msg]);
});
