/**
 * 帧编解码单测。跑:node --test src/
 *
 * 重点测"被 TCP 切开"和"分片"这两种情况 —— 真机上第一次出问题
 * 就是这两种形态,而不是教科书上的完整帧。
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  FrameReader,
  PayloadTooLargeError,
  encodeFrame,
  encodeMaskedFrame,
  closeFrame,
  pingFrame,
  tokenEquals,
  OPCODES,
} from "./ws.js";

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

test("超过 MAX_PAYLOAD 的载荷被拒绝,错误可被识别", () => {
  // 断言错误的 code 而不是措辞:调用方(broker、mesh)靠 code 决定发 1009
  // 而不是笼统断开。措辞改了不该让这个契约静默失效。
  assert.throws(
    () => encodeFrame("x".repeat(64 * 1024 + 1)),
    (err) => err instanceof PayloadTooLargeError && err.code === "PAYLOAD_TOO_LARGE" && err.size === 64 * 1024 + 1,
  );
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
  assert.throws(
    () => reader.feed(hdr),
    (err) => err?.code === "PAYLOAD_TOO_LARGE" && err.size === 1024 * 1024 * 1024,
    "声称的长度超限时要立刻抛出可识别的错误,不等数据到齐",
  );
});

test("UTF-8 多字节字符跨帧边界不损坏", () => {
  const { reader, got } = collect();
  const msg = "中文测试 emoji 🚀 ok";
  // 逐字节喂,让多字节字符的字节被切断
  feedByteByByte(reader, encodeFrame(msg));
  assert.deepEqual(got.text, [msg]);
});

// ---------------------------------------------------------------- token 比较
//
// broker 和 mesh 曾经用不同的比较方式:一个 timingSafeEqual,一个普通 `!==`。
// 同一个 token 在两种 transport 下强度不同,取决于用户选了哪个模式。
// 现在两边共用 tokenEquals。

test("tokenEquals:相等才算通过", () => {
  assert.equal(tokenEquals("same-token", "same-token"), true);
  assert.equal(tokenEquals("same-token", "different"), false);
  assert.equal(tokenEquals("", ""), true, "两个空值相等");
  assert.equal(tokenEquals("x", ""), false);
  assert.equal(tokenEquals(null, "x"), false, "null 不炸,判为不等");
  assert.equal(tokenEquals(undefined, undefined), true, "都缺失时相等(由调用方决定是否允许)");
});

test("tokenEquals:长度不同不抛异常", () => {
  // timingSafeEqual 在长度不同时会抛。必须先哈希再比 —— 直接比长度再
  // 提前返回,那个提前返回本身就泄露长度。
  for (const [a, b] of [["a", "aaaaaaaaaa"], ["", "x"], ["long".repeat(100), "long"]]) {
    assert.equal(tokenEquals(a, b), false, `${a.length} vs ${b.length} 不该抛`);
  }
});

test("tokenEquals:多字节和超长输入都稳", () => {
  const cjk = "中文令牌".repeat(50);
  assert.equal(tokenEquals(cjk, cjk), true);
  assert.equal(tokenEquals(cjk, `${cjk}x`), false);
  const huge = "z".repeat(100_000);
  assert.doesNotThrow(() => tokenEquals(huge, huge), "超长输入不该抛");
  assert.equal(tokenEquals(huge, huge), true);
});

test("tokenEquals:前缀相同的 token 判为不等", () => {
  // 普通的 == 在这里会逐个字节比、遇到不同就返回,时间随匹配长度变化
  const base = "a".repeat(64);
  assert.equal(tokenEquals(base, `${"a".repeat(63)}b`), false, "只差最后一个字符也要判不等");
  assert.equal(tokenEquals(base, `${"b"}${"a".repeat(63)}`), false, "只差第一个字符也要判不等");
});


test("closeFrame 可以带原因,且码仍在前两个字节", () => {
  const f = closeFrame(1009, "message too large");
  // 结构:[0x88][len][code hi][code lo][reason...]
  assert.equal(f[0], 0x88);
  assert.equal(f.readUInt16BE(2), 1009);
  assert.equal(f.subarray(4).toString("utf8"), "message too large");
});

test("closeFrame 原因过长会被截断,不超过控制帧 125 字节上限", () => {
  assert.doesNotThrow(() => closeFrame(1009, "x".repeat(500)));
  const f = closeFrame(1009, "x".repeat(500));
  assert.ok(f[1] <= 125, `控制帧载荷必须 ≤ 125,实际 ${f[1]}`);
});

test("closeFrame 不带原因时和旧行为一致", () => {
  const f = closeFrame(1000);
  assert.equal(f[1], 2, "只有 2 字节的码");
  assert.equal(f.readUInt16BE(2), 1000);
});
