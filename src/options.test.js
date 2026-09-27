/**
 * 连接选项的解析与校验。
 *
 * 同一组选项有三个入口(环境变量 / /team 命令 / team_join 工具),
 * 解析只做一次 —— 所以这里的测试覆盖的是三个入口共同的契约。
 * 少了它,"命令能设、工具设不了"这类分叉只能靠人肉发现。
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  ALL_KEYS,
  checkModeRequirements,
  parseLabels,
  parseOptionArgs,
  parseSeeds,
  validateOptions,
} from "./options.js";

// ---------------------------------------------------------------- 参数解析

test("parseOptionArgs:--key value 和 --key=value 都支持", () => {
  assert.deepEqual(parseOptionArgs(["--mode", "mesh"]).values, { mode: "mesh" });
  assert.deepEqual(parseOptionArgs(["--mode=mesh"]).values, { mode: "mesh" });
});

test("parseOptionArgs:key=value 不带横线也认(方便随手写)", () => {
  assert.deepEqual(parseOptionArgs(["mode=swim"]).values, { mode: "swim" });
});

test("parseOptionArgs:位置参数留在 rest 里", () => {
  const r = parseOptionArgs(["myteam", "--mode", "mesh", "extra"]);
  assert.deepEqual(r.rest, ["myteam", "extra"]);
  assert.deepEqual(r.values, { mode: "mesh" });
});

test("parseOptionArgs:选项跟在位置参数后面也认", () => {
  const r = parseOptionArgs(["myteam", "http://h:1", "--mode", "mesh", "--seeds", "a:1,b:2"]);
  assert.deepEqual(r.rest, ["myteam", "http://h:1"]);
  assert.equal(r.values.mode, "mesh");
  assert.equal(r.values.seeds, "a:1,b:2");
});

test("parseOptionArgs:选项后面直接跟另一个选项时,不当成它的值", () => {
  // --labels 后面是 --mode,不能把 "--mode" 当成标签
  const r = parseOptionArgs(["--labels", "--mode", "mesh"]);
  assert.equal(r.values.labels, "");
  assert.equal(r.values.mode, "mesh");
});

test("parseOptionArgs:认不出的选项被报出来,而不是静默忽略", () => {
  // 静默忽略的后果是用户以为设了,其实没有 —— 这个坑踩过好几次
  const r = parseOptionArgs(["--mod", "mesh"]);
  assert.deepEqual(r.unknown, ["mod"]);
  assert.equal(r.values.mode, undefined);
});

test("parseOptionArgs:选项名大小写不敏感", () => {
  assert.deepEqual(parseOptionArgs(["--MODE", "mesh"]).values, { mode: "mesh" });
});

test("parseOptionArgs:空数组和空字符串不炸", () => {
  assert.deepEqual(parseOptionArgs([]).values, {});
  assert.deepEqual(parseOptionArgs(["", "  "]).values, {});
  assert.deepEqual(parseOptionArgs(undefined).values, {});
});

test("ALL_KEYS 覆盖三个入口文档里承诺的全部选项", () => {
  assert.deepEqual(
    [...ALL_KEYS].sort(),
    ["labels", "listen", "mode", "name", "port", "seeds", "token", "url"],
  );
});

// ---------------------------------------------------------------- seeds / labels

test("parseSeeds:字符串、数组、空白", () => {
  assert.deepEqual(parseSeeds("a:1,b:2"), ["a:1", "b:2"]);
  assert.deepEqual(parseSeeds(["a:1", " b:2 "]), ["a:1", "b:2"]);
  assert.deepEqual(parseSeeds(""), []);
  assert.deepEqual(parseSeeds("a:1,,b:2"), ["a:1", "b:2"]);
});

test("parseLabels 同上", () => {
  assert.deepEqual(parseLabels("web, api"), ["web", "api"]);
  assert.deepEqual(parseLabels(""), []);
});

// ---------------------------------------------------------------- 校验

test("validateOptions:url 协议", () => {
  assert.equal(validateOptions({ url: "http://h:1" }).ok, true);
  assert.equal(validateOptions({ url: "ws://h:1" }).ok, true, "ws:// 也接受");
  const bad = validateOptions({ url: "h:1" });
  assert.equal(bad.ok, false);
  assert.match(bad.reason, /http/);
});

test("validateOptions:mode 必须是三种之一", () => {
  for (const m of ["broker", "mesh", "swim"]) {
    assert.equal(validateOptions({ mode: m }).ok, true, m);
  }
  const bad = validateOptions({ mode: "raft" });
  assert.equal(bad.ok, false);
  assert.match(bad.reason, /broker/);
});

test("validateOptions:seed 必须带端口", () => {
  assert.equal(validateOptions({ seeds: "10.0.0.1:7946" }).ok, true);
  const bad = validateOptions({ seeds: "10.0.0.1" });
  assert.equal(bad.ok, false);
  assert.match(bad.reason, /host:port/);
});

test("validateOptions:token 太短被拒", () => {
  assert.equal(validateOptions({ token: "x".repeat(64) }).ok, true);
  assert.equal(validateOptions({ token: "short" }).ok, false);
});

test("validateOptions:name 允许常见写法,拒绝空格和路径分隔符", () => {
  for (const n of ["laptop", "dev01-web", "a.b_c", "A1"]) {
    assert.equal(validateOptions({ name: n }).ok, true, n);
  }
  for (const n of ["has space", "with/slash", "-leading"]) {
    assert.equal(validateOptions({ name: n }).ok, false, n);
  }
  // 空字符串走的是"没给"这条路,不是"名字非法" —— 否则
  // `--name ""` 会报一个让人摸不着头脑的错误
  assert.equal(validateOptions({ name: "" }).ok, true);
  assert.equal(validateOptions({ name: "" }).session.name, undefined);
});

test("validateOptions:标签上限 8,和成员表一致", () => {
  assert.equal(validateOptions({ labels: "a,b,c" }).ok, true);
  const bad = validateOptions({ labels: Array.from({ length: 9 }, (_, i) => `l${i}`).join(",") });
  assert.equal(bad.ok, false);
  assert.match(bad.reason, /8/);
});

test("validateOptions:team 级和 session 级分开归位", () => {
  const r = validateOptions({ url: "http://h:1", mode: "mesh", name: "me", labels: "web", seeds: "a:1" });
  assert.equal(r.ok, true);
  assert.deepEqual(r.team, { url: "http://h:1", mode: "mesh", seeds: ["a:1"] });
  assert.deepEqual(r.session, { name: "me", labels: ["web"] });
});

test("validateOptions:空值表示没给,不覆盖已有配置", () => {
  const r = validateOptions({ mode: "", labels: "" });
  assert.equal(r.ok, true);
  assert.equal(r.team.mode, undefined);
  assert.equal(r.session.labels, undefined);
});

// ---------------------------------------------------------------- 模式要求

test("checkModeRequirements:任何模式都要 token", () => {
  const r = checkModeRequirements({ mode: "mesh", seeds: ["a:1"] });
  assert.equal(r.ok, false);
  assert.match(r.reason, /token/);
});

test("checkModeRequirements:broker 要 url,并给出写法", () => {
  const r = checkModeRequirements({ mode: "broker", token: "t" });
  assert.equal(r.ok, false);
  assert.match(r.reason, /--url/);
  assert.equal(checkModeRequirements({ mode: "broker", token: "t", url: "http://h:1" }).ok, true);
});

test("checkModeRequirements:mesh/swim 没 seeds 可用但带警告", () => {
  for (const mode of ["mesh", "swim"]) {
    const r = checkModeRequirements({ mode, token: "t" });
    assert.equal(r.ok, true, `${mode} 没有种子不该是致命错误`);
    assert.match(r.warning, /seeds/, `${mode} 要说清后果`);
    assert.equal(checkModeRequirements({ mode, token: "t", seeds: ["a:1"] }).warning, undefined);
  }
});

// ---------------------------------------------------------------- 监听设置(仅本次运行)

test("validateOptions:port 接受 0(内核分配)和合法范围", () => {
  assert.equal(validateOptions({ port: "0" }).session.port, 0, "0 = 自动分配,是合法值");
  assert.equal(validateOptions({ port: "19801" }).session.port, 19801);
  assert.equal(validateOptions({ port: "65535" }).session.port, 65535);
});

test("validateOptions:port 拒绝越界和非数字", () => {
  for (const bad of ["65536", "-1", "abc", "19.5"]) {
    const r = validateOptions({ port: bad });
    assert.equal(r.ok, false, `port=${bad} 应被拒`);
    assert.match(r.reason, /port/);
  }
});

test("validateOptions:listen 接受 IP、主机名和空值透传", () => {
  assert.equal(validateOptions({ listen: "127.0.0.1" }).session.listen, "127.0.0.1");
  assert.equal(validateOptions({ listen: "0.0.0.0" }).session.listen, "0.0.0.0");
  assert.equal(validateOptions({ listen: "::" }).session.listen, "::");
  assert.equal(validateOptions({ listen: "my-host" }).session.listen, "my-host");
  // 空值走"没给"那条路,不是报错
  assert.equal(validateOptions({ listen: "" }).session.listen, undefined);
});

test("validateOptions:listen 含空格时报错", () => {
  const r = validateOptions({ listen: "127.0.0.1 8080" });
  assert.equal(r.ok, false);
  assert.match(r.reason, /listen/);
});

test("port 和 listen 归到 session 级 —— 绝不落盘", () => {
  const r = validateOptions({ port: "19801", listen: "127.0.0.1", url: "http://h:1" });
  assert.equal(r.ok, true);
  assert.equal(r.session.port, 19801);
  assert.equal(r.session.listen, "127.0.0.1");
  assert.equal("port" in r.team, false, "端口不能进 team 配置");
  assert.equal("listen" in r.team, false);
});

test("ALL_KEYS 包含 port 和 listen", () => {
  assert.ok(ALL_KEYS.includes("port"));
  assert.ok(ALL_KEYS.includes("listen"));
});
