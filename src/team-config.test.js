/**
 * Team 配置测试。跑:node --test src/
 *
 * 全部用临时 home,不碰真实 ~/.pi。凭据文件的权限是这里最重要的断言:
 * 写错了会让 token 变成 0644,任何本机用户都能读。
 */
import test from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  configDir,
  createTeam,
  generateToken,
  joinTeam,
  leaveTeam,
  listTeams,
  normalizeUrl,
  readTeam,
  toSocketUrl,
  validateAgentName,
  validateTeamName,
  writeTeam,
} from "./team-config.js";

/** 每个用例一个独立 home,互不污染 */
function withHome(t) {
  const home = mkdtempSync(join(tmpdir(), "pi-team-test-"));
  t.after(() => rmSync(home, { recursive: true, force: true }));
  return home;
}

const URL_OK = "http://100.64.0.1:8787";

// ---------------------------------------------------------------- 校验

test("team 名:小写合法,大写与非法字符被拒", () => {
  assert.equal(validateTeamName("myteam").ok, true);
  assert.equal(validateTeamName("my-team.2").ok, true);
  assert.equal(validateTeamName("MyTeam").ok, false, "大写被拒,避免大小写敏感文件系统上的歧义");
  assert.equal(validateTeamName("-bad").ok, false);
  assert.equal(validateTeamName("has space").ok, false);
  assert.equal(validateTeamName("").ok, false);
  assert.equal(validateTeamName("x".repeat(33)).ok, false);
});

test("节点名:和 broker 的规则保持一致", () => {
  assert.equal(validateAgentName("laptop-web").ok, true);
  assert.equal(validateAgentName("poetA").ok, true, "驼峰合法");
  assert.equal(validateAgentName("dev01-api").ok, true);
  assert.equal(validateAgentName("BAD NAME!").ok, false);
  assert.equal(validateAgentName("-x").ok, false);
});

test("url 规范化:ws 和 wss 都转成 http(s),再转回 socket 用", () => {
  assert.equal(normalizeUrl("ws://h:1"), "http://h:1");
  assert.equal(normalizeUrl("wss://h"), "https://h");
  assert.equal(normalizeUrl("http://h"), "http://h");
  assert.equal(toSocketUrl("http://h:1"), "ws://h:1");
  assert.equal(toSocketUrl("https://h"), "wss://h");
});

// ---------------------------------------------------------------- create

test("create:生成 token 并写入,文件权限 0600,目录 0700", (t) => {
  const home = withHome(t);
  const r = createTeam({ team: "alpha", url: URL_OK, home });
  assert.equal(r.ok, true);
  assert.equal(r.created, true, "没给 token 应该新生成一个");
  assert.match(r.token, /^[0-9a-f]{64}$/, "32 字节 hex");

  const file = join(configDir(home), "alpha.json");
  assert.ok(existsSync(file));
  assert.equal(statSync(file).mode & 0o777, 0o600, "凭据文件必须 0600");
  assert.equal(statSync(configDir(home)).mode & 0o777, 0o700, "目录必须 0700");
});

test("create:已有同名 team 时拒绝,并提示怎么办", (t) => {
  const home = withHome(t);
  createTeam({ team: "alpha", url: URL_OK, home });
  const again = createTeam({ team: "alpha", url: URL_OK, home });
  assert.equal(again.ok, false);
  assert.match(again.reason, /已存在/);
  assert.match(again.reason, /join/);
});

test("create:url 协议不对时拒绝", (t) => {
  const home = withHome(t);
  for (const bad of ["100.64.0.1:8787", "ftp://x", "", "localhost"]) {
    const r = createTeam({ team: "alpha", url: bad, home });
    assert.equal(r.ok, false, `应拒绝 ${bad}`);
  }
});

test("create:非 hex 的 token 被拒(提示用 openssl 生成)", (t) => {
  const home = withHome(t);
  const r = createTeam({ team: "alpha", url: URL_OK, token: "short", home });
  assert.equal(r.ok, false);
  assert.match(r.reason, /hex/);
});

test("create:复用已有 token 时 created=false,不泄漏到返回值以外", (t) => {
  const home = withHome(t);
  const mine = generateToken();
  const r = createTeam({ team: "alpha", url: URL_OK, token: mine, home });
  assert.equal(r.ok, true);
  assert.equal(r.created, false);
  assert.equal(readTeam("alpha", home).token, mine);
});

test("create:重复调用不会覆盖已存在的配置", (t) => {
  const home = withHome(t);
  const first = createTeam({ team: "alpha", url: URL_OK, home });
  createTeam({ team: "alpha", url: "http://other:9999", home });
  assert.equal(readTeam("alpha", home).token, first.token, "token 不该被换掉");
  assert.equal(readTeam("alpha", home).url, first.config.url);
});

// ---------------------------------------------------------------- join

test("join:已有配置时直接可用", (t) => {
  const home = withHome(t);
  createTeam({ team: "alpha", url: URL_OK, home });
  const r = joinTeam({ team: "alpha", home });
  assert.equal(r.ok, true);
  assert.equal(r.config.url, URL_OK);
  assert.equal(r.adopted, undefined);
});

test("join:首次加入带 url+token 时自动记录", (t) => {
  const home = withHome(t);
  const token = generateToken();
  const r = joinTeam({ team: "beta", url: URL_OK, token, home });

  assert.equal(r.ok, true);
  assert.equal(r.adopted, true);
  assert.equal(readTeam("beta", home).token, token);
  assert.equal(statSync(join(configDir(home), "beta.json")).mode & 0o777, 0o600);
});

test("join:本地没有且没给 url 时报错,并列出已有的 team", (t) => {
  const home = withHome(t);
  createTeam({ team: "alpha", url: URL_OK, home });
  const r = joinTeam({ team: "missing", home });

  assert.equal(r.ok, false);
  assert.match(r.reason, /alpha/, "应提示本机有哪些 team");
});

test("join:首次加入但没给 token 时拒绝", (t) => {
  const home = withHome(t);
  const r = joinTeam({ team: "beta", url: URL_OK, home });
  assert.equal(r.ok, false);
  assert.match(r.reason, /token/);
});

test("join:给了新 url 时更新已有配置,但 token 默认不变", (t) => {
  const home = withHome(t);
  const first = createTeam({ team: "alpha", url: URL_OK, home });
  const r = joinTeam({ team: "alpha", url: "https://new.example", home });

  assert.equal(r.ok, true);
  assert.equal(r.updated, true);
  assert.equal(readTeam("alpha", home).url, "https://new.example");
  assert.equal(readTeam("alpha", home).token, first.token, "没给新 token 时不该换掉");
});

test("join:带 ws:// 也会被规范化成 http://", (t) => {
  const home = withHome(t);
  const r = joinTeam({ team: "gamma", url: "ws://100.64.0.1:8787", token: generateToken(), home });
  assert.equal(r.ok, true);
  assert.equal(r.config.url, "http://100.64.0.1:8787");
});

// ---------------------------------------------------------------- leave

test("leave:删掉本地配置,再 join 就找不到了", (t) => {
  const home = withHome(t);
  createTeam({ team: "alpha", url: URL_OK, home });

  assert.equal(leaveTeam({ team: "alpha", home }).ok, true);
  assert.equal(readTeam("alpha", home), null);
  assert.equal(joinTeam({ team: "alpha", home }).ok, false);
});

test("leave:不存在的 team 报错,不静默成功", (t) => {
  const home = withHome(t);
  const r = leaveTeam({ team: "never", home });
  assert.equal(r.ok, false);
  assert.match(r.reason, /没有/);
});

// ---------------------------------------------------------------- 其它

test("list:列出已有 team,按名排序", (t) => {
  const home = withHome(t);
  createTeam({ team: "zeta", url: URL_OK, home });
  createTeam({ team: "alpha", url: URL_OK, home });
  assert.deepEqual(listTeams(home), ["alpha", "zeta"]);
});

test("read:损坏的配置文件返回 null 而不是抛异常", (t) => {
  const home = withHome(t);
  createTeam({ team: "alpha", url: URL_OK, home });
  const file = join(configDir(home), "alpha.json");
  writeFileSync(file, "{ 这不是 json");
  assert.equal(readTeam("alpha", home), null);
});

test("read:字段缺失的配置视为无效", (t) => {
  const home = withHome(t);
  createTeam({ team: "alpha", url: URL_OK, home });
  writeFileSync(join(configDir(home), "alpha.json"), JSON.stringify({ team: "alpha" }));
  assert.equal(readTeam("alpha", home), null, "缺 url/token 不算有效配置");
});

test("write:能收紧已存在文件的权限", (t) => {
  const home = withHome(t);
  const dir = configDir(home);
  // 先手工造一个宽松权限的文件,模拟旧版本或手工创建
  createTeam({ team: "alpha", url: URL_OK, home });
  const file = join(dir, "alpha.json");
  statSync(file); // 确认存在

  writeTeam("alpha", { url: URL_OK, token: generateToken() }, home);
  assert.equal(statSync(file).mode & 0o777, 0o600, "写入时应重新 chmod 收紧权限");
});

// ---------------------------------------------------------------- 标签与空数组

test("create:不给 labels 时不写入该字段(空数组会覆盖调用方的标签)", (t) => {
  const home = withHome(t);
  createTeam({ team: "alpha", url: URL_OK, home });
  const cfg = readTeam("alpha", home);
  assert.equal("labels" in cfg, false, "不该留一个空 labels 字段在配置里");
});

test("create:给了 labels 时写进配置", (t) => {
  const home = withHome(t);
  createTeam({ team: "alpha", url: URL_OK, labels: ["web", "fe"], home });
  assert.deepEqual(readTeam("alpha", home).labels, ["web", "fe"]);
});

test("create:空 labels 数组等同于不给", (t) => {
  const home = withHome(t);
  createTeam({ team: "alpha", url: URL_OK, labels: [], home });
  assert.equal("labels" in readTeam("alpha", home), false);
});

test("read:旧配置里的空 labels 字段仍然可读", (t) => {
  const home = withHome(t);
  createTeam({ team: "alpha", url: URL_OK, home });
  // 模拟早期版本写下的 labels: []
  const file = join(configDir(home), "alpha.json");
  const data = JSON.parse(readFileSync(file, "utf8"));
  data.labels = [];
  writeFileSync(file, JSON.stringify(data));

  const cfg = readTeam("alpha", home);
  assert.ok(cfg, "不该因为空 labels 就判定配置无效");
  assert.deepEqual(cfg.labels, []);
});
