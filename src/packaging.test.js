/**
 * 发布包内容的守卫。
 *
 * package.json 的 files 逐个列出源文件,而不是整个 src/ —— 后者会把
 * *.test.js 一起发布。代价是新增源文件时容易忘了加进列表,那样发布包
 * 会缺一个模块,用户那边 import 直接失败,而仓库里的测试全绿。
 *
 * 这个测试把两个方向都卡住:该发的都发了,不该发的都没发。
 */
import test from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const pkg = JSON.parse(readFileSync(join(ROOT, "package.json"), "utf8"));
const files = new Set(pkg.files);

const sources = readdirSync(join(ROOT, "src")).filter((f) => f.endsWith(".js") && !f.endsWith(".test.js"));

test("每个 src/ 源文件都在发布列表里", () => {
  const missing = sources.filter((f) => !files.has(`src/${f}`));
  assert.deepEqual(missing, [], `这些文件不会被发布,装了包的用户 import 会失败:${missing.join(", ")}`);
});

test("测试文件不在发布列表里", () => {
  const leaked = [...files].filter((f) => f.endsWith(".test.js") || f === "src/" || f === "src" || f.startsWith("e2e"));
  assert.deepEqual(leaked, [], `这些会把测试打进包:${leaked.join(", ")}`);
});

test("入口文件和 SWIM 源码都在发布列表里", () => {
  for (const f of ["index.ts", "broker.mjs", "swim/main.go", "swim/go.mod", "swim/go.sum"]) {
    assert.ok(files.has(f), `${f} 缺失 —— 没有它${f.startsWith("swim") ? " swim 模式建不了边车" : "插件起不来"}`);
  }
});

test("pi.extensions 指向的文件都会被发布", () => {
  for (const ext of pkg.pi?.extensions ?? []) {
    const rel = ext.replace(/^\.\//, "");
    assert.ok(files.has(rel), `pi.extensions 声明了 ${ext},但它不在 files 里`);
  }
});

test("index.ts 引用的 src 模块都会被发布", () => {
  const src = readFileSync(join(ROOT, "index.ts"), "utf8");
  const imported = [...src.matchAll(/from\s+"\.\/(src\/[^"]+\.js)"/g)].map((m) => m[1]);
  assert.ok(imported.length > 0, "前置条件:应能解析出 index.ts 的导入");
  const missing = imported.filter((f) => !files.has(f));
  assert.deepEqual(missing, [], `index.ts 导入了这些,但它们不会被发布:${missing.join(", ")}`);
});

test("bin 指向的文件都会被发布,且路径不带 ./ 前缀", () => {
  // npm 11 会把 "./broker.mjs" 标为 invalid 并在发布时"纠正"。
  // 目前纠正后还能用,但依赖它的纠错不是个可靠的前提 —— 哪天不再纠正,
  // README 第一步的 pi-agent-team-broker 就会变成 command not found。
  for (const [cmd, path] of Object.entries(pkg.bin ?? {})) {
    assert.equal(path.startsWith("./"), false, `bin.${cmd} 不该带 ./ 前缀,实际 "${path}"`);
    assert.ok(files.has(path), `bin.${cmd} 指向 ${path},但它不在 files 里`);
  }
});

test("bin 指向的文件有 shebang,否则装完不能直接执行", () => {
  for (const [cmd, path] of Object.entries(pkg.bin ?? {})) {
    const head = readFileSync(join(ROOT, path), "utf8").split("\n")[0];
    assert.match(head, /^#!.*node/, `bin.${cmd} (${path}) 第一行应是 #!/usr/bin/env node`);
  }
});

test("所有 peerDependency 都标了 optional", () => {
  // 这个包有两种用法:作为 Pi 扩展(peer 必须存在),以及只为跑 broker
  // 而安装(那台机器上不需要 Pi)。
  //
  // 不标 optional 时 npx -p 会把 Pi 连同它的依赖一起拉下来 —— 实测
  // 435 MB,而 broker 只是个单文件脚本。npm install 只花 532 KB,所以
  // 只看 npm install 发现不了;0.2.0 就是这么发出去的。
  const peers = Object.keys(pkg.peerDependencies ?? {});
  const meta = pkg.peerDependenciesMeta ?? {};
  const required = peers.filter((p) => meta[p]?.optional !== true);
  assert.deepEqual(
    required,
    [],
    `这些 peer 没标 optional,npx 会强装它们:${required.join(", ")}`,
  );
});
