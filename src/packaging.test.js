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
