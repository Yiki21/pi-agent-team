/**
 * index.ts 接线测试。跑:node --test src/
 *
 * 这一层以前完全没有测试:决策逻辑在 src/ 里被覆盖得很好,但"把 Pi 的事件
 * 接到决策上"这段胶水只能靠真机手测。发现的几个问题(系统提示被整体替换、
 * 会话切换后状态残留、abort/error 那一轮不再提醒)恰好都在这个盲区里。
 *
 * 这里用一个假的 `pi` 驱动真实的扩展工厂,只断言可观测的行为。
 * 用 Node 的类型剥离直接跑 .ts,不需要先构建。
 */
import test from "node:test";
import assert from "node:assert/strict";
import registerExtension from "../index.ts";

function fakeUi() {
  return {
    notify() {},
    setStatus() {},
    select: async () => undefined,
    confirm: async () => false,
    input: async () => undefined,
  };
}

/** 用一个假的 pi 加载真实扩展 */
function load() {
  const handlers = new Map();
  const flags = new Map();
  const tools = [];
  const commands = [];

  registerExtension({
    on(event, handler) {
      const list = handlers.get(event) ?? [];
      list.push(handler);
      handlers.set(event, list);
    },
    registerTool(tool) {
      tools.push(tool);
    },
    registerCommand(name, options) {
      commands.push({ name, options });
    },
    registerFlag(name, options) {
      flags.set(name, options);
    },
    // 和 Pi 一致:未注册的名字无条件返回 undefined
    getFlag() {
      return undefined;
    },
    registerEntryRenderer() {},
    appendEntry() {},
    sendMessage() {},
  });

  return { handlers, flags, tools, commands };
}

/** 依次触发某个事件的所有 handler,返回它们的结果 */
async function emit(fake, event, payload = {}, ctx = { ui: fakeUi() }) {
  const out = [];
  for (const handler of fake.handlers.get(event) ?? []) {
    out.push(await handler(payload, ctx));
  }
  return out;
}

// ---------------------------------------------------------------- 系统提示

test("before_agent_start 用 sections 追加,不替换整个系统提示", async () => {
  const fake = load();
  const event = {
    prompt: "hi",
    systemPrompt: "ORIGINAL PROMPT",
    systemPromptOptions: { sections: { existing: "KEEP ME" } },
  };

  const results = await emit(fake, "before_agent_start", event);

  for (const r of results) {
    assert.equal(
      r?.systemPrompt,
      undefined,
      "返回 systemPrompt 会替换掉这一轮的完整提示,排在我们后面的 handler 全部失效",
    );
  }
  assert.equal(event.systemPromptOptions.sections.existing, "KEEP ME", "别人的 section 要留着");
  assert.equal(typeof event.systemPromptOptions.sections.team, "string");
  assert.match(event.systemPromptOptions.sections.team, /Team/);
});

// ---------------------------------------------------------------- flag 注册

test("--team-announce 真的注册了", () => {
  const fake = load();
  for (const name of ["team", "team-name", "team-announce", "team-reply"]) {
    assert.equal(fake.flags.has(name), true, `${name} 必须注册,否则 getFlag 永远是 undefined`);
  }
});

// ---------------------------------------------------------------- 会话生命周期

test("session_start 清掉上一条会话的待回复队列", async () => {
  const fake = load();
  const ctx = { ui: fakeUi() };

  await emit(fake, "session_start", { reason: "new" }, ctx);
  // 灌入一个队友请求:进入 pendingReplies
  await emit(
    fake,
    "message_end",
    { message: { role: "custom", customType: "team-msg", content: "[team 待回复] peer: DO IT" } },
    ctx,
  );

  // 换会话。resume / fork 都不该继承上一个会话的待回复请求,
  // 否则模型会被提醒去回复一条它从未见过的请求。
  await emit(fake, "session_start", { reason: "resume" }, ctx);

  // 这一轮干净收尾时,不该产出任何针对旧请求的提醒
  const actions = await emit(fake, "agent_settled", { type: "agent_settled" }, ctx);
  for (const a of actions) {
    assert.equal(a, undefined, "新会话不该提醒回复上一个会话的请求");
  }
});

test("session_shutdown 只注册一个 handler,不抛异常", async () => {
  const fake = load();
  const ctx = { ui: fakeUi() };
  await emit(fake, "session_start", { reason: "new" }, ctx);
  const results = await emit(fake, "session_shutdown", {}, ctx);
  assert.equal(results.length, 1);
});

// ---------------------------------------------------------------- 工具/命令表面

test("注册了核心工具", () => {
  const fake = load();
  const names = fake.tools.map((t) => t.name);
  for (const required of ["team_send", "team_roster", "team_info"]) {
    assert.equal(names.includes(required), true, `缺少工具 ${required}(已有 ${names.join(",")})`);
  }
});

test("注册了 /team 命令", () => {
  const fake = load();
  assert.equal(
    fake.commands.some((c) => c.name === "team"),
    true,
  );
});
