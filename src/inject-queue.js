/**
 * 注入队列:决定一条队友消息是立刻作为 prompt 送进模型,还是先排着。
 *
 * ── 要解决的问题 ──
 * 实测(Pi 0.87,两条消息落在同一个同步 tick 里):
 *
 *   29664ms  INJECT X ok (direct)
 *   29664ms  INJECT Y ok (direct)        ← 调用没抛异常
 *   29666ms  before_agent_start  prompt="PROBE-X..."
 *   29666ms  before_agent_start  prompt="PROBE-Y..."   ← Y 也触发了
 *   30139ms  agent_start
 *   30158ms  message_end role=user text="PROBE-X..."   ← 但只有 X 进了模型
 *   37358ms  message_end role=assistant text="EX"
 *   37365ms  agent_end                  ← 没有第二个 turn,Y 消失了
 *
 * 也就是说:先注但尚未开始处理的直发 prompt 会被后一条覆盖,而两次
 * 调用都返回成功。调用方以为发出去了,其实没有 —— 这比抛异常更糟,
 * 因为发信人会一直等一个永远不会来的回信。
 *
 * 为什么不能靠 `ctx.isIdle()`:它在同一 tick 里两次都返回 true
 * (同一份探针实测),所以第二条也走直发,正好落进这个覆盖窗口。
 *
 * ── 规则 ──
 *   run 尚未开始(第一条已发出、agent_start 还没来)
 *     → 第一条直发,prompt 由此开始一个 run
 *     → 其余全部排进本地队列,不再直发
 *   agent_start 到达
 *     → 把队列里的逐条以 followUp 投出(此时 run 确实在跑,不会被拒)
 *   run 正在跑(agent_start 之后、agent_settled 之前)
 *     → 直接 followUp
 *   run 已结束(agent_settled 之后)
 *     → 直发,开始新的 run
 *
 * ── 为什么把队列排到 agent_start 才投 ──
 * agent_start 之前投 followUp 是无效的:那时代理还没开始处理,Pi 会
 * 把它当普通 prompt,于是又落回上面的覆盖窗口。
 *
 * ── 为什么单独一个模块 ──
 * 这段逻辑只有在真实 Pi 里、且时序凑巧时才会出问题,在 index.ts 里
 * 没法测。抽成纯函数后,上面那张时序表可以直接写成单元测试。
 */

/**
 * @typedef {object} InjectQueueState
 * @property {"ready"|"starting"|"running"} phase
 *   ready    run 没在跑,下一条可以直发
 *   starting 第一条已发出,等 agent_start(run 尚未真正开始)
 *   running  run 正在跑,只能 followUp
 * @property {string[]} queued 等待投出的 payload,按到达顺序
 */

/** @returns {InjectQueueState} */
export function createInjectState() {
  return { phase: "ready", queued: [] };
}

/**
 * 接受一条要注入的消息,给出该怎么投。
 *
 * 纯函数:不改原状态,返回新状态。这样"同一 tick 里连着调两次"在
 * 测试里就是普普通通的两次调用,不需要仿真任何时序。
 *
 * @returns {{ state: InjectQueueState, mode: "direct"|"followUp"|"queued" }}
 *   direct   立刻作为 prompt 送出(会开始一个 run)
 *   followUp 立刻以 followUp 送出(run 在跑)
 *   queued   先排着,等 agent_start 再投
 */
export function onInject(state, payload) {
  if (state.phase === "running") {
    // run 确实在跑,Pi 明确支持 followUp
    return { state, mode: "followUp" };
  }

  if (state.phase === "ready") {
    // 空闲:直发,由它开始一个 run。
    // 不能在这里继续直发第二条 —— 见文件头那段时序。
    return { state: { ...state, phase: "starting" }, mode: "direct" };
  }

  // phase === "starting":第一条已发出但 run 还没开始。
  // 再直发就会被覆盖,所以排队。
  return { state: { ...state, queued: [...state.queued, payload] }, mode: "queued" };
}

/**
 * agent_start 到了:run 正式开始,把排队的一次性投出。
 *
 * @returns {{ state: InjectQueueState, flush: string[] }}
 *   flush 是要以 followUp 投出的 payload,顺序即到达顺序
 */
export function onRunStart(state) {
  return { state: { phase: "running", queued: [] }, flush: state.queued };
}

/**
 * agent_settled 到了:run 结束,回到空闲。
 *
 * 队列一并清掉:能走到这里说明 run 已经完整跑完,那些排队的 payload
 * 在 agent_start 时就已经投出去了,不该再留一份。
 */
export function onSettled(state) {
  return { phase: "ready", queued: [] };
}

/** 有没有排队的消息 —— 给状态显示用 */
export const hasQueued = (state) => state.queued.length > 0;
