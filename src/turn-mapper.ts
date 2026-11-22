/**
 * TurnMapper — 把 claude stream-json 事件映射回“哪个用户输入该收到回复”。
 *
 * 背景（2026-11-22 实测）：headless 下派后台 subagent 时，事件流会出现两类
 * 不属于用户回合的噪声：
 *   1. 子 agent 的 assistant 消息带 parent_tool_use_id，其思考/执行文本会混入
 *      用户可见回复；
 *   2. subagent 完成后 Claude Code 会自动生成一个“回合”，其 result 带
 *      origin.kind="task-notification"。如果把它当普通 result，会弹走 FIFO
 *      队列里属于真实用户消息的 ctx，造成回复错位甚至丢回复。
 *
 * 本模块是纯状态机，便于用事件夹具做单元测试；daemon.ts 只负责 I/O。
 */

type Json = Record<string, unknown>;

export interface MapperReply<Ctx> {
  ctx: Ctx;
  text: string;
}

export interface MapperOutcome<Ctx> {
  reply?: MapperReply<Ctx>;
  /** result 没有对应用户回合（例如后台任务收尾），已丢弃 */
  droppedOrphan?: boolean;
  /** 后台任务（task-notification）的 result，已静默忽略 */
  ignoredBackground?: boolean;
}

export class TurnMapper<Ctx> {
  private turns: Ctx[] = [];
  private buffer: string[] = [];

  get pending(): number {
    return this.turns.length;
  }

  enqueue(ctx: Ctx): void {
    this.turns.push(ctx);
  }

  ingest(ev: Json): MapperOutcome<Ctx> {
    const type = ev.type;

    if (type === 'assistant') {
      // 子 agent 的内部消息不进入用户可见回复
      if (ev.parent_tool_use_id != null) return {};
      const message = ev.message as Json | undefined;
      const content = message?.content;
      if (Array.isArray(content)) {
        for (const block of content as Json[]) {
          if (block.type === 'text' && typeof block.text === 'string') {
            this.buffer.push(block.text);
          }
        }
      }
      return {};
    }

    if (type === 'result') {
      const origin = ev.origin as Json | undefined;
      if (origin && origin.kind === 'task-notification') {
        // 后台任务收尾回合：不消费用户 ctx；清掉它产生的文本，避免污染下一条回复
        this.buffer = [];
        return { ignoredBackground: true };
      }

      const ctx = this.turns.shift();
      let text = this.buffer.join('\n').trim();
      this.buffer = [];
      if (!text) {
        const resultText = typeof ev.result === 'string' ? ev.result : '';
        const cmd = typeof ev.local_command === 'string' ? ev.local_command : '';
        text = resultText.trim() || (cmd ? `已执行 /${cmd}` : '（无输出）');
      }
      if (!ctx) return { droppedOrphan: true };
      return { reply: { ctx, text } };
    }

    return {};
  }

  /**
   * 空闲兜底：如果仍有用户回合挂起且缓冲里有文本，说明 result 一直没来，
   * 直接把已生成的文本发给队首 ctx，避免永久静默。
   */
  flushIdle(): MapperReply<Ctx> | undefined {
    if (!this.turns.length) return undefined;
    const text = this.buffer.join('\n').trim();
    if (!text) return undefined;
    this.buffer = [];
    const ctx = this.turns.shift() as Ctx;
    return { ctx, text };
  }
}
