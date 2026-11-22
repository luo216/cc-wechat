#!/usr/bin/env node
/**
 * cc-wechat daemon 模式 — 无 tmux 的微信 ↔ Claude Code 桥
 *
 * 架构：iLink 长轮询 → 常驻 claude 进程（-p --input-format stream-json）
 *       微信消息写成一行 stream-json 用户输入；内置斜杠命令（/compact 等）
 *       由 Claude Code 本地执行，不经过模型。
 *
 * 用法：cc-wechat daemon --cwd /home/steve/WorkSpace/bot
 */

import './proxy.js';

import { spawn, type ChildProcess } from 'node:child_process';
import readline from 'node:readline';
import { existsSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

import { getActiveAccount, loadSyncBuf, saveSyncBuf, getStateDir } from './store.js';
import { getUpdates, sendMessage, sendTyping, getConfig } from './ilink-api.js';
import { stripMarkdown, chunkText } from './text-utils.js';
import { uploadMedia, downloadMedia } from './cdn.js';
import type { AccountData, WeixinMessage } from './types.js';
import { MessageItemType } from './types.js';
import { TurnMapper } from './turn-mapper.js';

const SESSION_FILE = 'claude-session.json';
const SESSION_EXPIRED_ERRCODE = -14;
const MAX_SESSION_RETRIES = 3;
const MAX_CONSECUTIVE_FAILURES = 5;
const INITIAL_RETRY_DELAY_MS = 2_000;
const MAX_RETRY_DELAY_MS = 30_000;
const SESSION_PAUSE_MS = 5 * 60_000;

function log(...args: unknown[]): void {
  console.log(new Date().toISOString(), ...args);
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    signal?.addEventListener('abort', () => {
      clearTimeout(t);
      reject(new Error('aborted'));
    }, { once: true });
  });
}

// ─── 会话状态持久化 ───────────────────────────────────

interface SessionState {
  sessionId?: string;
  cwd: string;
}

function sessionPath(): string {
  return join(getStateDir(), SESSION_FILE);
}

function loadSession(): SessionState | null {
  try {
    return JSON.parse(readFileSync(sessionPath(), 'utf-8')) as SessionState;
  } catch {
    return null;
  }
}

function saveSession(state: SessionState): void {
  writeFileSync(sessionPath(), JSON.stringify(state, null, 2), 'utf-8');
}

// ─── 微信消息 → 文本（复制自 server.ts 的 extractText） ──

async function extractText(msg: WeixinMessage): Promise<string> {
  const parts: string[] = [];
  for (const item of msg.item_list ?? []) {
    const t = item.type ?? 0;

    if (t === MessageItemType.TEXT) {
      if (item.text_item?.text) {
        if (item.ref_msg) {
          const refContent = item.ref_msg.title ?? item.ref_msg.message_item?.text_item?.text ?? '';
          if (refContent) parts.push(`[引用: ${refContent}]`);
        }
        parts.push(item.text_item.text);
      }
    } else if (t === MessageItemType.IMAGE) {
      let desc = '[图片]';
      const media = item.image_item?.media;
      if ((media?.encrypt_query_param || media?.full_url) && media?.aes_key) {
        try {
          const filePath = await downloadMedia({
            encryptQueryParam: media.encrypt_query_param ?? '',
            aesKeyBase64: media.aes_key,
            fullUrl: media.full_url,
          });
          desc += `\n[附件: ${filePath}]`;
        } catch { /* 下载失败不阻塞 */ }
      }
      parts.push(desc);
    } else if (t === MessageItemType.VOICE) {
      parts.push(`[语音] ${item.voice_item?.text ?? ''}`);
    } else if (t === MessageItemType.FILE) {
      let desc = `[文件: ${item.file_item?.file_name ?? 'unknown'}]`;
      const media = item.file_item?.media;
      if ((media?.encrypt_query_param || media?.full_url) && media?.aes_key) {
        try {
          const filePath = await downloadMedia({
            encryptQueryParam: media.encrypt_query_param ?? '',
            aesKeyBase64: media.aes_key,
            fileName: item.file_item?.file_name,
            fullUrl: media.full_url,
          });
          desc += `\n[附件: ${filePath}]`;
        } catch { /* 下载失败不阻塞 */ }
      }
      parts.push(desc);
    } else if (t === MessageItemType.VIDEO) {
      parts.push('[视频]');
    }
  }
  return parts.join('\n') || '[空消息]';
}

// ─── 单个微信输入的上下文，用于把 result 回给正确的人 ──

interface TurnCtx {
  userId: string;
  contextToken: string;
  refMsgId?: string;
}

type Json = Record<string, unknown>;

/**
 * 常驻 Claude Code 进程管理器。
 * 每个写入 stdin 的用户输入对应一个 TurnCtx；每个 result 事件消费一个。
 */
class ClaudeRunner {
  private child: ChildProcess | null = null;
  private mapper = new TurnMapper<TurnCtx>();
  private lastEventAt = Date.now();
  private idleTimer: NodeJS.Timeout | null = null;
  private sessionId: string | undefined;
  private stopping = false;
  private restartTimer: NodeJS.Timeout | null = null;

  /** 回合挂起但长时间没有任何事件时，兜底发送已生成的文本 */
  private static readonly IDLE_FLUSH_MS = 120_000;

  constructor(
    private readonly cwd: string,
    private readonly claudeBin: string,
    private readonly onReply: (ctx: TurnCtx, text: string) => Promise<void>,
  ) {}

  start(): void {
    this.stopping = false;
    if (!this.idleTimer) {
      this.idleTimer = setInterval(() => {
        if (Date.now() - this.lastEventAt < ClaudeRunner.IDLE_FLUSH_MS) return;
        const out = this.mapper.flushIdle();
        if (!out) return;
        log('[claude] result timeout, flushing buffered text');
        void this.onReply(out.ctx, out.text).catch((err) => log('[reply] failed:', String(err)));
      }, 5_000);
      this.idleTimer.unref?.();
    }
    const saved = loadSession();
    const resumeId = saved?.cwd === this.cwd ? saved.sessionId : undefined;

    const args = [
      '-p',
      '--input-format', 'stream-json',
      '--output-format', 'stream-json',
      '--verbose',
      '--permission-mode', 'bypassPermissions',
      ...(resumeId ? ['--resume', resumeId] : []),
    ];

    log(`[claude] spawn: ${this.claudeBin} ${args.join(' ')} (cwd=${this.cwd}${resumeId ? `, resume=${resumeId}` : ''})`);
    const child = spawn(this.claudeBin, args, {
      cwd: this.cwd,
      stdio: ['pipe', 'pipe', 'inherit'],
      env: process.env,
    });
    this.child = child;

    const rl = readline.createInterface({ input: child.stdout });
    rl.on('line', (line) => this.handleLine(line));

    child.on('exit', (code, signal) => {
      log(`[claude] exited code=${code} signal=${signal}`);
      this.child = null;
      if (!this.stopping) this.scheduleRestart();
    });
    child.on('error', (err) => {
      log(`[claude] spawn error: ${String(err)}`);
      this.scheduleRestart();
    });
  }

  private scheduleRestart(): void {
    if (this.restartTimer || this.stopping) return;
    this.restartTimer = setTimeout(() => {
      this.restartTimer = null;
      this.start();
    }, 3_000);
  }

  private sendInput(text: string, ctx: TurnCtx): void {
    if (!this.child) {
      log('[claude] not running, input dropped');
      void this.onReply(ctx, 'Claude 进程未就绪，请稍后重试').catch(() => {});
      return;
    }
    this.mapper.enqueue(ctx);
    const payload = { type: 'user', message: { role: 'user', content: [{ type: 'text', text }] } };
    this.child.stdin?.write(JSON.stringify(payload) + '\n');
  }

  private handleLine(line: string): void {
    let ev: Json;
    try {
      ev = JSON.parse(line) as Json;
    } catch {
      return;
    }

    this.lastEventAt = Date.now();
    const type = ev.type;

    if (type === 'system' && ev.subtype === 'init') {
      const sid = ev.session_id;
      if (typeof sid === 'string') {
        this.sessionId = sid;
        saveSession({ sessionId: sid, cwd: this.cwd });
        log(`[claude] session=${sid}`);
      }
      return;
    }

    const out = this.mapper.ingest(ev);
    if (out.ignoredBackground) {
      log('[claude] background task result ignored');
      return;
    }
    if (out.droppedOrphan) {
      log('[claude] result without pending turn, dropped');
      return;
    }
    if (out.reply) {
      void this.onReply(out.reply.ctx, out.reply.text).catch((err) => log('[reply] failed:', String(err)));
    }
  }

  send(text: string, ctx: TurnCtx): void {
    this.sendInput(text, ctx);
  }

  stop(): void {
    this.stopping = true;
    if (this.restartTimer) {
      clearTimeout(this.restartTimer);
      this.restartTimer = null;
    }
    if (this.idleTimer) {
      clearInterval(this.idleTimer);
      this.idleTimer = null;
    }
    this.child?.kill('SIGTERM');
  }

  get currentSessionId(): string | undefined {
    return this.sessionId;
  }
}

// ─── 回复微信 ─────────────────────────────────────────
//
// 除纯文本外，assistant 输出里可携带本地媒体，由 daemon 上传后发到微信：
//   [[image:/abs/path.png]]   → 图片
//   [[file:/abs/path.pdf]]    → 文件（按扩展名自动判定图片/视频/文件）
//   [[media:/abs/path]]       → 同上（自动判定）
// 也兼容 Markdown 图片语法（仅当目标路径存在且为本地文件时）：
//   ![截图](/abs/path.png)

interface OutgoingMedia {
  path: string;
  /** 原始标记，用于失败时提示 */
  raw: string;
}

const MEDIA_MARKER_RE = /\[\[(?:image|img|file|media):([^\]]+)\]\]/gi;
const MD_IMAGE_RE = /!\[[^\]]*\]\(([^)\s]+)\)/g;

function normalizeMediaPath(raw: string): string {
  let p = raw.trim().replace(/^["']|["']$/g, '');
  if (p.startsWith('file://')) p = p.slice('file://'.length);
  if (p.startsWith('~/')) p = join(process.env.HOME ?? '', p.slice(2));
  return resolve(p);
}

function isLocalFile(p: string): boolean {
  try {
    return existsSync(p) && statSync(p).isFile();
  } catch {
    return false;
  }
}

/** 拆出文本里的媒体标记与 Markdown 本地图片；返回剩余文本与媒体列表（保持出现顺序） */
function extractOutgoingMedia(text: string): { text: string; media: OutgoingMedia[] } {
  const found: Array<{ index: number; media: OutgoingMedia }> = [];

  for (const match of text.matchAll(MEDIA_MARKER_RE)) {
    const rawPath = match[1] ?? '';
    found.push({
      index: match.index ?? 0,
      media: { path: normalizeMediaPath(rawPath), raw: match[0] },
    });
  }

  for (const match of text.matchAll(MD_IMAGE_RE)) {
    const rawPath = match[1] ?? '';
    const p = normalizeMediaPath(rawPath);
    // Markdown 图片只在确为本地文件时升级为媒体；HTTP 链接保持原样（stripMarkdown 会去掉语法）
    if (isLocalFile(p)) {
      found.push({ index: match.index ?? 0, media: { path: p, raw: match[0] } });
    }
  }

  found.sort((a, b) => a.index - b.index);

  let rest = text;
  for (const item of found) rest = rest.replace(item.media.raw, '');

  return { text: rest, media: found.map((f) => f.media) };
}

function makeReplier(account: AccountData) {
  return async (ctx: TurnCtx, text: string): Promise<void> => {
    const { text: body, media } = extractOutgoingMedia(text);

    const plain = stripMarkdown(body).trim();
    if (plain) {
      for (const chunk of chunkText(plain)) {
        await sendMessage(account.token, ctx.userId, chunk, ctx.contextToken, account.baseUrl, ctx.refMsgId);
      }
    }

    for (const item of media) {
      if (!isLocalFile(item.path)) {
        log(`[media] 跳过不存在的文件: ${item.path}`);
        await sendMessage(
          account.token,
          ctx.userId,
          `（媒体文件不存在，未发送: ${item.path}）`,
          ctx.contextToken,
          account.baseUrl,
          ctx.refMsgId,
        ).catch(() => {});
        continue;
      }
      try {
        await uploadMedia({
          token: account.token,
          toUser: ctx.userId,
          contextToken: ctx.contextToken,
          filePath: item.path,
          baseUrl: account.baseUrl,
        });
        log(`[media] 已发送: ${item.path}`);
      } catch (err) {
        log(`[media] 发送失败: ${item.path}: ${String(err)}`);
        await sendMessage(
          account.token,
          ctx.userId,
          `（媒体发送失败: ${item.path}）`,
          ctx.contextToken,
          account.baseUrl,
          ctx.refMsgId,
        ).catch(() => {});
      }
    }
  };
}

// ─── iLink 轮询循环 ───────────────────────────────────

async function pollLoop(
  account: AccountData,
  runner: ClaudeRunner,
  abort: AbortSignal,
  typingTicketCache: Map<string, string>,
): Promise<void> {
  let buf = loadSyncBuf();
  let consecutiveFailures = 0;
  let sessionRetries = 0;
  let retryDelay = INITIAL_RETRY_DELAY_MS;
  let nextTimeoutMs: number | undefined;

  while (!abort.aborted) {
    try {
      const resp = await getUpdates(account.token, buf, account.baseUrl, nextTimeoutMs);

      if (resp.longpolling_timeout_ms) nextTimeoutMs = resp.longpolling_timeout_ms;

      if ((resp.ret !== undefined && resp.ret !== 0) || (resp.errcode !== undefined && resp.errcode !== 0)) {
        const errcode = resp.errcode ?? resp.ret ?? 0;
        if (errcode === SESSION_EXPIRED_ERRCODE) {
          sessionRetries++;
          log(`[ilink] session 过期 (${sessionRetries}/${MAX_SESSION_RETRIES})，请运行 cc-wechat login 重新扫码`);
          if (sessionRetries >= MAX_SESSION_RETRIES) {
            log('[ilink] 重试耗尽，等待重新登录后重启 daemon');
            await sleep(SESSION_PAUSE_MS, abort);
            sessionRetries = 0;
            continue;
          }
          await sleep(SESSION_PAUSE_MS, abort);
          continue;
        }
        consecutiveFailures++;
        log(`[ilink] API 错误 errcode=${errcode} errmsg=${resp.errmsg ?? ''} (${consecutiveFailures}/${MAX_CONSECUTIVE_FAILURES})`);
        if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
          await sleep(SESSION_PAUSE_MS, abort);
          consecutiveFailures = 0;
        } else {
          await sleep(retryDelay, abort);
          retryDelay = Math.min(retryDelay * 2, MAX_RETRY_DELAY_MS);
        }
        continue;
      }

      consecutiveFailures = 0;
      retryDelay = INITIAL_RETRY_DELAY_MS;
      if (resp.get_updates_buf) {
        buf = resp.get_updates_buf;
        saveSyncBuf(buf);
      }

      for (const msg of resp.msgs ?? []) {
        if (msg.message_type !== 1) continue;
        const fromUser = msg.from_user_id ?? '';
        const contextToken = msg.context_token ?? '';
        const text = await extractText(msg);
        const ctx: TurnCtx = {
          userId: fromUser,
          contextToken,
          refMsgId: msg.message_id !== undefined ? String(msg.message_id) : undefined,
        };

        try {
          const config = await getConfig(account.token, fromUser, contextToken, account.baseUrl);
          if (config.typing_ticket) typingTicketCache.set(fromUser, config.typing_ticket);
        } catch { /* ignore */ }
        try {
          const ticket = typingTicketCache.get(fromUser);
          if (ticket) await sendTyping(account.token, fromUser, ticket, 1, account.baseUrl);
        } catch { /* ignore */ }

        log(`[wechat] ← ${fromUser}: ${text.slice(0, 80).replace(/\n/g, ' ')}`);
        runner.send(text, ctx);
      }
    } catch (err) {
      if (abort.aborted) return;
      consecutiveFailures++;
      log(`[ilink] 网络错误: ${String(err)} (${consecutiveFailures}/${MAX_CONSECUTIVE_FAILURES})`);
      if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
        await sleep(SESSION_PAUSE_MS, abort);
        consecutiveFailures = 0;
      } else {
        await sleep(retryDelay, abort);
        retryDelay = Math.min(retryDelay * 2, MAX_RETRY_DELAY_MS);
      }
    }
  }
}

// ─── 入口 ────────────────────────────────────────────

function parseArgs(argv: string[]): { cwd: string; claudeBin: string } {
  let cwd = process.cwd();
  let claudeBin = process.env.CC_WECHAT_CLAUDE || 'claude';
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--cwd' && argv[i + 1]) cwd = argv[++i]!;
    else if (a === '--claude' && argv[i + 1]) claudeBin = argv[++i]!;
  }
  return { cwd, claudeBin };
}

async function main(): Promise<void> {
  const { cwd, claudeBin } = parseArgs(process.argv.slice(2));
  const account = getActiveAccount();
  if (!account) {
    console.error('尚未登录。请先运行: cc-wechat login');
    process.exit(1);
  }

  const abort = new AbortController();
  const typingTicketCache = new Map<string, string>();
  const reply = makeReplier(account);
  const runner = new ClaudeRunner(cwd, claudeBin, async (ctx, text) => {
    await reply(ctx, text);
    try {
      const ticket = typingTicketCache.get(ctx.userId);
      if (ticket) await sendTyping(account.token, ctx.userId, ticket, 0, account.baseUrl);
    } catch { /* ignore */ }
  });

  const shutdown = (sig: string): void => {
    log(`[daemon] ${sig}, shutting down`);
    abort.abort();
    runner.stop();
    setTimeout(() => process.exit(0), 500);
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  runner.start();
  log(`[daemon] started, bot=${account.botId}, cwd=${cwd}`);

  try {
    await pollLoop(account, runner, abort.signal, typingTicketCache);
  } catch (err) {
    if (!abort.signal.aborted) log('[daemon] poll loop crashed:', String(err));
  }
}

main().catch((err) => {
  console.error('[daemon] fatal:', String(err));
  process.exit(1);
});
