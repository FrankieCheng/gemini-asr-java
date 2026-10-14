/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.speech.live;

import com.google.auth.oauth2.GoogleCredentials;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import okhttp3.OkHttpClient;

/**
 * 面向超过 10 分钟实时语音的转写器：在 {@code gemini-3.5-transcribe-live} 之上做客户端会话轮换。
 *
 * <h2>用法</h2>
 * <pre>
 *   try (RotatingLiveTranscriber t = new RotatingLiveTranscriber(cfg, listener)) {
 *     t.start();
 *     while (source.hasAudio()) {
 *       t.feed(source.read());   // 16kHz mono 16-bit PCM，任意长度，按实时节奏推入
 *     }
 *     t.finish();                // 刷出尾部结果并关闭所有会话
 *   }
 * </pre>
 *
 * <h2>行为</h2>
 * <ol>
 *   <li><b>软落定（主机制）</b>：当前段 ≥{@code flushAfterMs}（默认 8s）且客户端检测到停顿时，在同一会话发送
 *       audioStreamEnd，服务端约 0.5s 内输出 Final，随后继续推流、不重连。实测：Final 最大间隔 42s→19s，
 *       服务端自动分段处的丢词基本消除（WER 4.84%→3.98%），630s 内轮换次数 5→1。</li>
 *   <li><b>轮换触发</b>（任一）：会话运行满 {@code rotateAfterMs}；收到 GoAway；看门狗
 *       （有未落定 interim 且超过 {@code stallMs} 无 Final，作为软落定失效时的兜底）。</li>
 *   <li><b>先建后断</b>：后台建立新会话，期间音频继续发给旧会话；新会话 setupComplete 后再切换。</li>
 *   <li><b>静音切点</b>：客户端能量 VAD 检测到 ≥{@code silenceMs} 静音时切换；{@code maxCutWaitMs} 内找不到
 *       则强制切换，并把最近 {@code overlapMs} 音频重放给新会话，发出 Final 时按词去重。</li>
 *   <li><b>旧会话收尾</b>：audioStreamEnd → 等未落定 interim 变成 Final（≤{@code drainMaxMs}）→
 *       仍未落定则用累计 interim 兜底 → close。不等待 turnComplete（该模型从不发送）。</li>
 *   <li><b>断线恢复</b>：连接被意外关闭或发送失败时，立即重建会话，并从上一个 Final 结束处重放音频
 *       （≤{@code maxReplayMs}），重复文本按词去重。</li>
 *   <li><b>有序输出</b>：Final 按会话顺序发出，旧会话收尾完成前，新会话的 Final 先缓存；
 *       时间戳统一换算为全程流时间。</li>
 * </ol>
 *
 * <p>线程模型：{@link #feed} 只入队，不阻塞；一个内部 worker 线程负责发送、轮换与恢复；
 * 回调在内部线程上执行。
 */
public final class RotatingLiveTranscriber implements AutoCloseable {

  private static final int FRAME_MS = 100;
  private static final int FRAME_BYTES = LiveTranscriptionConfig.SAMPLE_RATE * 2 * FRAME_MS / 1000;
  private static final int MAX_CONNECT_ATTEMPTS = 5;

  private enum State { ACTIVE, DRAINING, DONE }

  /** 每个会话的客户端状态。所有字段在 {@code lock} 下读写（ws 除外）。 */
  private final class Sess {
    final LiveWsSession ws;
    final int index;
    long audioStartStreamMs = -1;
    long audioEndStreamMs = -1;
    boolean overlapWithPrev = false;
    State state = State.ACTIVE;
    String endReason = "-";
    boolean goAway = false;
    long goAwayWallMs = -1;
    // 当前未落定段
    String pendingText = "";
    long pendingStartOffset = -1;
    long pendingEndOffset = -1;
    // 统计 / 看门狗
    long lastFinalStreamMs = -1;
    long lastFinalEndGlobalMs = -1;
    long replayFromMs = -1;
    long lastFlushStreamMs = -1;
    int flushes = 0;
    boolean offsetsMissingReported = false;
    long lastFinalWallMs = 0;
    int finals = 0;
    int fallbacks = 0;
    // 有序输出
    final Deque<TranscriptEvent> unemitted = new ArrayDeque<>();
    final List<String> emittedTexts = new ArrayList<>();
    boolean emittedAny = false;

    Sess(LiveWsSession ws) {
      this.ws = ws;
      this.index = ws.index;
    }

    long global(long offsetMs) {
      return offsetMs < 0 || audioStartStreamMs < 0 ? -1 : audioStartStreamMs + offsetMs;
    }
  }

  /** 会话统计（只读快照）。 */
  public static final class SessionInfo {
    public final int index;
    public final long audioStartMs;
    public final long audioEndMs;
    public final String endReason;
    public final boolean goAway;
    public final int finals;
    public final int interimFallbacks;
    public final int softFlushes;
    public final String emittedText;

    SessionInfo(Sess s) {
      index = s.index;
      audioStartMs = s.audioStartStreamMs;
      audioEndMs = s.audioEndStreamMs;
      endReason = s.endReason;
      goAway = s.goAway;
      finals = s.finals;
      interimFallbacks = s.fallbacks;
      softFlushes = s.flushes;
      emittedText = String.join(" ", s.emittedTexts);
    }
  }

  private final LiveTranscriptionConfig cfg;
  private final TranscriptListener listener;
  private final GoogleCredentials credentials;
  private final OkHttpClient http;
  private final Object lock = new Object();

  private final BlockingQueue<byte[]> inbox = new LinkedBlockingQueue<>();
  private byte[] partial = new byte[0];
  private final AtomicLong streamPosMs = new AtomicLong(0);
  private final List<Sess> sessions = new ArrayList<>(); // 按切换顺序
  private final List<CompletableFuture<Void>> drains = Collections.synchronizedList(new ArrayList<>());
  private final Deque<long[]> framePos = new ArrayDeque<>(); // [streamStartMs] 与 frames 对齐
  private final Deque<byte[]> frames = new ArrayDeque<>(); // 最近 maxReplayMs 的音频
  private final Deque<String> emittedTail = new ArrayDeque<>(); // 最近已发出的词（去重用）

  private volatile boolean feeding = true;
  private volatile boolean abortRequested = false;
  private Thread worker;
  private int nextIndex = 1;
  private volatile Sess current;

  public RotatingLiveTranscriber(LiveTranscriptionConfig cfg, TranscriptListener listener)
      throws java.io.IOException {
    this.cfg = cfg;
    this.listener = listener;
    this.credentials =
        GoogleCredentials.getApplicationDefault()
            .createScoped("https://www.googleapis.com/auth/cloud-platform");
    this.http = new OkHttpClient.Builder().readTimeout(0, TimeUnit.MILLISECONDS).build();
  }

  // =============================================================================================
  // 公共 API
  // =============================================================================================

  /** 建立第一个会话并启动 worker。 */
  public void start() throws Exception {
    Sess first = openWithRetry();
    synchronized (lock) {
      first.audioStartStreamMs = 0;
      sessions.add(first);
      current = first;
    }
    worker = new Thread(this::workerLoop, "rotating-live-transcriber");
    worker.setDaemon(true);
    worker.start();
  }

  /** 推入 16kHz/mono/16-bit PCM（任意长度）。应按实时节奏调用。 */
  public void feed(byte[] pcm) {
    if (!feeding) {
      throw new IllegalStateException("finish() 之后不能再 feed");
    }
    byte[] buf = new byte[partial.length + pcm.length];
    System.arraycopy(partial, 0, buf, 0, partial.length);
    System.arraycopy(pcm, 0, buf, partial.length, pcm.length);
    int off = 0;
    while (buf.length - off >= FRAME_BYTES) {
      inbox.add(Arrays.copyOfRange(buf, off, off + FRAME_BYTES));
      off += FRAME_BYTES;
    }
    partial = Arrays.copyOfRange(buf, off, buf.length);
  }

  /** 音频结束：刷出剩余数据，收尾所有会话，并等待全部 Final 发出。 */
  public void finish() throws InterruptedException {
    if (partial.length > 0) {
      inbox.add(partial);
      partial = new byte[0];
    }
    feeding = false;
    if (worker != null) {
      worker.join(cfg.drainMaxMs + 60_000 + inbox.size() * (long) FRAME_MS);
    }
  }

  /** 会话统计快照。 */
  public List<SessionInfo> sessionInfos() {
    synchronized (lock) {
      List<SessionInfo> out = new ArrayList<>();
      for (Sess s : sessions) {
        out.add(new SessionInfo(s));
      }
      return out;
    }
  }

  /** 测试用：立即掐断当前会话的连接，模拟网络故障。 */
  public void abortCurrentSessionForTest() {
    abortRequested = true;
  }

  @Override
  public void close() {
    feeding = false;
    if (worker != null) {
      worker.interrupt();
    }
    synchronized (lock) {
      for (Sess s : sessions) {
        if (s.state != State.DONE) {
          s.ws.close();
        }
      }
    }
    http.dispatcher().executorService().shutdown();
    http.connectionPool().evictAll();
  }

  // =============================================================================================
  // Worker：发送 / 轮换 / 恢复
  // =============================================================================================

  private void workerLoop() {
    CompletableFuture<Sess> pending = null;
    long pendingSinceWall = -1;
    long nextAttemptWall = 0;
    int silentFrames = 0;
    final int silenceFramesNeeded = (int) Math.max(1, cfg.silenceMs / FRAME_MS);
    long lastFrameWallMs = System.currentTimeMillis();
    try {
      byte[] carry = null; // 发送失败的帧：恢复后优先重发，保证音频顺序
      while (true) {
        byte[] frame = carry != null ? carry : inbox.poll(50, TimeUnit.MILLISECONDS);
        carry = null;
        boolean idle = frame == null;
        if (idle && !feeding && inbox.isEmpty()) {
          break;
        }
        Sess cur = current;

        if (abortRequested) {
          abortRequested = false;
          event("测试：掐断 S%d 连接", cur.index);
          cur.ws.abortForTest();
          Thread.sleep(200); // 等待 OkHttp 回调把会话标记为 dead
        }

        // 1) 断线恢复（服务端关闭 / 网络故障 / 发送失败）
        if (cur.ws.isDead()) {
          if (pending != null) {
            pending.thenAccept(s -> s.ws.close());
            pending = null;
          }
          cur = recover(cur);
        }

        // 2) 发送音频
        if (!idle) {
          long pos = streamPosMs.get();
          if (!cur.ws.sendAudio(frame)) {
            carry = frame;
            continue;
          }
          remember(pos, frame);
          streamPosMs.addAndGet(frame.length * 1000L / (LiveTranscriptionConfig.SAMPLE_RATE * 2));
          int level = rms(frame);
          silentFrames = level < silenceThreshold(level) ? silentFrames + 1 : 0;
          if (silentFrames == 0) {
            flushedThisPause = false;
          }
          lastFrameWallMs = System.currentTimeMillis();

          // 2.5) 软落定：停顿时，若本段已足够长且有未落定语音，在同一会话发 audioStreamEnd。
          // 本段越长，对停顿长度的要求越低（300ms → 200ms → 100ms），避免连续语音/嘈杂环境下迟迟不落定。
          if (cfg.flushAfterMs > 0 && silentFrames > 0 && !flushedThisPause) {
            boolean doFlush;
            long now = streamPosMs.get();
            synchronized (lock) {
              long segStart = Math.max(cur.audioStartStreamMs,
                  Math.max(cur.lastFlushStreamMs, cur.lastFinalStreamMs));
              long seg = now - segStart;
              int needed = seg < 2 * cfg.flushAfterMs ? silenceFramesNeeded
                  : seg < 3 * cfg.flushAfterMs ? Math.max(1, silenceFramesNeeded - 1) : 1;
              doFlush = !cur.pendingText.isBlank() && seg >= cfg.flushAfterMs
                  && silentFrames >= needed;
              if (doFlush) {
                cur.lastFlushStreamMs = now;
                cur.flushes++;
              }
            }
            if (doFlush) {
              flushedThisPause = true;
              cur.ws.sendAudioStreamEnd();
            }
          }
        } else if (System.currentTimeMillis() - lastFrameWallMs >= cfg.silenceMs + FRAME_MS) {
          // 真正的输入中断（音频源暂停）才等同静音；实时节奏下帧间的短暂空轮询不算
          silentFrames = silenceFramesNeeded;
        }

        // 3) 轮换触发
        if (pending == null && System.currentTimeMillis() >= nextAttemptWall) {
          String reason = rotationReason(cur);
          if (reason != null) {
            event("触发轮换 S%d（%s）→ 后台建立 S%d", cur.index, reason, nextIndex);
            synchronized (lock) {
              cur.endReason = reason;
            }
            pending = CompletableFuture.supplyAsync(this::openOrThrow);
            pendingSinceWall = System.currentTimeMillis();
          }
        }

        // 4) 新会话就绪 → 选切点
        if (pending != null && pending.isDone()) {
          Sess next;
          try {
            next = pending.join();
          } catch (Exception e) {
            event("新会话建立失败，1s 后重试：%s", rootMessage(e));
            pending = null;
            nextAttemptWall = System.currentTimeMillis() + 1000;
            continue;
          }
          long waited = System.currentTimeMillis() - pendingSinceWall;
          boolean silence = silentFrames >= silenceFramesNeeded;
          boolean goAwayUrgent;
          synchronized (lock) {
            goAwayUrgent = cur.goAway && System.currentTimeMillis() - cur.goAwayWallMs > 15_000;
          }
          if (silence || waited >= cfg.maxCutWaitMs || goAwayUrgent) {
            cutOver(cur, next, silence ? "silence" : "forced");
            pending = null;
          }
        }
      }

      // 音频结束：收尾当前会话
      Sess cur = current;
      if (pending != null) {
        pending.thenAccept(s -> s.ws.close());
      }
      synchronized (lock) {
        if (cur.endReason.equals("-")) {
          cur.endReason = "eof";
        }
      }
      startDrain(cur);
      CompletableFuture.allOf(drains.toArray(new CompletableFuture[0]))
          .get(cfg.drainMaxMs + 30_000, TimeUnit.MILLISECONDS);
      flush();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      listener.onError(e);
    }
  }

  private String rotationReason(Sess cur) {
    synchronized (lock) {
      if (cur.goAway) {
        return "GoAway";
      }
      long age = System.currentTimeMillis() - cur.ws.openedWallMs;
      if (age >= cfg.rotateAfterMs) {
        return String.format(Locale.ROOT, "到龄 %ds", age / 1000);
      }
      if (cfg.stallMs > 0 && !cur.pendingText.isBlank()) {
        long since = streamPosMs.get() - Math.max(cur.lastFinalStreamMs, cur.audioStartStreamMs);
        if (since >= cfg.stallMs) {
          return String.format(Locale.ROOT, "看门狗：%ds 无 Final", since / 1000);
        }
      }
      return null;
    }
  }

  /** 切换：新会话接管后续音频，旧会话异步收尾。 */
  private void cutOver(Sess old, Sess next, String how) {
    long pos = streamPosMs.get();
    if ("forced".equals(how)) {
      long from = pos - cfg.overlapMs;
      long start = replayFrom(next, from);
      synchronized (lock) {
        next.audioStartStreamMs = start;
        next.overlapWithPrev = true;
      }
    } else {
      synchronized (lock) {
        next.audioStartStreamMs = pos;
      }
    }
    synchronized (lock) {
      old.audioEndStreamMs = pos;
      sessions.add(next);
      current = next;
    }
    event("切换完成 S%d → S%d @ %.1fs（%s）", old.index, next.index, pos / 1000.0, how);
    startDrain(old);
  }

  /** 断线恢复：重建会话并从上一个 Final 结束处重放音频。 */
  private Sess recover(Sess dead) throws Exception {
    long pos = streamPosMs.get();
    long from;
    synchronized (lock) {
      from = dead.replayFromMs >= 0 ? dead.replayFromMs : dead.audioStartStreamMs;
      dead.audioEndStreamMs = pos;
      dead.endReason = "断线";
      // 未落定的 interim 由重放音频重新转写，这里丢弃，避免重复
      dead.pendingText = "";
      dead.state = State.DONE;
    }
    event("S%d 连接已断开，重建会话并从 %.1fs 重放 %.1fs 音频", dead.index, from / 1000.0,
        (pos - from) / 1000.0);
    Sess next = openWithRetry();
    long start = replayFrom(next, from);
    if (start > from) {
      event("警告：重放缓冲不足，%.1fs~%.1fs 的音频无法补转写", from / 1000.0, start / 1000.0);
    }
    synchronized (lock) {
      next.audioStartStreamMs = start;
      next.overlapWithPrev = true;
      sessions.add(next);
      current = next;
    }
    flush();
    return next;
  }

  /** 把缓冲中 ≥fromMs 的音频立即发给新会话，返回实际重放起点。 */
  private long replayFrom(Sess next, long fromMs) {
    long start = -1;
    var itPos = framePos.iterator();
    var itFrame = frames.iterator();
    while (itPos.hasNext()) {
      long p = itPos.next()[0];
      byte[] f = itFrame.next();
      if (p + FRAME_MS <= fromMs) {
        continue;
      }
      if (start < 0) {
        start = p;
      }
      next.ws.sendAudio(f);
    }
    return start < 0 ? streamPosMs.get() : start;
  }

  private void remember(long pos, byte[] frame) {
    framePos.addLast(new long[] {pos});
    frames.addLast(frame);
    long keep = Math.max(cfg.maxReplayMs, cfg.overlapMs);
    while (!framePos.isEmpty() && pos - framePos.peekFirst()[0] > keep) {
      framePos.removeFirst();
      frames.removeFirst();
    }
  }

  // =============================================================================================
  // 会话建立 / 收尾
  // =============================================================================================

  private Sess openOrThrow() {
    try {
      return open();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Sess openWithRetry() throws Exception {
    Exception last = null;
    for (int attempt = 1; attempt <= MAX_CONNECT_ATTEMPTS; attempt++) {
      try {
        return open();
      } catch (Exception e) {
        last = e;
        long backoff = Math.min(8000, 500L << attempt);
        event("建连失败（第 %d 次）：%s，%dms 后重试", attempt, rootMessage(e), backoff);
        Thread.sleep(backoff);
      }
    }
    listener.onError(last);
    throw last;
  }

  private Sess open() throws Exception {
    int idx;
    synchronized (lock) {
      idx = nextIndex++;
    }
    String token;
    synchronized (credentials) {
      credentials.refreshIfExpired();
      token = credentials.getAccessToken().getTokenValue();
    }
    Sess[] holder = new Sess[1];
    LiveWsSession ws = LiveWsSession.open(idx, http, cfg, token, new SessionCallback(holder));
    Sess s = new Sess(ws);
    holder[0] = s;
    event("S%d 已建立", idx);
    return s;
  }

  private void startDrain(Sess s) {
    synchronized (lock) {
      if (s.state != State.ACTIVE) {
        return;
      }
      s.state = State.DRAINING;
      if (s.audioEndStreamMs < 0) {
        s.audioEndStreamMs = streamPosMs.get();
      }
    }
    drains.add(CompletableFuture.runAsync(() -> drain(s)));
  }

  private void drain(Sess s) {
    long t0 = System.currentTimeMillis();
    try {
      s.ws.sendAudioStreamEnd();
      while (true) {
        Thread.sleep(250);
        long now = System.currentTimeMillis();
        boolean pendingInterim;
        long lastFinal;
        synchronized (lock) {
          pendingInterim = !s.pendingText.isBlank();
          lastFinal = s.lastFinalWallMs;
        }
        long elapsed = now - t0;
        boolean quiet = now - Math.max(lastFinal, t0) >= cfg.drainQuietMs;
        if (s.ws.isDead() || elapsed >= cfg.drainMaxMs || (!pendingInterim && quiet)) {
          break;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      synchronized (lock) {
        if (!s.pendingText.isBlank()) {
          s.unemitted.add(new TranscriptEvent(TranscriptEvent.Type.FINAL,
              TranscriptEvent.Source.INTERIM_FALLBACK, s.pendingText.trim(),
              s.global(s.pendingStartOffset), s.global(s.pendingEndOffset), s.index));
          s.fallbacks++;
          s.pendingText = "";
          event("S%d 尾部 interim 未落定，使用 interim 兜底", s.index);
        }
        s.state = State.DONE;
      }
      s.ws.close();
      event("S%d 已收尾（%.1fs~%.1fs，Final=%d，兜底=%d，原因=%s）", s.index,
          s.audioStartStreamMs / 1000.0, s.audioEndStreamMs / 1000.0, s.finals, s.fallbacks,
          s.endReason);
      flush();
    }
  }

  /** 服务端事件 → 客户端状态。 */
  private final class SessionCallback implements LiveWsSession.Callback {
    private final Sess[] holder;

    SessionCallback(Sess[] holder) {
      this.holder = holder;
    }

    private Sess sess() {
      return holder[0];
    }

    @Override
    public void onSetupComplete(LiveWsSession ws) {}

    @Override
    public void onInterim(LiveWsSession ws, String text, long startOffset, long endOffset) {
      Sess s = sess();
      if (s == null) {
        return;
      }
      TranscriptEvent ev;
      boolean warn = false;
      synchronized (lock) {
        if (s.state == State.DONE) {
          return;
        }
        if (s.pendingText.isBlank() || startOffset != s.pendingStartOffset) {
          s.pendingStartOffset = startOffset;
        }
        s.pendingText = text;
        s.pendingEndOffset = endOffset;
        if (startOffset < 0 && !s.offsetsMissingReported) {
          s.offsetsMissingReported = true;
          warn = true;
        }
        ev = new TranscriptEvent(TranscriptEvent.Type.INTERIM, TranscriptEvent.Source.SERVER, text,
            s.global(startOffset), s.global(endOffset), s.index);
      }
      if (warn) {
        event("S%d 服务端 interim 未携带时间戳，Final 时间戳改为客户端估算", s.index);
      }
      listener.onInterim(ev);
    }

    @Override
    public void onFinal(LiveWsSession ws, String text) {
      Sess s = sess();
      if (s == null) {
        return;
      }
      synchronized (lock) {
        if (s.state == State.DONE) {
          return;
        }
        long pos = streamPosMs.get();
        long start = s.global(s.pendingStartOffset);
        long end = s.global(s.pendingEndOffset);
        boolean exact = end >= 0;
        if (start < 0) {
          // 估算：本段从上一个 Final 结束处（或会话起点）开始
          start = Math.max(s.lastFinalEndGlobalMs, s.audioStartStreamMs);
        }
        if (end < 0) {
          end = pos; // 估算上界：收到 Final 时已发送到的位置
        }
        s.unemitted.add(new TranscriptEvent(TranscriptEvent.Type.FINAL,
            TranscriptEvent.Source.SERVER, text, start, end, s.index));
        s.finals++;
        s.pendingText = "";
        s.pendingStartOffset = -1;
        s.lastFinalStreamMs = pos;
        s.lastFinalWallMs = System.currentTimeMillis();
        s.lastFinalEndGlobalMs = end;
        // 断线重放起点：时间戳精确时从段尾开始；估算时回退 3s，多出的部分靠去重消除
        s.replayFromMs = exact ? end : Math.max(s.audioStartStreamMs, end - 3000);
      }
      flush();
    }

    @Override
    public void onGoAway(LiveWsSession ws, long timeLeftMs) {
      Sess s = sess();
      if (s == null) {
        return;
      }
      synchronized (lock) {
        s.goAway = true;
        s.goAwayWallMs = System.currentTimeMillis();
      }
      event("S%d 收到 GoAway（timeLeft=%dms）", s.index, timeLeftMs);
    }

    @Override
    public void onClosed(LiveWsSession ws, boolean initiatedByClient, String reason) {
      if (!initiatedByClient) {
        event("S%d 连接被关闭：%s", ws.index, reason);
      }
    }
  }

  // =============================================================================================
  // 有序输出与去重
  // =============================================================================================

  /** 按会话顺序发出 Final；前一个会话收尾完成前，后续会话的 Final 先缓存。 */
  private void flush() {
    List<TranscriptEvent> out = new ArrayList<>();
    synchronized (lock) {
      for (Sess s : sessions) {
        while (!s.unemitted.isEmpty()) {
          TranscriptEvent ev = s.unemitted.pollFirst();
          if (s.overlapWithPrev && !s.emittedAny) {
            String trimmed = dropOverlap(emittedTail.toArray(new String[0]), ev.text);
            if (trimmed.isBlank()) {
              continue;
            }
            ev = ev.withText(trimmed);
          }
          s.emittedAny = true;
          s.emittedTexts.add(ev.text);
          for (String w : ev.text.split("\\s+")) {
            emittedTail.addLast(w);
            if (emittedTail.size() > 60) {
              emittedTail.removeFirst();
            }
          }
          out.add(ev);
        }
        if (s.state != State.DONE) {
          break;
        }
      }
    }
    for (TranscriptEvent ev : out) {
      listener.onFinal(ev);
    }
  }

  /**
   * 去掉 text 中与已发出文本重复的开头部分。两种情况：
   * <ol>
   *   <li>重放音频比已发出文本更早开始：在 text 前 200 词中查找已发出文本的最后 k（6→4）个词，
   *       找到则丢弃到该位置为止；</li>
   *   <li>text 开头恰好是已发出文本的结尾：前缀/后缀匹配（40→2 词）。</li>
   * </ol>
   */
  static String dropOverlap(String[] tailWords, String text) {
    // 按空白和连字符切分后比较（服务端对同一短语常有 "500-millisecond" / "500 millisecond" 两种写法）
    String[] tail = String.join(" ", tailWords).trim().split("[\\s\\-]+");
    String[] words = text.trim().split("[\\s\\-]+");
    String[] nt = new String[tail.length];
    for (int i = 0; i < tail.length; i++) {
      nt[i] = norm(tail[i]);
    }
    String[] nw = new String[words.length];
    for (int i = 0; i < words.length; i++) {
      nw[i] = norm(words[i]);
    }
    if (nw.length == 0 || nt.length == 0) {
      return text;
    }
    // 1) 锚点搜索（允许忽略已发出文本最后 0~2 个词：服务端在段尾常有半个词或写法差异）
    for (int skip = 0; skip <= 2; skip++) {
      for (int k = Math.min(6, nt.length - skip); k >= 4; k--) {
        int limit = Math.min(200, nw.length) - k;
        for (int i = 0; i <= limit; i++) {
          boolean match = true;
          for (int j = 0; j < k; j++) {
            if (!nt[nt.length - skip - k + j].equals(nw[i + j])) {
              match = false;
              break;
            }
          }
          if (match) {
            int from = Math.min(words.length, i + k + skip);
            return String.join(" ", Arrays.copyOfRange(words, from, words.length));
          }
        }
      }
    }
    // 2) 前缀/后缀匹配
    for (int k = Math.min(40, Math.min(nt.length, nw.length)); k >= 2; k--) {
      boolean match = true;
      for (int j = 0; j < k; j++) {
        if (!nt[nt.length - k + j].equals(nw[j])) {
          match = false;
          break;
        }
      }
      if (match) {
        return String.join(" ", Arrays.copyOfRange(words, k, words.length));
      }
    }
    return text;
  }

  private static String norm(String w) {
    return w.toLowerCase(Locale.ROOT).replaceAll("[^\\p{L}\\p{N}]", "");
  }

  // =============================================================================================
  // 工具
  // =============================================================================================

  // 自适应静音阈值（仅 worker 线程访问）：最近 10s 帧能量的噪声底（p10）+ 15% × (语音电平 p80 − 噪声底)，
  // 且不低于 cfg.silenceRms。实测 TED 现场录音噪声底约 250~400，固定阈值 300 时停顿检出率不足 1/3。
  private final int[] levelRing = new int[100];
  private int levelCount = 0;
  private int cachedThreshold = -1;
  private boolean flushedThisPause = false;

  private int silenceThreshold(int level) {
    levelRing[levelCount % levelRing.length] = level;
    levelCount++;
    if (!cfg.adaptiveSilence) {
      return cfg.silenceRms;
    }
    if (cachedThreshold < 0 || levelCount % 10 == 0) {
      int n = Math.min(levelCount, levelRing.length);
      int[] s = Arrays.copyOf(levelRing, n);
      Arrays.sort(s);
      int floor = s[(int) (0.10 * (n - 1))];
      int speech = s[(int) (0.80 * (n - 1))];
      cachedThreshold = Math.max(cfg.silenceRms, floor + (int) (0.15 * Math.max(0, speech - floor)));
    }
    return cachedThreshold;
  }

  private static int rms(byte[] pcm) {
    ByteBuffer bb = ByteBuffer.wrap(pcm).order(ByteOrder.LITTLE_ENDIAN);
    long sum = 0;
    int n = pcm.length / 2;
    for (int i = 0; i < n; i++) {
      int v = bb.getShort();
      sum += (long) v * v;
    }
    return (int) Math.sqrt((double) sum / Math.max(1, n));
  }

  private static String rootMessage(Throwable e) {
    Throwable t = e;
    while (t.getCause() != null) {
      t = t.getCause();
    }
    return t.toString();
  }

  private void event(String fmt, Object... args) {
    listener.onSessionEvent(String.format(Locale.ROOT, "[stream %.1fs] ", streamPosMs.get() / 1000.0)
        + String.format(Locale.ROOT, fmt, args));
  }
}
