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
    final boolean manualVad;
    final long flushAfterMs;
    long audioStartStreamMs = -1;
    long audioEndStreamMs = -1;
    boolean overlapWithPrev = false;
    State state = State.ACTIVE;
    String endReason = "-";
    boolean goAway = false;
    long goAwayWallMs = -1;
    // 服务端话轮状态（对标官方 Python _gemini_stt.py _TranscriptionState）
    boolean speaking = false;
    boolean turnHadFinal = false;
    long lastServerMessageWallMs = System.currentTimeMillis();
    boolean generationCompletedDuringDrain = false;
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
    boolean awaitingSoftFlushFinal = false;
    // manualVad（远场 disabled=true）会话内大窗口翻转状态：
    // 发送 activityEnd 后需等服务端确认结束（speech_active 置回 false）再发 activityStart，防止被服务端丢弃
    boolean awaitingManualRestart = false;
    boolean manualTurnClosed = false;
    long manualRestartFromStreamMs = -1;
    long manualRestartSentWallMs = -1;
    boolean offsetsMissingReported = false;
    long lastFinalWallMs = 0;
    int finals = 0;
    int serverFinals = 0;
    int fallbacks = 0;
    // 有序输出
    final Deque<TranscriptEvent> unemitted = new ArrayDeque<>();
    final List<String> emittedTexts = new ArrayList<>();
    boolean emittedAny = false;

    Sess(LiveWsSession ws, boolean manualVad, long flushAfterMs) {
      this.ws = ws;
      this.index = ws.index;
      this.manualVad = manualVad;
      this.flushAfterMs = flushAfterMs;
    }

    /** 对应官方 Python {@code _TranscriptionState.pending}：是否有正在进行或尚未定稿的话轮。 */
    boolean pending() {
      return speaking || !pendingText.isBlank();
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
    public final String state;
    public final String endReason;
    public final boolean goAway;
    public final int finals;
    public final int interimFallbacks;
    public final int softFlushes;
    public final String emittedText;

    SessionInfo(Sess s) {
      this(
          s.index,
          s.audioStartStreamMs,
          s.audioEndStreamMs,
          s.state.name(),
          s.endReason,
          s.goAway,
          s.finals,
          s.fallbacks,
          s.flushes,
          String.join(" ", s.emittedTexts));
    }

    public SessionInfo(
        int index,
        long audioStartMs,
        long audioEndMs,
        String state,
        String endReason,
        boolean goAway,
        int finals,
        int interimFallbacks,
        int softFlushes,
        String emittedText) {
      this.index = index;
      this.audioStartMs = audioStartMs;
      this.audioEndMs = audioEndMs;
      this.state = state;
      this.endReason = endReason;
      this.goAway = goAway;
      this.finals = finals;
      this.interimFallbacks = interimFallbacks;
      this.softFlushes = softFlushes;
      this.emittedText = emittedText;
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
  private volatile boolean forceRotateRequested = false;
  private volatile boolean forceFlushRequested = false;
  private volatile boolean runtimeManualVad;
  private volatile long runtimeFlushAfterMs;
  private volatile boolean autoFieldProbed = false;
  private volatile boolean autoFieldUpgradeRequested = false;
  private int activeAcousticFrames = 0;
  private int activeFramesSinceServerText = 0;
  private int emptyServerTurns = 0;
  private volatile int warmingIndex = -1;
  private volatile long warmingStartStreamMs = -1;
  private final java.util.concurrent.atomic.AtomicInteger officialCleanCuts =
      new java.util.concurrent.atomic.AtomicInteger(0);
  private final java.util.concurrent.atomic.AtomicInteger fallbackCuts =
      new java.util.concurrent.atomic.AtomicInteger(0);
  private Thread worker;
  private int nextIndex = 1;
  private volatile Sess current;

  public RotatingLiveTranscriber(LiveTranscriptionConfig cfg, TranscriptListener listener)
      throws java.io.IOException {
    this.cfg = cfg;
    this.runtimeManualVad = cfg.manualVad;
    this.runtimeFlushAfterMs = cfg.flushAfterMs;
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
    if (first.manualVad) {
      first.ws.sendActivityStart();
    }
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

  /** 会话统计快照（含正在后台预热的 Make-Before-Break 会话）。 */
  public List<SessionInfo> sessionInfos() {
    synchronized (lock) {
      List<SessionInfo> out = new ArrayList<>();
      for (Sess s : sessions) {
        out.add(new SessionInfo(s));
      }
      int wIdx = warmingIndex;
      if (wIdx > 0) {
        out.add(
            new SessionInfo(
                wIdx,
                warmingStartStreamMs,
                streamPosMs.get(),
                "WARMING",
                "后台预热就绪待切",
                false,
                0,
                0,
                0,
                ""));
      }
      return out;
    }
  }

  /** 当前环形重放缓冲区保留的音频毫秒数。 */
  public long bufferedReplayMs() {
    synchronized (lock) {
      if (framePos.isEmpty()) {
        return 0L;
      }
      return Math.max(0L, streamPosMs.get() - framePos.peekFirst()[0]);
    }
  }

  /** 通过官方 {@code !pending}（服务端句末确认）干净完成的轮换次数。 */
  public int officialCleanCutCount() {
    return officialCleanCuts.get();
  }

  /** 因超长紧凑独白未出句末而降级为客户端静音/强制重叠切流的次数。 */
  public int fallbackCutCount() {
    return fallbackCuts.get();
  }

  /** 服务端 Configurable Endpointing 原生自然断句产出的 Final 段数。 */
  public int serverNaturalFinalsCount() {
    synchronized (lock) {
      int sum = 0;
      for (Sess s : sessions) {
        sum += s.serverFinals;
      }
      return sum;
    }
  }

  /** 测试用：立即掐断当前会话的连接，模拟网络故障。 */
  public void abortCurrentSessionForTest() {
    abortRequested = true;
  }

  /** 演示/测试用：立即触发一次先建后切（Make-Before-Break）会话轮换。 */
  public void forceRotateForTest() {
    forceRotateRequested = true;
  }

  /** 演示/测试用：立即在当前会话内发送一次软落定（audioStreamEnd）。 */
  public void forceSoftFlushForTest() {
    forceFlushRequested = true;
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

        if (forceFlushRequested) {
          forceFlushRequested = false;
          long now = streamPosMs.get();
          synchronized (lock) {
            cur.lastFlushStreamMs = now;
            cur.flushes++;
            cur.awaitingSoftFlushFinal = true;
            if (cur.manualVad) {
              cur.awaitingManualRestart = true;
              cur.manualTurnClosed = false;
              cur.manualRestartFromStreamMs = now;
              cur.manualRestartSentWallMs = System.currentTimeMillis();
            }
          }
          if (cur.manualVad) {
            event("手动触发 S%d 会话内大窗闭合（activityEnd → 待落定重启 activityStart）", cur.index);
            cur.ws.sendActivityEnd();
          } else {
            event("手动触发 S%d 会话内软刷（audioStreamEnd）", cur.index);
            cur.ws.sendAudioStreamEnd();
          }
        }

        // 1) 断线恢复（服务端关闭 / 网络故障 / 发送失败）
        if (cur.ws.isDead()) {
          if (pending != null) {
            pending.thenAccept(s -> s.ws.close());
            pending = null;
          }
          warmingIndex = -1;
          cur = recover(cur);
        }

        // 1.5) manualVad（远场 disabled=true）会话内大窗闭合后的安全重启：
        // 服务端收到 activityEnd 后需等内部处理完积压帧并把 speech_active 置回 false，
        // 若立刻发 activityStart 会被忽略；因此待收到服务端句末确认后再发 activityStart 并回放期间暂存帧。
        boolean holdAudioForManualRestart = false;
        if (cur.manualVad) {
          boolean shouldResumeManualTurn = false;
          long resumeFromStreamMs = -1;
          synchronized (lock) {
            if (cur.awaitingManualRestart) {
              boolean timedOut =
                  System.currentTimeMillis() - cur.manualRestartSentWallMs >= 1_500;
              if (cur.manualTurnClosed || timedOut) {
                cur.awaitingManualRestart = false;
                cur.manualTurnClosed = false;
                resumeFromStreamMs = cur.manualRestartFromStreamMs;
                cur.manualRestartFromStreamMs = -1;
                shouldResumeManualTurn = true;
              } else {
                holdAudioForManualRestart = true;
              }
            }
          }
          if (shouldResumeManualTurn) {
            if (!cur.ws.sendActivityStart()) {
              if (!idle) {
                carry = frame;
              }
              continue;
            }
            if (resumeFromStreamMs >= 0) {
              replayFrom(cur, resumeFromStreamMs);
            }
          }
        }

        // 2) 发送音频（若正在等待 manualVad 大窗落定，帧只写入环形重放缓冲，待 activityStart 后零丢帧补发）
        if (!idle) {
          long pos = streamPosMs.get();
          if (!holdAudioForManualRestart) {
            if (!cur.ws.sendAudio(frame)) {
              carry = frame;
              continue;
            }
          }
          remember(pos, frame);
          streamPosMs.addAndGet(frame.length * 1000L / (LiveTranscriptionConfig.SAMPLE_RATE * 2));
          int level = rms(frame);
          silentFrames = level < silenceThreshold(level) ? silentFrames + 1 : 0;
          if (silentFrames == 0) {
            flushedThisPause = false;
          }
          if (level > 180) {
            activeAcousticFrames++;
            synchronized (lock) {
              activeFramesSinceServerText++;
            }
          }
          lastFrameWallMs = System.currentTimeMillis();

          // 2.2) 自适应近场/远场探测（方案三 autoFieldDetect）：
          // 指标 A：前 3s+ 且累计 >=1.2s 有效声学活动后，计算 P80 语音电平与 P80/P10 信噪比；
          // 指标 B：服务端反馈兜底——若连续 >=5.5s 有人声能量但服务端未返回任何文字，或连续 >=2 次空话轮。
          if (cfg.autoFieldDetect && !runtimeManualVad) {
            if (!autoFieldProbed && levelCount >= 30 && activeAcousticFrames >= 12) {
              autoFieldProbed = true;
              int n = Math.min(levelCount, levelRing.length);
              int[] sorted = Arrays.copyOf(levelRing, n);
              Arrays.sort(sorted);
              int p10 = sorted[(int) (0.10 * (n - 1))];
              int p80 = sorted[(int) (0.80 * (n - 1))];
              double snrRatio = (double) p80 / Math.max(1, p10);
              if (p80 < 1350 || snrRatio < 4.5) {
                runtimeManualVad = true;
                runtimeFlushAfterMs = LiveTranscriptionConfig.DEFAULT_FAR_FIELD_FLUSH_MS;
                autoFieldUpgradeRequested = true;
                event(
                    "自适应探针：检出远场/高混响声学特征 (P80=%d, P10=%d, SNR=%.1fx) → 热切换至远场模式 (manualVad=true)",
                    p80, p10, snrRatio);
              } else {
                event(
                    "自适应探针：确认为近场高信噪比语音 (P80=%d, P10=%d, SNR=%.1fx) → 保持 Auto-VAD 按句定稿模式",
                    p80, p10, snrRatio);
              }
            } else {
              int turns;
              int droughtFrames;
              synchronized (lock) {
                turns = emptyServerTurns;
                droughtFrames = activeFramesSinceServerText;
              }
              if (turns >= 2 || droughtFrames >= 55) {
                autoFieldProbed = true;
                runtimeManualVad = true;
                runtimeFlushAfterMs = LiveTranscriptionConfig.DEFAULT_FAR_FIELD_FLUSH_MS;
                autoFieldUpgradeRequested = true;
                event(
                    "自适应探针：检出服务端 Auto-VAD 吞字迹象 (空话轮=%d, 连续未出字人声=%.1fs) → 热切换至远场模式 (manualVad=true)",
                    turns, droughtFrames * 0.1);
              }
            }
          }

          // 2.5) 会话内定期落定与统一自适应定稿（unifiedAdaptive）：
          // - 统一自适应模式（unifiedAdaptive=true, manualVad=true）：
          //   实时根据最近 10s 滑动窗口的 P80 与 SNR(P80/P10) 自动分档：
          //   * ⚡ 近场高信噪比档 (P80>=1350 && SNR>=4.5x)：已说 >=3.5s 且出现 >=600ms 自然停顿立即出 Final；或满 15s 微停顿兜底；
          //   * 🛡️ 远场低信噪比抗噪档 (P80<1350 || SNR<4.5x)：满 15s 且出现 >=250ms 微停顿出 Final；或已说 >=6.0s 且出现 >=1.8s 话轮长停顿出 Final。
          // - 自动 VAD（Method C 默认 24s）：日常断句交由服务端，超 24s 趁微停顿发 audioStreamEnd 破除死锁；
          // - 远场手动 VAD（farFieldMode 默认 45s）：趁微停顿发 activityEnd 落盘整段长上下文。
          if (cfg.unifiedAdaptive && levelCount >= 30 && levelCount % 10 == 0) {
            boolean nearNow = isNearFieldCleanWindow();
            if (lastLoggedNearTier == null || lastLoggedNearTier != nearNow) {
              if (lastLoggedNearTier == null || Math.abs(levelCount - lastTierChangeFrame) >= 30) {
                lastLoggedNearTier = nearNow;
                lastTierChangeFrame = levelCount;
                double snr = (double) cachedSpeech / Math.max(1, cachedFloor);
                event(
                    "统一自适应定稿：当前 10s 滑动窗 P80=%d, P10=%d, SNR=%.1fx → 激活「%s」",
                    cachedSpeech,
                    cachedFloor,
                    snr,
                    nearNow
                        ? "⚡近场按句灵敏定稿档 (≥3.5s语音+600ms停顿 / 15s兜底)"
                        : "🛡️远场长上下文抗噪档 (15s窗口 / ≥1.8s长停顿)");
              }
            }
          }
          if (cur.flushAfterMs > 0
              && silentFrames > 0
              && !flushedThisPause
              && !holdAudioForManualRestart
              && !(cur.manualVad && pending != null)) {
            boolean doFlush;
            long now = streamPosMs.get();
            boolean nearClean = cfg.unifiedAdaptive && isNearFieldCleanWindow();
            synchronized (lock) {
              long segStart = Math.max(cur.audioStartStreamMs,
                  Math.max(cur.lastFlushStreamMs, cur.lastFinalStreamMs));
              long seg = now - segStart;
              int needed = seg < 2 * cur.flushAfterMs ? silenceFramesNeeded
                  : seg < 3 * cur.flushAfterMs ? Math.max(1, silenceFramesNeeded - 1) : 1;
              boolean baseReady = !cur.awaitingManualRestart && !cur.pendingText.isBlank();
              if (cfg.unifiedAdaptive) {
                boolean windowFlush = seg >= cur.flushAfterMs && silentFrames >= needed;
                boolean dynamicSentenceFlush =
                    nearClean
                        ? (seg >= 3_500 && silentFrames >= 6)
                        : (seg >= 6_000 && silentFrames >= 18);
                doFlush = baseReady && (dynamicSentenceFlush || windowFlush);
              } else {
                doFlush = baseReady && seg >= cur.flushAfterMs && silentFrames >= needed;
              }
              if (doFlush) {
                cur.lastFlushStreamMs = now;
                cur.flushes++;
                cur.awaitingSoftFlushFinal = true;
                if (cur.manualVad) {
                  cur.awaitingManualRestart = true;
                  cur.manualTurnClosed = false;
                  cur.manualRestartFromStreamMs = now;
                  cur.manualRestartSentWallMs = System.currentTimeMillis();
                }
              }
            }
            if (doFlush) {
              flushedThisPause = true;
              if (cur.manualVad) {
                cur.ws.sendActivityEnd();
              } else {
                cur.ws.sendAudioStreamEnd();
              }
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
            int targetNextIdx;
            synchronized (lock) {
              targetNextIdx = nextIndex;
              cur.endReason = reason;
            }
            event("触发轮换 S%d（%s）→ 后台建立 S%d", cur.index, reason, targetNextIdx);
            warmingStartStreamMs = streamPosMs.get();
            warmingIndex = targetNextIdx;
            pending = CompletableFuture.supplyAsync(this::openOrThrow);
            pendingSinceWall = System.currentTimeMillis();
          }
        }

        // 4) 新会话就绪 → 分级选切点
        if (pending != null && pending.isDone()) {
          boolean waitingManualTurnClose;
          synchronized (lock) {
            waitingManualTurnClose = cur.awaitingManualRestart;
          }
          if (!waitingManualTurnClose) {
            Sess next;
            try {
              next = pending.join();
            } catch (Exception e) {
              Throwable cause = unwrapCause(e);
              if (cfg.officialFirst
                  && cause instanceof LiveWsSession.ConnectFailureException cfe
                  && !cfe.isTransient()) {
                throw cfe;
              }
              event("新会话建立失败，1s 后重试：%s", rootMessage(e));
              pending = null;
              warmingIndex = -1;
              nextAttemptWall = System.currentTimeMillis() + 1000;
              continue;
            }
            long waited = System.currentTimeMillis() - pendingSinceWall;
            boolean clientSilence = silentFrames >= silenceFramesNeeded;
            boolean officialNotPending;
            boolean goAwayUrgent;
            synchronized (lock) {
              officialNotPending = !cur.pending();
              goAwayUrgent = cur.goAway && System.currentTimeMillis() - cur.goAwayWallMs > 15_000;
            }
            String cutHow = null;
            if (!cur.manualVad && next.manualVad) {
              // 自适应探针从 Auto-VAD 升级至远场 manualVad 模式：立即无等待切流并从旧会话最后定稿处全量重放
              cutHow = "auto_far_field";
            } else if (cfg.officialFirst) {
              // Method C（官方优先）：自动 VAD 下在 preferServerCutoverMs（默认 12s）内优先认官方 !cur.pending() 句末切点；
              // 若开启 manualVad（服务端不会自动断句，切流时必须调 startDrain 发 activityEnd 落定）或超出等待窗口 / GoAway 紧急，
              // 则在客户端微停顿时无盲区切流并后台排空旧会话。
              if (!cur.manualVad && officialNotPending) {
                cutHow = "official_gap";
              } else if ((cur.manualVad || waited >= cfg.preferServerCutoverMs || goAwayUrgent)
                  && clientSilence) {
                cutHow = "silence";
              } else if (waited >= cfg.maxCutWaitMs || goAwayUrgent) {
                cutHow = "forced";
              }
            } else {
              // Method B（传统模式）：客户端 300ms 低 RMS 与服务端句末并列抢切
              if (!cur.manualVad && officialNotPending && silentFrames >= 1) {
                cutHow = "official_gap";
              } else if (clientSilence) {
                cutHow = "silence";
              } else if (waited >= cfg.maxCutWaitMs || goAwayUrgent) {
                cutHow = "forced";
              }
            }
            if (cutHow != null) {
              warmingIndex = -1;
              if ("official_gap".equals(cutHow)) {
                officialCleanCuts.incrementAndGet();
              } else {
                fallbackCuts.incrementAndGet();
              }
              cutOver(cur, next, cutHow, silentFrames * (long) FRAME_MS);
              pending = null;
            }
          }
        }
      }

      // 音频结束：收尾当前会话
      Sess cur = current;
      warmingIndex = -1;
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
      if (autoFieldUpgradeRequested) {
        autoFieldUpgradeRequested = false;
        return "自适应切换远场模式";
      }
      if (forceRotateRequested) {
        forceRotateRequested = false;
        return "手动触发轮换";
      }
      if (cur.goAway) {
        return "GoAway";
      }
      long age = System.currentTimeMillis() - cur.ws.openedWallMs;
      if (age >= cfg.rotateAfterMs) {
        return String.format(Locale.ROOT, "到龄 %ds", age / 1000);
      }
      long effectiveStallMs = cur.manualVad ? Math.max(cfg.stallMs, 90_000L) : cfg.stallMs;
      if (effectiveStallMs > 0 && !cur.pendingText.isBlank()) {
        long since = streamPosMs.get() - Math.max(cur.lastFinalStreamMs, cur.audioStartStreamMs);
        if (since >= effectiveStallMs) {
          return String.format(Locale.ROOT, "看门狗：%ds 无 Final", since / 1000);
        }
      }
      return null;
    }
  }

  /**
   * 切换：新会话接管后续音频，旧会话异步收尾。
   *
   * <p>支持四级切流语义：
   * <ul>
   *   <li>{@code auto_far_field}（自适应升级远场模式）：从旧会话最后一次 Final 结束处全量重放未定稿音频，把被 Auto-VAD 漏掉的开头词 100% 追回；</li>
   *   <li>{@code official_gap}（Method C 首选，对应官方 {@code _gemini_stt.py} 的 {@code not state.pending}）：
   *       旧会话已定稿句末，仅从 {@code max(lastFinalEndGlobalMs, pos - tailMs)} 回放纯句后尾随缓冲，不重放已落定词；</li>
   *   <li>{@code silence}（超时降级客户端静音切点）：在静音窗口内回放最多 {@code tailMs} 前导帧并开启词级去重兜底；</li>
   *   <li>{@code forced}（强制切流兜底）：重放最近 {@code cfg.overlapMs}（默认 2000ms）音频并按词锚点去重。</li>
   * </ul>
   */
  private void cutOver(Sess old, Sess next, String how, long clientSilentMs) {
    long pos = streamPosMs.get();
    if (next.manualVad) {
      next.ws.sendActivityStart();
    }
    if ("auto_far_field".equals(how)) {
      long from;
      synchronized (lock) {
        from = old.lastFinalEndGlobalMs >= 0 ? old.lastFinalEndGlobalMs : old.audioStartStreamMs;
        old.pendingText = "";
        old.speaking = false;
      }
      long start = replayFrom(next, from);
      synchronized (lock) {
        next.audioStartStreamMs = start;
        next.overlapWithPrev = true;
      }
    } else if ("forced".equals(how)) {
      long from = pos - cfg.overlapMs;
      long start = replayFrom(next, from);
      synchronized (lock) {
        next.audioStartStreamMs = start;
        next.overlapWithPrev = true;
      }
    } else if ("official_gap".equals(how)) {
      long maxTail = cfg.effectiveTailReplayMs();
      long postFinalStart;
      synchronized (lock) {
        long floor = old.lastFinalEndGlobalMs >= 0 ? old.lastFinalEndGlobalMs : old.audioStartStreamMs;
        // 1:1 对齐官方 _gemini_stt.py trim_before(tell - tail_bytes)：服务端返回 finished=true 时，
        // 最近 effectiveTailReplayMs()（默认 500ms）恰为触发端点判定的句末静音区，从此处衔接既不截词也不切入上一句尾词内部。
        postFinalStart = Math.max(floor, pos - maxTail);
      }
      if (pos > postFinalStart) {
        long start = replayFrom(next, postFinalStart);
        synchronized (lock) {
          next.audioStartStreamMs = start;
          next.overlapWithPrev = true;
        }
      } else {
        synchronized (lock) {
          next.audioStartStreamMs = pos;
        }
      }
    } else {
      long maxTail = cfg.effectiveTailReplayMs();
      long preRollMs;
      synchronized (lock) {
        // 若旧会话还有未落定语音（将在 drain(old) 中落定），仅回放当前静音期内的帧；
        // 若旧会话已完全落定（!old.pending()），可回放至 maxTail
        preRollMs = old.pending() ? Math.min(maxTail, clientSilentMs) : maxTail;
      }
      if (preRollMs > 0) {
        long start = replayFrom(next, pos - preRollMs);
        synchronized (lock) {
          next.audioStartStreamMs = start;
          next.overlapWithPrev = true;
        }
      } else {
        synchronized (lock) {
          next.audioStartStreamMs = pos;
        }
      }
    }
    synchronized (lock) {
      old.audioEndStreamMs = pos;
      if ("official_gap".equals(how) || "auto_far_field".equals(how)) {
        old.state = State.DONE;
      }
      sessions.add(next);
      current = next;
    }
    String label =
        "auto_far_field".equals(how)
            ? "auto_far_field·声学探针升级远场并全量回放"
            : "official_gap".equals(how)
                ? "official_gap·官方句末切点"
                : "silence".equals(how) ? "silence·客户端静音兜底" : "forced·重叠兜底";
    event("切换完成 S%d → S%d @ %.1fs（%s）", old.index, next.index, pos / 1000.0, label);
    if ("official_gap".equals(how) || "auto_far_field".equals(how)) {
      // 对齐官方 _gemini_stt.py：旧会话已完成句末落定或已由远场会话全量回放接管，直接关闭旧会话。
      old.ws.close();
      event("S%d 已收尾（%.1fs~%.1fs，Final=%d，兜底=%d，原因=%s·零排空关闭）",
          old.index, old.audioStartStreamMs / 1000.0, old.audioEndStreamMs / 1000.0,
          old.finals, old.fallbacks, old.endReason);
      flush();
    } else {
      startDrain(old);
    }
  }

  /** 断线恢复：遵循官方瞬态错误分类重建会话，并从上一个 Final 结束处重放音频。 */
  private Sess recover(Sess dead) throws Exception {
    if (cfg.officialFirst && !dead.ws.isTransientFailure()) {
      throw new IllegalStateException("S" + dead.index + " 发生非瞬态连接错误，停止重连");
    }
    long pos = streamPosMs.get();
    long from;
    synchronized (lock) {
      from = dead.replayFromMs >= 0 ? dead.replayFromMs : dead.audioStartStreamMs;
      dead.audioEndStreamMs = pos;
      dead.endReason = "断线";
      // 未落定的 interim 由重放音频重新转写，这里丢弃，避免重复
      dead.pendingText = "";
      dead.speaking = false;
      dead.awaitingSoftFlushFinal = false;
      dead.state = State.DONE;
    }
    event("S%d 连接已断开，重建会话并从 %.1fs 重放 %.1fs 音频", dead.index, from / 1000.0,
        (pos - from) / 1000.0);
    Sess next = openWithRetry();
    if (next.manualVad) {
      next.ws.sendActivityStart();
    }
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
    long keep = Math.max(cfg.maxReplayMs, Math.max(cfg.overlapMs, cfg.effectiveTailReplayMs()));
    while (!framePos.isEmpty() && pos - framePos.peekFirst()[0] > keep) {
      framePos.removeFirst();
      frames.removeFirst();
    }
  }

  // =============================================================================================
  // 会话建立 / 收尾
  // =============================================================================================

  private static Throwable unwrapCause(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null && (cur instanceof RuntimeException || cur instanceof java.util.concurrent.CompletionException)) {
      cur = cur.getCause();
    }
    return cur;
  }

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
        if (cfg.officialFirst
            && e instanceof LiveWsSession.ConnectFailureException cfe
            && !cfe.isTransient()) {
          event("建连遭遇非瞬态错误（code=%d, http=%d），遵循官方策略立即中止重试：%s",
              cfe.closeCode, cfe.httpStatus, rootMessage(e));
          listener.onError(e);
          throw e;
        }
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
    boolean useManualVad = runtimeManualVad;
    long useFlushAfterMs = runtimeFlushAfterMs;
    synchronized (lock) {
      idx = nextIndex++;
    }
    String token;
    synchronized (credentials) {
      credentials.refreshIfExpired();
      token = credentials.getAccessToken().getTokenValue();
    }
    LiveTranscriptionConfig sessCfg =
        (useManualVad == cfg.manualVad && useFlushAfterMs == cfg.flushAfterMs)
            ? cfg
            : cfg.withManualVad(useManualVad, useFlushAfterMs);
    Sess[] holder = new Sess[1];
    LiveWsSession ws = LiveWsSession.open(idx, http, sessCfg, token, new SessionCallback(holder));
    Sess s = new Sess(ws, useManualVad, useFlushAfterMs);
    holder[0] = s;
    event("S%d 已建立（manualVad=%s）", idx, useManualVad);
    return s;
  }

  private void startDrain(Sess s) {
    synchronized (lock) {
      if (s.state != State.ACTIVE) {
        return;
      }
      s.state = State.DRAINING;
      s.generationCompletedDuringDrain = false;
      if (s.audioEndStreamMs < 0) {
        s.audioEndStreamMs = streamPosMs.get();
      }
    }
    drains.add(CompletableFuture.runAsync(() -> drain(s)));
  }

  /**
   * 对应官方 Python {@code _gemini_stt.py._flush}：
   * <ol>
   *   <li>若开启 {@code manualVad}，发送 {@code activityEnd} 并等待服务端确认结束；</li>
   *   <li>若为自动 VAD，先发送 {@code audioStreamEnd: true}（立即结束服务端已处理到的语音），
   *       紧接着补发 {@code effectiveSilenceDurationMs() + flushSilenceMarginMs} 毫秒的全零 PCM 静音帧
   *       （解决服务端流式 Tokenizer 尚未推进到最后一帧语音时忽略 {@code audioStreamEnd} 的问题）；</li>
   *   <li>等待未落定话轮返回 Final 且服务端在 {@code drainQuietMs} 内保持静默。</li>
   * </ol>
   */
  private void drain(Sess s) {
    long t0 = System.currentTimeMillis();
    try {
      if (s.manualVad) {
        s.ws.sendActivityEnd();
      } else {
        s.ws.sendAudioStreamEnd();
        long trailingSilenceMs = cfg.effectiveSilenceDurationMs() + cfg.flushSilenceMarginMs;
        s.ws.sendSilence(trailingSilenceMs);
      }
      while (true) {
        Thread.sleep(100);
        long now = System.currentTimeMillis();
        boolean pendingInterim;
        boolean genDone;
        long lastActivity;
        synchronized (lock) {
          pendingInterim = !s.pendingText.isBlank();
          genDone = s.generationCompletedDuringDrain;
          lastActivity = Math.max(s.lastFinalWallMs, s.lastServerMessageWallMs);
        }
        long elapsed = now - t0;
        long minQuietElapsed = Math.max(cfg.drainQuietMs, s.manualVad ? 1_500L : 600L);
        boolean quiet =
            elapsed >= minQuietElapsed
                && now - Math.max(lastActivity, t0) >= (s.manualVad ? Math.max(cfg.drainQuietMs, 1_200L) : cfg.drainQuietMs);
        if (s.ws.isDead()
            || elapsed >= cfg.drainMaxMs
            || (!pendingInterim && (genDone || quiet))) {
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
          s.speaking = false;
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
    public void onServerMessage(LiveWsSession ws) {
      Sess s = sess();
      if (s != null) {
        synchronized (lock) {
          s.lastServerMessageWallMs = System.currentTimeMillis();
        }
      }
    }

    @Override
    public void onVoiceActivity(LiveWsSession ws, boolean active) {
      Sess s = sess();
      if (s != null) {
        synchronized (lock) {
          if (s.state == State.DONE) {
            return;
          }
          s.speaking = active;
          if (active) {
            s.turnHadFinal = false;
          } else {
            if (s.awaitingManualRestart) {
              s.manualTurnClosed = true;
            }
            if (s.state == State.DRAINING) {
              s.generationCompletedDuringDrain = true;
            }
          }
        }
      }
    }

    @Override
    public void onInterim(LiveWsSession ws, String text, long startOffset, long endOffset) {
      onInterim(ws, text, startOffset, endOffset, null);
    }

    @Override
    public void onInterim(
        LiveWsSession ws, String text, long startOffset, long endOffset, String rawJson) {
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
        if (text != null && !text.isBlank()) {
          activeFramesSinceServerText = 0;
          emptyServerTurns = 0;
        }
        s.speaking = true;
        s.turnHadFinal = false;
        if (s.pendingText.isBlank() || startOffset != s.pendingStartOffset) {
          s.pendingStartOffset = startOffset;
        }
        s.pendingText = text;
        s.pendingEndOffset = endOffset;
        if (startOffset < 0 && !s.offsetsMissingReported) {
          s.offsetsMissingReported = true;
          warn = true;
        }
        ev =
            new TranscriptEvent(
                TranscriptEvent.Type.INTERIM,
                TranscriptEvent.Source.SERVER,
                text,
                s.global(startOffset),
                s.global(endOffset),
                s.index,
                rawJson);
      }
      if (warn) {
        event("S%d 服务端 interim 未携带时间戳，Final 时间戳改为客户端估算", s.index);
      }
      listener.onInterim(ev);
    }

    @Override
    public void onFinal(LiveWsSession ws, String text) {
      onFinal(ws, text, null);
    }

    @Override
    public void onFinal(LiveWsSession ws, String text, String rawJson) {
      Sess s = sess();
      if (s == null) {
        return;
      }
      synchronized (lock) {
        if (s.state == State.DONE) {
          return;
        }
        if (text != null && !text.isBlank()) {
          activeFramesSinceServerText = 0;
          emptyServerTurns = 0;
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
        boolean wasSoftFlush = s.awaitingSoftFlushFinal;
        s.awaitingSoftFlushFinal = false;
        TranscriptEvent.Source src =
            wasSoftFlush ? TranscriptEvent.Source.SOFT_FLUSH : TranscriptEvent.Source.SERVER;
        s.unemitted.add(
            new TranscriptEvent(
                TranscriptEvent.Type.FINAL, src, text, start, end, s.index, rawJson));
        s.finals++;
        if (!wasSoftFlush) {
          s.serverFinals++;
        }
        s.speaking = false;
        if (s.awaitingManualRestart) {
          s.manualTurnClosed = true;
        }
        s.turnHadFinal = true;
        s.pendingText = "";
        s.pendingStartOffset = -1;
        s.lastFinalStreamMs = pos;
        s.lastFinalWallMs = System.currentTimeMillis();
        s.lastFinalEndGlobalMs = end;
        // 对应官方 Python _AudioStream.mark_finalized()：
        // 保留定稿前最后 tail_ms = min(500ms, silenceDurationMs) 的静音音频，防止刚开口的新词首音节被截断
        long tailMs = cfg.effectiveTailReplayMs();
        s.replayFromMs =
            exact
                ? Math.max(s.audioStartStreamMs, end - tailMs)
                : Math.max(s.audioStartStreamMs, end - Math.max(3000L, tailMs));
      }
      flush();
    }

    @Override
    public void onGenerationComplete(LiveWsSession ws) {
      Sess s = sess();
      if (s == null) {
        return;
      }
      synchronized (lock) {
        if (s.state == State.DONE) {
          return;
        }
        s.speaking = false;
        if (s.awaitingManualRestart) {
          s.manualTurnClosed = true;
        }
        if (s.state == State.DRAINING) {
          s.generationCompletedDuringDrain = true;
        }
        // 对应官方 Python _TranscriptionState.on_message(generation_complete)：
        // 仅当该话轮无识别文字（!turnHadFinal，如咳嗽/底噪触发后直接 generationComplete）时推进重放游标
        if (!s.turnHadFinal && s.pendingText.isBlank()) {
          if (activeAcousticFrames >= 10) {
            emptyServerTurns++;
          }
          long pos = streamPosMs.get();
          s.replayFromMs = Math.max(s.audioStartStreamMs, pos - cfg.effectiveTailReplayMs());
        }
      }
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
   *   <li>text 开头恰好是已发出文本的结尾：前缀/后缀匹配（40→1 词）。</li>
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
    // 2) 前缀/后缀匹配（含 1 词边界重叠消重，如切流边界处的单连词 "but" / "and"）
    for (int k = Math.min(40, Math.min(nt.length, nw.length)); k >= 1; k--) {
      if (k == 1 && nw[0].isEmpty()) {
        continue;
      }
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
    // 3) 无空格语言（中文 / 日文 / 泰文等 CJK/Thai 脚本）字符级重叠消重：
    // 解决无空格句子整句被视为单 token 导致切流微回放残留 2~10 字重复（如 "社零增6.3%。" + "6.3，但..."）的问题。
    String tailJoined = String.join("", tailWords);
    if (containsUnspacedScript(tailJoined) || containsUnspacedScript(text)) {
      return dropUnspacedCharOverlap(tailJoined, text);
    }
    return text;
  }

  private static boolean containsUnspacedScript(String s) {
    for (int i = 0; i < s.length(); i++) {
      Character.UnicodeScript sc = Character.UnicodeScript.of(s.codePointAt(i));
      if (sc == Character.UnicodeScript.HAN
          || sc == Character.UnicodeScript.THAI
          || sc == Character.UnicodeScript.HIRAGANA
          || sc == Character.UnicodeScript.KATAKANA) {
        return true;
      }
    }
    return false;
  }

  private static boolean isWordCodePoint(int cp) {
    if (Character.isLetterOrDigit(cp)) {
      return true;
    }
    int type = Character.getType(cp);
    return type == Character.NON_SPACING_MARK
        || type == Character.COMBINING_SPACING_MARK
        || type == Character.ENCLOSING_MARK;
  }

  private static String dropUnspacedCharOverlap(String tailJoined, String text) {
    StringBuilder tNorm = new StringBuilder();
    for (int i = 0; i < tailJoined.length(); ) {
      int cp = tailJoined.codePointAt(i);
      if (isWordCodePoint(cp)) {
        tNorm.appendCodePoint(Character.toLowerCase(cp));
      }
      i += Character.charCount(cp);
    }
    StringBuilder wNorm = new StringBuilder();
    int[] wEndIdx = new int[text.length()];
    int wCount = 0;
    for (int i = 0; i < text.length(); ) {
      int cp = text.codePointAt(i);
      int nextI = i + Character.charCount(cp);
      if (isWordCodePoint(cp)) {
        wNorm.appendCodePoint(Character.toLowerCase(cp));
        wEndIdx[wCount++] = nextI;
      }
      i = nextI;
    }
    int tLen = tNorm.length();
    if (tLen < 2 || wCount < 2) {
      return text;
    }
    String ts = tNorm.length() > 160 ? tNorm.substring(tNorm.length() - 160) : tNorm.toString();
    tLen = ts.length();
    String ws = wNorm.toString();

    // 3a) 字符级锚点搜索（重放起点早于上一句末尾，匹配 >=5 个归一化字符，容忍末尾 0~2 字差异）
    for (int skip = 0; skip <= 2; skip++) {
      for (int k = Math.min(12, tLen - skip); k >= 5; k--) {
        String needle = ts.substring(tLen - skip - k, tLen - skip);
        int limit = Math.min(120, ws.length()) - k;
        for (int i = 0; i <= limit; i++) {
          if (ws.regionMatches(i, needle, 0, k)) {
            int cutNormIdx = Math.min(wCount, i + k + skip) - 1;
            return stripLeadingSeparatorPunct(text.substring(wEndIdx[cutNormIdx]));
          }
        }
      }
    }

    // 3b) 字符级后缀-前缀精确匹配（k >= 2，避免如 "...做好。" + "好的..." 的单字误删）
    for (int k = Math.min(40, Math.min(tLen, ws.length())); k >= 2; k--) {
      if (ts.regionMatches(tLen - k, ws, 0, k)) {
        if (k == 2
            && ts.charAt(tLen - 2) <= 'z'
            && Character.isLetter(ts.charAt(tLen - 2))
            && ts.charAt(tLen - 1) <= 'z'
            && Character.isLetter(ts.charAt(tLen - 1))) {
          continue;
        }
        return stripLeadingSeparatorPunct(text.substring(wEndIdx[k - 1]));
      }
    }
    return text;
  }

  private static String stripLeadingSeparatorPunct(String s) {
    int i = 0;
    while (i < s.length()) {
      char c = s.charAt(i);
      if (Character.isWhitespace(c)
          || ",.;:!?%‰，。、；：！？）》】])".indexOf(c) >= 0) {
        i++;
      } else {
        break;
      }
    }
    return s.substring(i);
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
  private int cachedFloor = 100;
  private int cachedSpeech = 1000;
  private Boolean lastLoggedNearTier = null;
  private int lastTierChangeFrame = 0;
  private boolean flushedThisPause = false;

  private boolean isNearFieldCleanWindow() {
    double snr = (double) cachedSpeech / Math.max(1, cachedFloor);
    return levelCount >= 20 && cachedSpeech >= 1350 && snr >= 4.5;
  }

  private int silenceThreshold(int level) {
    levelRing[levelCount % levelRing.length] = level;
    levelCount++;
    if (cachedThreshold < 0 || levelCount % 10 == 0) {
      int n = Math.min(levelCount, levelRing.length);
      int[] s = Arrays.copyOf(levelRing, n);
      Arrays.sort(s);
      cachedFloor = s[(int) (0.10 * (n - 1))];
      cachedSpeech = s[(int) (0.80 * (n - 1))];
      cachedThreshold =
          Math.max(cfg.silenceRms, cachedFloor + (int) (0.15 * Math.max(0, cachedSpeech - cachedFloor)));
    }
    if (!cfg.adaptiveSilence) {
      return cfg.silenceRms;
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
