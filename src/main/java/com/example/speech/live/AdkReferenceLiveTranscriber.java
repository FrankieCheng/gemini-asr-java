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
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import okhttp3.OkHttpClient;

/**
 * 1:1 忠实复刻官方 Python ADK 参考实现（{@code google.adk.live._gemini_stt.GeminiSTT}）的 Java 流式转写器，
 * 用于与双会话先建后切增强方案 {@link RotatingLiveTranscriber} 进行同源音频 A/B 实时对决。
 *
 * <p>严格遵循 {@code _gemini_stt.py} 的四大设计约定：
 * <ol>
 *   <li><b>严格单连接串行重启（Sequential Restart）</b>：同一时刻只维持 1 条活跃 WebSocket 连接，不后台预建
 *       {@code nextSession}。会话到龄（{@code rotateAfterMs}）后若 {@code pending == true}（说话未落定），
 *       继续推流等待服务端自然下发 Final；一旦 {@code !pending}（或收到 {@code GoAway}），立即关闭当前连接，
 *       再串行发起新连接握手（期间到来的实时音频帧暂存在输入队列中），建连后先补发 {@code replay()} 帧再追赶实时帧。</li>
 *   <li><b>纯服务端 Configurable Endpointing</b>：不做客户端 RMS 声学能量检测，也不在会话中途发送
 *       {@code audioStreamEnd} 软刷；仅在整段音频流结束（{@code _flush}）时发送
 *       {@code audioStreamEnd + (silenceDurationMs + 200ms)} 全零 PCM 静音帧并等待 500ms 静默窗口（最长 5s）。</li>
 *   <li><b>固定上限未落定缓冲（{@code _AudioStream}, 默认 {@code maxReplayMs = 10_000ms}）</b>：
 *       每次 Final / {@code generationComplete} 调用 {@code mark_finalized()} 裁剪至最后
 *       {@code tail_ms = min(500ms, silenceDurationMs)}；但未落定期间最多保留 {@code maxReplayMs}（默认 10s），
 *       若连续长独白超过 10s 未触发服务端断句时发生网络闪断或强制 {@code GoAway}，超出 10s 的前段音频将永久滑出重放窗。</li>
 *   <li><b>无词级重叠去重（无 {@code dropOverlap}） + 无超时 Interim 升格兜底</b>：
 *       句间切流完全依赖 {@code mark_finalized()} 静音边界；断线或收尾超时未落定的 {@code partial} 按原实现直接丢弃。</li>
 * </ol>
 */
public final class AdkReferenceLiveTranscriber implements AutoCloseable {

  private static final int FRAME_MS = 100;
  private static final int FRAME_BYTES = LiveTranscriptionConfig.SAMPLE_RATE * 2 * FRAME_MS / 1000;
  private static final long FLUSH_TIMEOUT_MS = 5_000L;
  private static final long FLUSH_QUIET_MS = 500L;
  private static final int MAX_RECONNECTS = 3;
  private static final long STABLE_SESSION_MS = 30_000L;

  private record AudioFrame(long streamPosMs, byte[] pcm) {}

  private static final class AdkSess {
    final int index;
    volatile LiveWsSession ws;
    long audioStartStreamMs = -1;
    long audioEndStreamMs = -1;
    String state = "CONNECTING";
    String endReason = "-";
    boolean goAway = false;
    int finals = 0;
    final List<String> emittedTexts = new ArrayList<>();

    AdkSess(int index) {
      this.index = index;
    }
  }

  private final LiveTranscriptionConfig cfg;
  private final long adkMaxReplayMs;
  private final long tailMs;
  private final TranscriptListener listener;
  private final GoogleCredentials credentials;
  private final OkHttpClient http;

  private final Object lock = new Object();
  private final BlockingQueue<byte[]> inbox = new LinkedBlockingQueue<>();
  private byte[] partialBytes = new byte[0];

  // 对应 _AudioStream._unfinalized
  private final Deque<AudioFrame> unfinalized = new ArrayDeque<>();
  private int unfinalizedBytes = 0;
  private long currentTurnTrimmedMs = 0L;
  private final AtomicLong totalDroppedAudioMs = new AtomicLong(0L);
  private final AtomicLong totalHandshakeGapMs = new AtomicLong(0L);
  private final AtomicLong streamPosMs = new AtomicLong(0L);

  // 对应 _TranscriptionState
  private boolean speaking = false;
  private boolean turnHadFinal = false;
  private String partialText = "";
  private long pendingStartOffset = -1L;
  private long pendingEndOffset = -1L;
  private boolean resumed = false;
  private boolean goAwayFlag = false;
  private boolean activityEnded = false;
  private long lastServerMsgWallMs = 0L;
  private long currentSessionOffsetBaseMs = 0L;
  private long lastFinalEndStreamMs = 0L;

  private final List<AdkSess> sessions = new ArrayList<>();
  private volatile AdkSess activeSess;
  private volatile boolean feeding = true;
  private volatile boolean abortRequested = false;
  private volatile boolean forceRotateDue = false;
  private volatile boolean waitingUtteranceEnd = false;
  private Thread worker;
  private int nextIndex = 1;

  public AdkReferenceLiveTranscriber(
      LiveTranscriptionConfig cfg, long adkMaxReplayMs, TranscriptListener listener)
      throws IOException {
    this.cfg = cfg;
    this.adkMaxReplayMs = Math.max(0L, adkMaxReplayMs);
    this.tailMs = cfg.effectiveTailReplayMs();
    this.listener = listener;
    this.credentials =
        GoogleCredentials.getApplicationDefault()
            .createScoped("https://www.googleapis.com/auth/cloud-platform");
    this.http = new OkHttpClient.Builder().readTimeout(0, TimeUnit.MILLISECONDS).build();
  }

  public void start() {
    worker = new Thread(this::transcribeLoop, "adk-reference-live-transcriber");
    worker.setDaemon(true);
    worker.start();
  }

  public void feed(byte[] pcm) {
    if (!feeding) {
      throw new IllegalStateException("finish() 之后不能再 feed");
    }
    byte[] buf = new byte[partialBytes.length + pcm.length];
    System.arraycopy(partialBytes, 0, buf, 0, partialBytes.length);
    System.arraycopy(pcm, 0, buf, partialBytes.length, pcm.length);
    int off = 0;
    while (buf.length - off >= FRAME_BYTES) {
      inbox.add(Arrays.copyOfRange(buf, off, off + FRAME_BYTES));
      off += FRAME_BYTES;
    }
    partialBytes = Arrays.copyOfRange(buf, off, buf.length);
  }

  public void finish() throws InterruptedException {
    if (partialBytes.length > 0) {
      inbox.add(partialBytes);
      partialBytes = new byte[0];
    }
    feeding = false;
    if (worker != null) {
      worker.join(FLUSH_TIMEOUT_MS + 60_000L + inbox.size() * (long) FRAME_MS);
    }
  }

  public void abortCurrentSessionForTest() {
    abortRequested = true;
  }

  public void forceRotateForTest() {
    forceRotateDue = true;
  }

  public long totalHandshakeGapMs() {
    return totalHandshakeGapMs.get();
  }

  public long droppedAudioMs() {
    return totalDroppedAudioMs.get();
  }

  public boolean isWaitingUtteranceEndForRotate() {
    return waitingUtteranceEnd;
  }

  public long bufferedUnfinalizedMs() {
    synchronized (lock) {
      return (long) unfinalizedBytes * 1000L / (LiveTranscriptionConfig.SAMPLE_RATE * 2);
    }
  }

  public List<RotatingLiveTranscriber.SessionInfo> sessionInfos() {
    synchronized (lock) {
      List<RotatingLiveTranscriber.SessionInfo> out = new ArrayList<>();
      for (AdkSess s : sessions) {
        out.add(
            new RotatingLiveTranscriber.SessionInfo(
                s.index,
                s.audioStartStreamMs,
                s.audioEndStreamMs,
                s.state,
                s.endReason,
                s.goAway,
                s.finals,
                0,
                0,
                String.join(" ", s.emittedTexts)));
      }
      return out;
    }
  }

  @Override
  public void close() {
    feeding = false;
    if (worker != null) {
      worker.interrupt();
    }
    synchronized (lock) {
      for (AdkSess s : sessions) {
        if (s.ws != null) {
          s.ws.close();
        }
      }
    }
    http.dispatcher().executorService().shutdown();
    http.connectionPool().evictAll();
  }

  // =============================================================================================
  // 对应 _gemini_stt.py: _transcribe / _run_session / _send / _flush
  // =============================================================================================

  private void transcribeLoop() {
    int failures = 0;
    byte[] carryFrame = null;
    long carryPosMs = -1L;

    while (!Thread.currentThread().isInterrupted()) {
      long sessionStartedWall = System.currentTimeMillis();
      AdkSess sess;
      synchronized (lock) {
        sess = new AdkSess(nextIndex++);
        sess.audioStartStreamMs = streamPosMs.get();
        sessions.add(sess);
        activeSess = sess;
      }

      try {
        // 1) 串行建立单会话（若为 S2+，此时无任何活跃连接，产生真实的串行握手空窗）
        long tConn0 = System.currentTimeMillis();
        LiveWsSession ws = openWs(sess);
        long handshakeMs = System.currentTimeMillis() - tConn0;
        if (sess.index > 1) {
          totalHandshakeGapMs.addAndGet(handshakeMs);
        }

        List<AudioFrame> replayList;
        long lostBeforeReplayMs = 0L;
        boolean wasPendingBeforeStart;
        synchronized (lock) {
          wasPendingBeforeStart = pendingLocked();
          resumed = wasPendingBeforeStart;
          speaking = false;
          partialText = "";
          goAwayFlag = false;
          activityEnded = false;
          waitingUtteranceEnd = false;

          replayList = new ArrayList<>(unfinalized);
          if (wasPendingBeforeStart && currentTurnTrimmedMs > 0) {
            lostBeforeReplayMs = currentTurnTrimmedMs;
            totalDroppedAudioMs.addAndGet(currentTurnTrimmedMs);
            currentTurnTrimmedMs = 0L;
          }
          long baseMs =
              replayList.isEmpty() ? streamPosMs.get() : replayList.get(0).streamPosMs();
          currentSessionOffsetBaseMs = baseMs;
          sess.ws = ws;
          sess.audioStartStreamMs = baseMs;
          sess.state = "ACTIVE";
        }

        if (sess.index == 1) {
          event("S1 已建立（握手 %dms）", handshakeMs);
        } else {
          double replaySec =
              replayList.stream().mapToInt(f -> f.pcm().length).sum()
                  / (double) (LiveTranscriptionConfig.SAMPLE_RATE * 2);
          event(
              "S%d 串行重启完成（单连接握手空窗 %dms，重放 %.1fs 缓冲音频）",
              sess.index, handshakeMs, replaySec);
        }
        if (lostBeforeReplayMs > 0) {
          event(
              "⚠️ [ADK 10s 重放上限触发] 断线前连续未落定语音超出 max_replay_ms=%.1fs，前 %.1fs 语音已滑出缓冲永久丢失！",
              adkMaxReplayMs / 1000.0, lostBeforeReplayMs / 1000.0);
        }

        // 2) _send: 先发手动起点（若开启）与重放帧
        if (cfg.manualVad) {
          ws.sendActivityStart();
        }
        for (AudioFrame rf : replayList) {
          if (!ws.sendAudio(rf.pcm())) {
            throw new IOException("重放帧发送失败");
          }
        }

        long liveSendStartedWall = System.currentTimeMillis();
        boolean ageNoticeLogged = false;
        boolean streamEndedNormally = false;

        // 3) 持续推流直到整段结束、收到 GoAway、或到龄且处于句间 (!pending)
        while (true) {
          if (abortRequested) {
            abortRequested = false;
            event("测试：掐断 S%d 连接", sess.index);
            ws.abortForTest();
            Thread.sleep(180);
          }
          if (ws.isDead()) {
            throw new IOException("WebSocket 连接断开");
          }

          byte[] frame;
          long frameStartMs;
          if (carryFrame != null) {
            frame = carryFrame;
            frameStartMs = carryPosMs;
            carryFrame = null;
          } else {
            frame = inbox.poll(40, TimeUnit.MILLISECONDS);
            if (frame == null) {
              if (!feeding && inbox.isEmpty()) {
                streamEndedNormally = true;
                break;
              }
              // 检查空轮询期间是否刚好服务端返回 Final 解除 pending，从而满足到龄轮换
              if (shouldRotateBetweenUtterances(liveSendStartedWall, sess, ageNoticeLogged)) {
                ageNoticeLogged = true;
                if (canCutNowLocked()) {
                  finishRotationClose(sess, ws);
                  break;
                }
              }
              continue;
            }
            frameStartMs = streamPosMs.get();
            long durMs = frame.length * 1000L / (LiveTranscriptionConfig.SAMPLE_RATE * 2);
            appendUnfinalized(new AudioFrame(frameStartMs, frame));
            streamPosMs.addAndGet(durMs);
          }

          if (!ws.sendAudio(frame)) {
            carryFrame = frame;
            carryPosMs = frameStartMs;
            throw new IOException("发送音频帧失败");
          }

          boolean goAwayNow;
          synchronized (lock) {
            goAwayNow = goAwayFlag;
          }
          if (goAwayNow) {
            synchronized (lock) {
              sess.endReason = "GoAway 立即重启";
              sess.audioEndStreamMs = streamPosMs.get();
              sess.state = "DONE";
            }
            event("S%d 收到 GoAway → 立即关闭旧连接并串行重开新会话", sess.index);
            ws.close();
            break;
          }

          if (shouldRotateBetweenUtterances(liveSendStartedWall, sess, ageNoticeLogged)) {
            ageNoticeLogged = true;
            if (canCutNowLocked()) {
              finishRotationClose(sess, ws);
              break;
            }
          }
        }

        if (streamEndedNormally) {
          flushAtEndOfStream(sess, ws);
          return;
        }

        // 正常轮换（ended == false）：回到外层 while 循环立即串行建立下一条连接
        failures = 0;

      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        synchronized (lock) {
          sess.audioEndStreamMs = streamPosMs.get();
          sess.endReason = "断线";
          sess.state = "DONE";
          waitingUtteranceEnd = false;
        }
        if (sess.ws != null) {
          sess.ws.close();
        }
        if (System.currentTimeMillis() - sessionStartedWall >= STABLE_SESSION_MS) {
          failures = 0;
        }
        failures++;
        if (failures > MAX_RECONNECTS) {
          event("连续重连超过上限 (%d 次)，终止：%s", MAX_RECONNECTS, e.getMessage());
          listener.onError(e);
          return;
        }
        double bufferedSec = bufferedUnfinalizedMs() / 1000.0;
        event(
            "S%d 异常断开（%s）→ 准备串行重开 S%d（可重放缓冲 %.1fs / 上限 %.1fs）",
            sess.index, e.getMessage(), nextIndex, bufferedSec, adkMaxReplayMs / 1000.0);
      }
    }
  }

  private boolean shouldRotateBetweenUtterances(
      long liveSendStartedWall, AdkSess sess, boolean alreadyNotified) {
    if (cfg.manualVad) {
      return false;
    }
    long elapsed = System.currentTimeMillis() - liveSendStartedWall;
    boolean due = forceRotateDue || elapsed >= cfg.rotateAfterMs;
    if (!due) {
      return false;
    }
    synchronized (lock) {
      if (pendingLocked()) {
        waitingUtteranceEnd = true;
        if (!alreadyNotified) {
          event(
              "S%d 已达轮换阈值，但当前 pending=true（句中未出 Final），ADK 模式继续等待句末...",
              sess.index);
        }
      }
    }
    return true;
  }

  private boolean canCutNowLocked() {
    synchronized (lock) {
      return !pendingLocked();
    }
  }

  private void finishRotationClose(AdkSess sess, LiveWsSession ws) {
    boolean manual = forceRotateDue;
    forceRotateDue = false;
    long pos = streamPosMs.get();
    synchronized (lock) {
      waitingUtteranceEnd = false;
      sess.audioEndStreamMs = pos;
      sess.endReason = manual ? "手动轮换(等句末)" : String.format(Locale.ROOT, "到龄 %ds(句末)", cfg.rotateAfterMs / 1000);
      sess.state = "DONE";
    }
    event("S%d 句末空闲 (!pending) → 关闭 S%d 并开始串行握手 S%d", sess.index, sess.index, nextIndex);
    ws.close();
  }

  /** 对应 _gemini_stt.py._flush：整段流结束时发送 audioStreamEnd + (silenceDurationMs+200ms) 全零帧。 */
  private void flushAtEndOfStream(AdkSess sess, LiveWsSession ws) throws InterruptedException {
    if (cfg.manualVad) {
      ws.sendActivityEnd();
      long deadline = System.currentTimeMillis() + FLUSH_TIMEOUT_MS;
      while (System.currentTimeMillis() < deadline && !ws.isDead()) {
        synchronized (lock) {
          if (activityEnded) {
            break;
          }
        }
        Thread.sleep(50);
      }
    } else {
      ws.sendAudioStreamEnd();
      ws.sendSilence(cfg.effectiveSilenceDurationMs() + cfg.flushSilenceMarginMs);
      long t0 = System.currentTimeMillis();
      long deadline = t0 + FLUSH_TIMEOUT_MS;
      while (!ws.isDead() && System.currentTimeMillis() < deadline) {
        Thread.sleep(80);
        long now = System.currentTimeMillis();
        boolean isPending;
        long lastMsg;
        synchronized (lock) {
          isPending = pendingLocked();
          lastMsg = Math.max(lastServerMsgWallMs, t0);
        }
        if (!isPending && (now - lastMsg >= FLUSH_QUIET_MS)) {
          break;
        }
      }
      boolean droppedTail;
      synchronized (lock) {
        droppedTail = pendingLocked();
      }
      if (droppedTail) {
        event("⚠️ [ADK _flush 超时] %.1fs 内未收到末尾 Final，按官方实现丢弃未落定 partial（无兜底）", FLUSH_TIMEOUT_MS / 1000.0);
      }
    }
    synchronized (lock) {
      sess.audioEndStreamMs = streamPosMs.get();
      sess.endReason = "eof";
      sess.state = "DONE";
    }
    ws.close();
    event("S%d 已收尾关闭（Final=%d，原因=eof）", sess.index, sess.finals);
  }

  // =============================================================================================
  // _AudioStream 与服务端消息回调
  // =============================================================================================

  private void appendUnfinalized(AudioFrame frame) {
    synchronized (lock) {
      unfinalized.addLast(frame);
      unfinalizedBytes += frame.pcm().length;
      int maxBytes = bytesFor(adkMaxReplayMs);
      while (!unfinalized.isEmpty()
          && unfinalizedBytes - unfinalized.peekFirst().pcm().length >= maxBytes) {
        AudioFrame removed = unfinalized.removeFirst();
        unfinalizedBytes -= removed.pcm().length;
        long removedMs =
            removed.pcm().length * 1000L / (LiveTranscriptionConfig.SAMPLE_RATE * 2);
        // 若移除的帧位于上一句 Final 结束点之后，说明当前长句未落定语音正在滑出 10s 缓冲窗
        if (removed.streamPosMs() + removedMs > lastFinalEndStreamMs) {
          currentTurnTrimmedMs += removedMs;
        }
      }
    }
  }

  private void markFinalizedLocked() {
    currentTurnTrimmedMs = 0L;
    int keepBytes = bytesFor(tailMs);
    while (!unfinalized.isEmpty()
        && unfinalizedBytes - unfinalized.peekFirst().pcm().length >= keepBytes) {
      AudioFrame removed = unfinalized.removeFirst();
      unfinalizedBytes -= removed.pcm().length;
    }
  }

  private static int bytesFor(long durationMs) {
    return (int) (LiveTranscriptionConfig.SAMPLE_RATE * durationMs / 1000L * 2L);
  }

  private boolean pendingLocked() {
    return speaking || !partialText.isBlank();
  }

  private LiveWsSession openWs(AdkSess sess) throws Exception {
    String token;
    synchronized (credentials) {
      credentials.refreshIfExpired();
      token = credentials.getAccessToken().getTokenValue();
    }
    return LiveWsSession.open(
        sess.index,
        http,
        cfg,
        token,
        new LiveWsSession.Callback() {
          @Override
          public void onSetupComplete(LiveWsSession s) {}

          @Override
          public void onServerMessage(LiveWsSession s) {
            synchronized (lock) {
              if (sess != activeSess) return;
              lastServerMsgWallMs = System.currentTimeMillis();
            }
          }

          @Override
          public void onVoiceActivity(LiveWsSession s, boolean active) {
            synchronized (lock) {
              if (sess != activeSess) return;
              if (active) {
                turnHadFinal = false;
                startSpeechLocked();
              } else {
                speaking = false;
                activityEnded = true;
              }
            }
          }

          @Override
          public void onInterim(LiveWsSession s, String text, long startMs, long endMs) {
            if (text == null || text.isBlank()) {
              return;
            }
            String trimmed = text.trim();
            long gStart;
            long gEnd;
            synchronized (lock) {
              if (sess != activeSess) return;
              turnHadFinal = false;
              startSpeechLocked();
              if (startMs >= 0) {
                pendingStartOffset = startMs;
              }
              if (endMs >= 0) {
                pendingEndOffset = endMs;
              }
              if (trimmed.equals(partialText)) {
                return;
              }
              partialText = trimmed;
              gStart =
                  pendingStartOffset >= 0
                      ? currentSessionOffsetBaseMs + pendingStartOffset
                      : lastFinalEndStreamMs;
              gEnd =
                  pendingEndOffset >= 0
                      ? currentSessionOffsetBaseMs + pendingEndOffset
                      : streamPosMs.get();
            }
            listener.onInterim(
                new TranscriptEvent(
                    TranscriptEvent.Type.INTERIM,
                    TranscriptEvent.Source.SERVER,
                    trimmed,
                    gStart,
                    gEnd,
                    sess.index));
          }

          @Override
          public void onFinal(LiveWsSession s, String text) {
            if (text == null || text.isBlank()) {
              return;
            }
            String trimmed = text.trim();
            long gStart;
            long gEnd;
            synchronized (lock) {
              if (sess != activeSess) return;
              gStart =
                  pendingStartOffset >= 0
                      ? currentSessionOffsetBaseMs + pendingStartOffset
                      : lastFinalEndStreamMs;
              gEnd =
                  pendingEndOffset >= 0
                      ? currentSessionOffsetBaseMs + pendingEndOffset
                      : streamPosMs.get();
              lastFinalEndStreamMs = Math.max(lastFinalEndStreamMs, gEnd);
              sess.finals++;
              sess.emittedTexts.add(trimmed);
              turnHadFinal = true;
              finalizeTurnLocked();
            }
            listener.onFinal(
                new TranscriptEvent(
                    TranscriptEvent.Type.FINAL,
                    TranscriptEvent.Source.SERVER,
                    trimmed,
                    gStart,
                    gEnd,
                    sess.index));
          }

          @Override
          public void onGenerationComplete(LiveWsSession s) {
            synchronized (lock) {
              if (sess != activeSess) return;
              if (!turnHadFinal && partialText.isBlank()) {
                lastFinalEndStreamMs = Math.max(lastFinalEndStreamMs, streamPosMs.get() - tailMs);
                finalizeTurnLocked();
              } else {
                speaking = false;
                partialText = "";
                turnHadFinal = false;
              }
            }
          }

          @Override
          public void onGoAway(LiveWsSession s, long timeLeftMs) {
            synchronized (lock) {
              if (sess != activeSess) return;
              goAwayFlag = true;
              sess.goAway = true;
            }
          }

          @Override
          public void onClosed(LiveWsSession s, boolean initiatedByClient, String reason) {
            if (!initiatedByClient) {
              event("S%d 连接被远端关闭：%s", sess.index, reason);
            }
          }
        });
  }

  private void startSpeechLocked() {
    if (!speaking) {
      speaking = true;
      activityEnded = false;
      if (resumed) {
        resumed = false;
      }
    }
  }

  private void finalizeTurnLocked() {
    speaking = false;
    partialText = "";
    pendingStartOffset = -1L;
    pendingEndOffset = -1L;
    resumed = false;
    markFinalizedLocked();
  }

  private void event(String fmt, Object... args) {
    listener.onSessionEvent(
        String.format(Locale.ROOT, "[stream %.1fs] ", streamPosMs.get() / 1000.0)
            + String.format(Locale.ROOT, fmt, args));
  }
}
