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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.genai.types.AudioTranscriptionConfig;
import com.google.genai.types.AutomaticActivityDetection;
import com.google.genai.types.EndSensitivity;
import com.google.genai.types.RealtimeInputConfig;
import com.google.genai.types.StartSensitivity;
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

/**
 * 单个 Live WebSocket 会话：优先复用官方 {@code com.google.genai.types} 强类型配置模型序列化，
 * 仅在底层 WebSocket 通道做薄封装以补齐 {@code google-genai:1.72.0} 缺失的断连检测与中间时间戳开关。
 */
final class LiveWsSession {

  /** 对齐官方 Python {@code _gemini_stt.py._TRANSIENT_CLOSE_CODES}。 */
  static final Set<Integer> TRANSIENT_CLOSE_CODES =
      Set.of(1000, 1001, 1006, 1011, 1012, 1013, 1014);

  /** 对齐官方 Python {@code _gemini_stt.py._TRANSIENT_STATUS_CODES}。 */
  static final Set<Integer> TRANSIENT_HTTP_CODES = Set.of(408, 429, 500, 502, 503, 504);

  /** 建连握手异常，携带 WebSocket close code 与 HTTP 状态码供上层判断是否属于可重试瞬态故障。 */
  static final class ConnectFailureException extends IllegalStateException {
    final int closeCode;
    final int httpStatus;

    ConnectFailureException(String message, int closeCode, int httpStatus) {
      super(message);
      this.closeCode = closeCode;
      this.httpStatus = httpStatus;
    }

    boolean isTransient() {
      return isTransientCode(closeCode, httpStatus);
    }
  }

  /** 对标官方 Python {@code _gemini_stt.py._is_transient}：判定关闭码或 HTTP 状态码是否属于可恢复瞬态错误。 */
  static boolean isTransientCode(int wsCloseCode, int httpStatus) {
    if (httpStatus > 0) {
      return TRANSIENT_HTTP_CODES.contains(httpStatus);
    }
    if (wsCloseCode > 0) {
      return TRANSIENT_CLOSE_CODES.contains(wsCloseCode);
    }
    // 无 HTTP 状态码且无标准 close 帧（如底层 TCP 重置 / IO 超时），对应 _gemini_stt.py 视为 1006 / ConnectionError
    return true;
  }

  /** 会话事件回调（在 OkHttp 线程上调用）。 */
  interface Callback {
    void onSetupComplete(LiveWsSession s);

    /** offset 为会话内相对时间（毫秒），未知为 -1。 */
    void onInterim(LiveWsSession s, String text, long startOffsetMs, long endOffsetMs);

    default void onInterim(
        LiveWsSession s, String text, long startOffsetMs, long endOffsetMs, String rawJson) {
      onInterim(s, text, startOffsetMs, endOffsetMs);
    }

    void onFinal(LiveWsSession s, String text);

    default void onFinal(LiveWsSession s, String text, String rawJson) {
      onFinal(s, text);
    }

    void onGoAway(LiveWsSession s, long timeLeftMs);

    /** 连接结束。{@code initiatedByClient=false} 表示服务端关闭或网络故障。 */
    void onClosed(LiveWsSession s, boolean initiatedByClient, String reason);

    /** 收到任意服务端消息（用于对标官方 Python _gemini_stt.py 的 message_received 事件）。 */
    default void onServerMessage(LiveWsSession s) {}

    /** 服务端 VAD 状态变化（ACTIVITY_START=true, ACTIVITY_END=false）。 */
    default void onVoiceActivity(LiveWsSession s, boolean active) {}

    /** 服务端确认当前话轮结束（generationComplete=true，无论是否包含有效词）。 */
    default void onGenerationComplete(LiveWsSession s) {}
  }

  private static final ObjectMapper JSON = new ObjectMapper();

  final int index;
  final long openedWallMs = System.currentTimeMillis();
  final CompletableFuture<Void> setupComplete = new CompletableFuture<>();
  private final Callback callback;
  private final WebSocket ws;
  private volatile boolean clientClosing = false;
  private volatile boolean dead = false;
  private volatile boolean closedNotified = false;
  private volatile int lastCloseCode = -1;
  private volatile int lastHttpStatus = -1;

  private LiveWsSession(
      int index, OkHttpClient http, LiveTranscriptionConfig cfg, String accessToken, Callback cb) {
    this.index = index;
    this.callback = cb;
    String host =
        "global".equals(cfg.location) ? "aiplatform.googleapis.com"
            : cfg.location + "-aiplatform.googleapis.com";
    String url =
        "wss://" + host + "/ws/google.cloud.aiplatform." + cfg.apiVersion
            + ".LlmBidiService/BidiGenerateContent";
    Request req =
        new Request.Builder()
            .url(url)
            .header("Authorization", "Bearer " + accessToken)
            .header("x-goog-user-project", cfg.project)
            .build();
    String setup = buildSetup(cfg);
    this.ws = http.newWebSocket(req, new Listener(setup));
  }

  /** 建立会话并等待 setupComplete。 */
  static LiveWsSession open(
      int index, OkHttpClient http, LiveTranscriptionConfig cfg, String accessToken, Callback cb)
      throws Exception {
    LiveWsSession s = new LiveWsSession(index, http, cfg, accessToken, cb);
    try {
      s.setupComplete.get(cfg.connectTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      s.ws.cancel();
      if (e.getCause() instanceof Exception cause) {
        throw cause;
      }
      throw e;
    } catch (Exception e) {
      s.ws.cancel();
      throw e;
    }
    return s;
  }

  /**
   * 优先使用官方 {@code com.google.genai.types}（{@link AudioTranscriptionConfig}、
   * {@link AutomaticActivityDetection}、{@link RealtimeInputConfig}）构建并序列化协议 JSON，
   * 仅对 SDK 1.72.0 尚未暴露的 {@code interimTranscriptTimestampEnabled} 布尔位做增量补充。
   */
  static String buildSetup(LiveTranscriptionConfig cfg) {
    try {
      ObjectNode root = JSON.createObjectNode();
      ObjectNode setup = root.putObject("setup");
      setup.put(
          "model",
          "projects/" + cfg.project + "/locations/" + cfg.location + "/publishers/google/models/"
              + cfg.model);
      setup.putObject("generationConfig").putArray("responseModalities").add("TEXT");

      AudioTranscriptionConfig.Builder iatBuilder = AudioTranscriptionConfig.builder();
      if (!cfg.languageCodes.isEmpty()) {
        iatBuilder.languageCodes(cfg.languageCodes);
      }
      if (!cfg.customVocabulary.isEmpty()) {
        iatBuilder.customVocabulary(cfg.customVocabulary);
      }
      setup.set("inputAudioTranscription", JSON.readTree(iatBuilder.build().toJson()));

      boolean hasAad =
          cfg.manualVad
              || cfg.silenceDurationMs > 0
              || cfg.prefixPaddingMs >= 0
              || cfg.startOfSpeechSensitivity.wireValue != null
              || cfg.endOfSpeechSensitivity.wireValue != null;
      if (cfg.interimTimestamps || hasAad) {
        ObjectNode ric;
        if (hasAad) {
          AutomaticActivityDetection.Builder aadBuilder = AutomaticActivityDetection.builder();
          if (cfg.manualVad) {
            aadBuilder.disabled(true);
          } else {
            if (cfg.startOfSpeechSensitivity.wireValue != null) {
              aadBuilder.startOfSpeechSensitivity(
                  new StartSensitivity(cfg.startOfSpeechSensitivity.wireValue));
            }
            if (cfg.endOfSpeechSensitivity.wireValue != null) {
              aadBuilder.endOfSpeechSensitivity(
                  new EndSensitivity(cfg.endOfSpeechSensitivity.wireValue));
            }
            if (cfg.prefixPaddingMs >= 0) {
              aadBuilder.prefixPaddingMs(cfg.prefixPaddingMs);
            }
            if (cfg.silenceDurationMs > 0) {
              aadBuilder.silenceDurationMs(cfg.silenceDurationMs);
            }
          }
          RealtimeInputConfig sdkRic =
              RealtimeInputConfig.builder()
                  .automaticActivityDetection(aadBuilder.build())
                  .build();
          ric = (ObjectNode) JSON.readTree(sdkRic.toJson());
        } else {
          ric = JSON.createObjectNode();
        }
        if (cfg.interimTimestamps) {
          ObjectNode orderedRic = JSON.createObjectNode();
          orderedRic.put("interimTranscriptTimestampEnabled", true);
          orderedRic.setAll(ric);
          ric = orderedRic;
        }
        setup.set("realtimeInputConfig", ric);
      }
      return root.toString();
    } catch (java.io.IOException e) {
      throw new IllegalStateException("序列化 Live setup 配置失败", e);
    }
  }

  boolean isDead() {
    return dead;
  }

  /** 发送一帧 PCM；连接已不可用时返回 false 并标记为 dead。 */
  boolean sendAudio(byte[] pcm) {
    if (dead) {
      return false;
    }
    String msg =
        "{\"realtimeInput\":{\"audio\":{\"data\":\"" + Base64.getEncoder().encodeToString(pcm)
            + "\",\"mimeType\":\"audio/pcm;rate=" + LiveTranscriptionConfig.SAMPLE_RATE + "\"}}}";
    boolean ok = ws.send(msg);
    if (!ok) {
      markDead("send failed (socket closed)");
    }
    return ok;
  }

  boolean sendAudioStreamEnd() {
    return !dead && ws.send("{\"realtimeInput\":{\"audioStreamEnd\":true}}");
  }

  boolean sendActivityStart() {
    return !dead && ws.send("{\"realtimeInput\":{\"activityStart\":{}}}");
  }

  boolean sendActivityEnd() {
    return !dead && ws.send("{\"realtimeInput\":{\"activityEnd\":{}}}");
  }

  /**
   * 对应官方 Python {@code _gemini_stt.py._flush}：发送 {@code durationMs} 毫秒的全零 PCM 静音帧，
   * 确保服务端流式 Tokenizer 即使尚未处理到最后一帧语音（因而忽略了 {@code audioStreamEnd}），
   * 也能被紧随其后的静音帧正常触发 EOS 落定 Final。
   */
  boolean sendSilence(long durationMs) {
    if (dead || durationMs <= 0) {
      return !dead;
    }
    int frameBytes = LiveTranscriptionConfig.SAMPLE_RATE * 2 * 100 / 1000; // 100ms = 3200B
    byte[] zeroFrame = new byte[frameBytes];
    long remainingMs = durationMs;
    while (remainingMs > 0 && !dead) {
      long chunkMs = Math.min(100, remainingMs);
      byte[] chunk =
          chunkMs == 100
              ? zeroFrame
              : new byte[(int) (LiveTranscriptionConfig.SAMPLE_RATE * 2 * chunkMs / 1000)];
      if (!sendAudio(chunk)) {
        return false;
      }
      remainingMs -= chunkMs;
    }
    return true;
  }

  /** 客户端主动正常关闭。 */
  void close() {
    clientClosing = true;
    ws.close(1000, "client rotation");
  }

  /** 立即中断连接（模拟网络故障，仅用于测试）。 */
  void abortForTest() {
    ws.cancel();
  }

  boolean isTransientFailure() {
    return isTransientCode(lastCloseCode, lastHttpStatus);
  }

  private void markDead(String reason) {
    markDead(reason, 1006, -1);
  }

  private void markDead(String reason, int closeCode, int httpStatus) {
    if (closeCode > 0) {
      this.lastCloseCode = closeCode;
    }
    if (httpStatus > 0) {
      this.lastHttpStatus = httpStatus;
    }
    dead = true;
    if (!closedNotified) {
      closedNotified = true;
      setupComplete.completeExceptionally(
          new ConnectFailureException(reason, this.lastCloseCode, this.lastHttpStatus));
      callback.onClosed(this, clientClosing, reason);
    }
  }

  static long parseDurationMs(JsonNode n) {
    if (n == null || n.isNull()) {
      return -1;
    }
    String t = n.asText().trim();
    if (t.endsWith("s")) {
      t = t.substring(0, t.length() - 1);
    }
    try {
      return Math.round(Double.parseDouble(t) * 1000);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private static final java.io.PrintWriter RAW_TAP = openRawTap();

  private static java.io.PrintWriter openRawTap() {
    String p = System.getProperty("live.rawLog");
    if (p == null || p.isBlank()) {
      return null;
    }
    try {
      return new java.io.PrintWriter(java.nio.file.Files.newBufferedWriter(java.nio.file.Path.of(p),
          java.nio.charset.StandardCharsets.UTF_8), true);
    } catch (java.io.IOException e) {
      return null;
    }
  }

  private void handle(String text) {
    if (RAW_TAP != null) {
      synchronized (RAW_TAP) {
        RAW_TAP.println(java.time.Instant.now() + "\tS" + index + "\t" + text.replace('\n', ' '));
      }
    }
    JsonNode root;
    try {
      root = JSON.readTree(text);
    } catch (Exception e) {
      return;
    }
    callback.onServerMessage(this);
    if (root.has("setupComplete")) {
      setupComplete.complete(null);
      callback.onSetupComplete(this);
    }
    JsonNode goAway = root.get("goAway");
    if (goAway != null) {
      callback.onGoAway(this, parseDurationMs(goAway.get("timeLeft")));
    }
    JsonNode va = root.get("voiceActivity");
    if (va != null && va.hasNonNull("voiceActivityType")) {
      String type = va.get("voiceActivityType").asText();
      if ("ACTIVITY_START".equals(type) || "VOICE_ACTIVITY_TYPE_START".equals(type)) {
        callback.onVoiceActivity(this, true);
      } else if ("ACTIVITY_END".equals(type) || "VOICE_ACTIVITY_TYPE_END".equals(type)) {
        callback.onVoiceActivity(this, false);
      }
    }
    JsonNode sc = root.get("serverContent");
    if (sc != null) {
      JsonNode interim = sc.get("interimInputTranscription");
      if (interim != null && interim.hasNonNull("text")) {
        callback.onInterim(
            this,
            interim.get("text").asText(),
            parseDurationMs(interim.get("startOffset")),
            parseDurationMs(interim.get("endOffset")),
            text);
      }
      JsonNode fin = sc.get("inputTranscription");
      if (fin != null && fin.hasNonNull("text") && !fin.get("text").asText().isBlank()) {
        callback.onFinal(this, fin.get("text").asText().trim(), text);
      }
      if (sc.path("generationComplete").asBoolean(false)) {
        callback.onGenerationComplete(this);
      }
    }
  }

  private final class Listener extends WebSocketListener {
    private final String setup;

    Listener(String setup) {
      this.setup = setup;
    }

    @Override
    public void onOpen(WebSocket webSocket, Response response) {
      webSocket.send(setup);
    }

    @Override
    public void onMessage(WebSocket webSocket, String text) {
      handle(text);
    }

    @Override
    public void onMessage(WebSocket webSocket, ByteString bytes) {
      handle(bytes.utf8());
    }

    @Override
    public void onClosing(WebSocket webSocket, int code, String reason) {
      webSocket.close(1000, null);
      markDead("closing code=" + code + " reason=" + reason, code, -1);
    }

    @Override
    public void onClosed(WebSocket webSocket, int code, String reason) {
      markDead("closed code=" + code + " reason=" + reason, code, -1);
    }

    @Override
    public void onFailure(WebSocket webSocket, Throwable t, Response response) {
      int httpCode = response != null ? response.code() : -1;
      markDead("failure " + t + (httpCode > 0 ? " http=" + httpCode : ""), 1006, httpCode);
    }
  }
}
