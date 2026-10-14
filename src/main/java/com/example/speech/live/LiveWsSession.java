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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

/**
 * 单个 Live WebSocket 会话（直接走 Vertex AI BidiGenerateContent 协议，不经过 google-genai SDK）。
 *
 * <p>为什么不用 SDK：SDK 1.72.0 (1) 不支持 {@code realtimeInputConfig.interimTranscriptTimestampEnabled}；
 * (2) 会话建立后服务端关闭连接时不通知业务代码，{@code sendRealtimeInput()} 也不报错，音频被静默丢弃。
 * 直接使用 WebSocket 可以拿到关闭帧和发送失败，从而做断线恢复。
 */
final class LiveWsSession {

  /** 会话事件回调（在 OkHttp 线程上调用）。 */
  interface Callback {
    void onSetupComplete(LiveWsSession s);

    /** offset 为会话内相对时间（毫秒），未知为 -1。 */
    void onInterim(LiveWsSession s, String text, long startOffsetMs, long endOffsetMs);

    void onFinal(LiveWsSession s, String text);

    void onGoAway(LiveWsSession s, long timeLeftMs);

    /** 连接结束。{@code initiatedByClient=false} 表示服务端关闭或网络故障。 */
    void onClosed(LiveWsSession s, boolean initiatedByClient, String reason);
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
    } catch (Exception e) {
      s.ws.cancel();
      throw e;
    }
    return s;
  }

  static String buildSetup(LiveTranscriptionConfig cfg) {
    ObjectNode root = JSON.createObjectNode();
    ObjectNode setup = root.putObject("setup");
    setup.put(
        "model",
        "projects/" + cfg.project + "/locations/" + cfg.location + "/publishers/google/models/"
            + cfg.model);
    setup.putObject("generationConfig").putArray("responseModalities").add("TEXT");
    ObjectNode iat = setup.putObject("inputAudioTranscription");
    if (!cfg.languageCodes.isEmpty()) {
      ArrayNode langs = iat.putArray("languageCodes");
      cfg.languageCodes.forEach(langs::add);
    }
    if (cfg.interimTimestamps) {
      setup.putObject("realtimeInputConfig").put("interimTranscriptTimestampEnabled", true);
    }
    return root.toString();
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

  /** 客户端主动正常关闭。 */
  void close() {
    clientClosing = true;
    ws.close(1000, "client rotation");
  }

  /** 立即中断连接（模拟网络故障，仅用于测试）。 */
  void abortForTest() {
    ws.cancel();
  }

  private void markDead(String reason) {
    dead = true;
    if (!closedNotified) {
      closedNotified = true;
      setupComplete.completeExceptionally(new IllegalStateException(reason));
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
    if (root.has("setupComplete")) {
      setupComplete.complete(null);
      callback.onSetupComplete(this);
    }
    JsonNode goAway = root.get("goAway");
    if (goAway != null) {
      callback.onGoAway(this, parseDurationMs(goAway.get("timeLeft")));
    }
    JsonNode sc = root.get("serverContent");
    if (sc != null) {
      JsonNode interim = sc.get("interimInputTranscription");
      if (interim != null && interim.hasNonNull("text")) {
        callback.onInterim(this, interim.get("text").asText(),
            parseDurationMs(interim.get("startOffset")), parseDurationMs(interim.get("endOffset")));
      }
      JsonNode fin = sc.get("inputTranscription");
      if (fin != null && fin.hasNonNull("text") && !fin.get("text").asText().isBlank()) {
        callback.onFinal(this, fin.get("text").asText().trim());
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
      markDead("closing code=" + code + " reason=" + reason);
    }

    @Override
    public void onClosed(WebSocket webSocket, int code, String reason) {
      markDead("closed code=" + code + " reason=" + reason);
    }

    @Override
    public void onFailure(WebSocket webSocket, Throwable t, Response response) {
      markDead("failure " + t + (response != null ? " http=" + response.code() : ""));
    }
  }
}
