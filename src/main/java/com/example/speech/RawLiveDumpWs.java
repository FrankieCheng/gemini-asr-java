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
package com.example.speech;

import com.google.auth.oauth2.GoogleCredentials;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Base64;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

/**
 * 原始 WebSocket 抓取（不经过 google-genai SDK）：SDK 1.72.0 的 RealtimeInputConfig 还没有
 * {@code interimTranscriptTimestampEnabled} 字段，因此直接手写 setup JSON。
 *
 * <p>单个会话，按实时节奏推送整段 WAV，推完发送 audioStreamEnd，再等 30s 后关闭；
 * 服务端每一帧原样写入文件，不做任何其他处理。
 *
 * <pre>
 *   mvn exec:java -Dexec.mainClass=com.example.speech.RawLiveDumpWs \
 *     -Dexec.args="test_video_630s.wav logs/raw_live_dump_interim_ts.jsonl true"
 * </pre>
 * 第 3 个参数：是否设置 realtimeInputConfig.interimTranscriptTimestampEnabled=true。
 */
public final class RawLiveDumpWs {

  public static void main(String[] args) throws Exception {
    String model = System.getProperty("model", "gemini-3.5-transcribe-live");
    String project = System.getenv().getOrDefault("GOOGLE_CLOUD_PROJECT", "cloud-llm-preview1");
    String location = System.getenv().getOrDefault("GOOGLE_CLOUD_REGION", "global");
    Path wav = Path.of(args.length > 0 ? args[0] : "test_video_630s.wav");
    Path out = Path.of(args.length > 1 ? args[1] : "logs/raw_live_dump_interim_ts.jsonl");
    boolean interimTs = args.length <= 2 || Boolean.parseBoolean(args[2]);
    Files.createDirectories(out.toAbsolutePath().getParent());

    byte[] pcm;
    try (AudioInputStream in = AudioSystem.getAudioInputStream(wav.toFile())) {
      pcm = in.readAllBytes(); // 16kHz / mono / 16-bit PCM
    }

    GoogleCredentials cred =
        GoogleCredentials.getApplicationDefault()
            .createScoped("https://www.googleapis.com/auth/cloud-platform");
    cred.refreshIfExpired();
    String token = cred.getAccessToken().getTokenValue();

    String host =
        "global".equals(location) ? "aiplatform.googleapis.com"
            : location + "-aiplatform.googleapis.com";
    String url =
        "wss://" + host
            + "/ws/google.cloud.aiplatform.v1beta1.LlmBidiService/BidiGenerateContent";
    String modelPath =
        "projects/" + project + "/locations/" + location + "/publishers/google/models/" + model;
    String setup =
        "{\"setup\":{\"model\":\"" + modelPath + "\","
            + "\"generationConfig\":{\"responseModalities\":[\"TEXT\"]},"
            + "\"inputAudioTranscription\":{}"
            + (interimTs ? ",\"realtimeInputConfig\":{\"interimTranscriptTimestampEnabled\":true}" : "")
            + "}}";

    long t0 = System.currentTimeMillis();
    PrintWriter w = new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8), true);
    CountDownLatch setupDone = new CountDownLatch(1);

    OkHttpClient http = new OkHttpClient.Builder().readTimeout(0, TimeUnit.MILLISECONDS).build();
    Request req =
        new Request.Builder()
            .url(url)
            .header("Authorization", "Bearer " + token)
            .header("x-goog-user-project", project)
            .build();

    log(w, t0, "CLIENT", "connect " + url);
    log(w, t0, "CLIENT", "setup " + setup);
    WebSocket ws =
        http.newWebSocket(
            req,
            new WebSocketListener() {
              @Override
              public void onOpen(WebSocket webSocket, Response response) {
                log(w, t0, "CLIENT", "ws open http=" + response.code());
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

              private void handle(String text) {
                log(w, t0, "SERVER", text);
                if (text.contains("\"setupComplete\"")) {
                  setupDone.countDown();
                }
              }

              @Override
              public void onClosing(WebSocket webSocket, int code, String reason) {
                log(w, t0, "CLIENT", "ws onClosing code=" + code + " reason=" + reason);
              }

              @Override
              public void onClosed(WebSocket webSocket, int code, String reason) {
                log(w, t0, "CLIENT", "ws onClosed code=" + code + " reason=" + reason);
              }

              @Override
              public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                log(w, t0, "CLIENT", "ws onFailure " + t
                    + (response != null ? " http=" + response.code() : ""));
                setupDone.countDown();
              }
            });

    if (!setupDone.await(30, TimeUnit.SECONDS)) {
      log(w, t0, "CLIENT", "setupComplete not received in 30s");
    }

    int chunk = 3200; // 100ms
    long start = System.nanoTime();
    for (int i = 0; i * chunk < pcm.length; i++) {
      long wait = start + i * 100_000_000L - System.nanoTime();
      if (wait > 0) {
        Thread.sleep(wait / 1_000_000L, (int) (wait % 1_000_000L));
      }
      int len = Math.min(chunk, pcm.length - i * chunk);
      byte[] b = new byte[len];
      System.arraycopy(pcm, i * chunk, b, 0, len);
      boolean ok =
          ws.send("{\"realtimeInput\":{\"audio\":{\"data\":\""
              + Base64.getEncoder().encodeToString(b)
              + "\",\"mimeType\":\"audio/pcm;rate=16000\"}}}");
      if (!ok) {
        log(w, t0, "CLIENT", "send returned false at chunk " + i);
      }
    }
    log(w, t0, "CLIENT", "audio sent (" + pcm.length / 32000.0 + "s), sending audioStreamEnd");
    ws.send("{\"realtimeInput\":{\"audioStreamEnd\":true}}");
    Thread.sleep(30_000);
    log(w, t0, "CLIENT", "closing");
    ws.close(1000, "done");
    Thread.sleep(2000);
    w.close();
    System.exit(0);
  }

  private static synchronized void log(PrintWriter w, long t0, String who, String text) {
    w.println(
        String.format(Locale.ROOT, "%s\t+%.1fs\t%s\t%s", Instant.now(),
            (System.currentTimeMillis() - t0) / 1000.0, who, text.replace('\n', ' ')));
  }
}
