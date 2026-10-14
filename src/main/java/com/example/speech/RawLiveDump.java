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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.genai.AsyncSession;
import com.google.genai.Client;
import com.google.genai.JsonSerializable;
import com.google.genai.types.AudioTranscriptionConfig;
import com.google.genai.types.Blob;
import com.google.genai.types.LiveConnectConfig;
import com.google.genai.types.LiveSendRealtimeInputParameters;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;

/**
 * 原始返回抓取：单个 Live 会话，按实时节奏推送整段 WAV（不轮换、不处理 GoAway、不做任何业务逻辑），
 * 音频推完后发送 audioStreamEnd，再等待 30s，把服务端的每一条消息原样（JSON）写入文件。
 *
 * <pre>
 *   mvn exec:java -Dexec.mainClass=com.example.speech.RawLiveDump \
 *     -Dexec.args="test_video_630s.wav logs/raw_live_dump.jsonl"
 * </pre>
 */
public final class RawLiveDump {

  static {
    // 仅为让 SDK 能解析服务端 "30s" 形式的 Duration（否则含 goAway 的消息会解析失败而丢失）
    SimpleModule m = new SimpleModule();
    m.addDeserializer(
        Duration.class,
        new StdDeserializer<>(Duration.class) {
          @Override
          public Duration deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            String t = p.getText();
            if (t == null || t.isBlank()) {
              return null;
            }
            t = t.trim();
            if (t.endsWith("s") && !t.startsWith("PT")) {
              return Duration.ofNanos(
                  Math.round(Double.parseDouble(t.substring(0, t.length() - 1)) * 1e9));
            }
            return Duration.parse(t);
          }
        });
    JsonSerializable.objectMapper().registerModule(m);
  }

  public static void main(String[] args) throws Exception {
    String model = System.getProperty("model", "gemini-3.5-transcribe-live");
    Path wav = Path.of(args.length > 0 ? args[0] : "test_video_630s.wav");
    Path out = Path.of(args.length > 1 ? args[1] : "logs/raw_live_dump.jsonl");
    Files.createDirectories(out.toAbsolutePath().getParent());

    byte[] pcm;
    try (AudioInputStream in = AudioSystem.getAudioInputStream(wav.toFile())) {
      pcm = in.readAllBytes(); // 需为 16kHz / mono / 16-bit PCM
    }

    long t0 = System.currentTimeMillis();
    try (PrintWriter w =
            new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8), true);
        Client client =
            Client.builder()
                .enterprise(true)
                .project(System.getenv().getOrDefault("GOOGLE_CLOUD_PROJECT", "cloud-llm-preview1"))
                .location(System.getenv().getOrDefault("GOOGLE_CLOUD_REGION", "global"))
                .build()) {

      LiveConnectConfig config =
          LiveConnectConfig.builder()
              .responseModalities("TEXT")
              .inputAudioTranscription(AudioTranscriptionConfig.builder().build())
              .build();

      log(w, t0, "CLIENT", "connect model=" + model + " audio=" + wav + " bytes=" + pcm.length);
      AsyncSession session = client.async.live.connect(model, config).get(30, TimeUnit.SECONDS);
      log(w, t0, "CLIENT", "setupComplete");

      session.receive(msg -> log(w, t0, "SERVER", msg.toJson()));

      int chunk = 3200; // 100ms
      long start = System.nanoTime();
      for (int i = 0; i * chunk < pcm.length; i++) {
        long due = start + i * 100_000_000L;
        long wait = due - System.nanoTime();
        if (wait > 0) {
          Thread.sleep(wait / 1_000_000L, (int) (wait % 1_000_000L));
        }
        int len = Math.min(chunk, pcm.length - i * chunk);
        byte[] b = new byte[len];
        System.arraycopy(pcm, i * chunk, b, 0, len);
        try {
          session
              .sendRealtimeInput(
                  LiveSendRealtimeInputParameters.builder()
                      .audio(Blob.builder().data(b).mimeType("audio/pcm;rate=16000").build())
                      .build())
              .join();
        } catch (Exception e) {
          log(w, t0, "CLIENT", "send error at chunk " + i + ": " + e);
        }
      }
      log(w, t0, "CLIENT", "audio sent (" + pcm.length / 32000.0 + "s), sending audioStreamEnd");
      try {
        session
            .sendRealtimeInput(LiveSendRealtimeInputParameters.builder().audioStreamEnd(true).build())
            .join();
      } catch (Exception e) {
        log(w, t0, "CLIENT", "audioStreamEnd error: " + e);
      }
      Thread.sleep(30_000);
      log(w, t0, "CLIENT", "closing");
      try {
        session.close().get(5, TimeUnit.SECONDS);
      } catch (Exception ignored) {
        // ignore
      }
    }
    System.exit(0);
  }

  private static synchronized void log(PrintWriter w, long t0, String who, String text) {
    String line =
        String.format(Locale.ROOT, "%s\t+%.1fs\t%s\t%s", Instant.now(),
            (System.currentTimeMillis() - t0) / 1000.0, who, text.replace('\n', ' '));
    w.println(line);
  }
}
