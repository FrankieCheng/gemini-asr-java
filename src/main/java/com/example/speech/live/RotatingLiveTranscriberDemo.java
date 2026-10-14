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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;

/**
 * {@link RotatingLiveTranscriber} 的演示 / 验收程序：把 WAV 文件按实时节奏 feed 进去，模拟直播音频源。
 *
 * <p>输出：
 * <ul>
 *   <li>{@code logs/<name>.events.jsonl}：全部事件（INTERIM / FINAL / SESSION），带全程时间戳</li>
 *   <li>{@code logs/<name>.tsv}：每个会话的文本汇总 + MERGED 全文拼接行</li>
 * </ul>
 *
 * <pre>
 *   mvn exec:java -Dexec.mainClass=com.example.speech.live.RotatingLiveTranscriberDemo \
 *     -Dexec.args="--audio=test_video_630s.wav --name=live_demo"
 *   # 模拟第 300 秒网络断开，验证断线恢复：
 *     -Dexec.args="--audio=test_video_630s.wav --name=live_kill --killAtSec=300"
 * </pre>
 */
public final class RotatingLiveTranscriberDemo {

  private static final ObjectMapper JSON = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    String audio = "test_video_630s.wav";
    String name = "live_demo";
    int killAtSec = -1;
    LiveTranscriptionConfig.Builder b =
        LiveTranscriptionConfig.builder()
            .project(System.getenv().getOrDefault("GOOGLE_CLOUD_PROJECT", "cloud-llm-preview1"))
            .location(System.getenv().getOrDefault("GOOGLE_CLOUD_REGION", "global"));
    for (String a : args) {
      String v = a.substring(a.indexOf('=') + 1);
      if (a.startsWith("--audio=")) {
        audio = v;
      } else if (a.startsWith("--name=")) {
        name = v;
      } else if (a.startsWith("--killAtSec=")) {
        killAtSec = Integer.parseInt(v);
      } else if (a.startsWith("--model=")) {
        b.model(v);
      } else if (a.startsWith("--rotateAfterSec=")) {
        b.rotateAfterMs(Long.parseLong(v) * 1000);
      } else if (a.startsWith("--stallSec=")) {
        b.stallMs(Long.parseLong(v) * 1000);
      } else if (a.startsWith("--flushSec=")) {
        b.flushAfterMs(Long.parseLong(v) * 1000);
      } else if (a.startsWith("--silenceRms=")) {
        b.silenceRms(Integer.parseInt(v));
      } else if (a.startsWith("--adaptive=")) {
        b.adaptiveSilence(Boolean.parseBoolean(v));
      } else if (a.startsWith("--maxCutWaitSec=")) {
        b.maxCutWaitMs(Long.parseLong(v) * 1000);
      }
    }
    LiveTranscriptionConfig cfg = b.build();

    byte[] pcm;
    try (AudioInputStream in = AudioSystem.getAudioInputStream(Path.of(audio).toFile())) {
      AudioFormat f = in.getFormat();
      if ((int) f.getSampleRate() != LiveTranscriptionConfig.SAMPLE_RATE || f.getChannels() != 1
          || f.getSampleSizeInBits() != 16) {
        throw new IllegalArgumentException("需要 16kHz/mono/16-bit PCM WAV，实际：" + f);
      }
      pcm = in.readAllBytes();
    }
    for (String a : args) {
      if (a.startsWith("--maxSec=")) {
        int maxBytes = Integer.parseInt(a.substring(9)) * 32000;
        if (pcm.length > maxBytes) {
          pcm = java.util.Arrays.copyOf(pcm, maxBytes);
        }
      }
    }

    Files.createDirectories(Path.of("logs"));
    PrintWriter events =
        new PrintWriter(
            Files.newBufferedWriter(Path.of("logs", name + ".events.jsonl"), StandardCharsets.UTF_8),
            true);
    List<TranscriptEvent> finals = Collections.synchronizedList(new ArrayList<>());
    long t0 = System.currentTimeMillis();

    TranscriptListener listener =
        new TranscriptListener() {
          @Override
          public void onInterim(TranscriptEvent e) {
            write(events, t0, e);
          }

          @Override
          public void onFinal(TranscriptEvent e) {
            finals.add(e);
            write(events, t0, e);
            System.out.println(e);
          }

          @Override
          public void onSessionEvent(String message) {
            ObjectNode n = JSON.createObjectNode();
            n.put("wallSec", (System.currentTimeMillis() - t0) / 1000.0);
            n.put("type", "SESSION");
            n.put("message", message);
            synchronized (events) {
              events.println(n);
            }
            System.out.println("    · " + message);
          }

          @Override
          public void onError(Throwable error) {
            System.err.println("ERROR: " + error);
          }
        };

    System.out.printf(Locale.ROOT, "模型=%s 音频=%s (%.1fs) rotateAfter=%ds stall=%ds%n", cfg.model,
        audio, pcm.length / 32000.0, cfg.rotateAfterMs / 1000, cfg.stallMs / 1000);

    List<RotatingLiveTranscriber.SessionInfo> infos;
    try (RotatingLiveTranscriber t = new RotatingLiveTranscriber(cfg, listener)) {
      t.start();
      int frame = 3200; // 100ms
      long start = System.nanoTime();
      for (int i = 0; i * frame < pcm.length; i++) {
        long wait = start + i * 100_000_000L - System.nanoTime();
        if (wait > 0) {
          Thread.sleep(wait / 1_000_000L, (int) (wait % 1_000_000L));
        }
        if (killAtSec > 0 && i == killAtSec * 10) {
          t.abortCurrentSessionForTest();
        }
        int len = Math.min(frame, pcm.length - i * frame);
        byte[] chunk = new byte[len];
        System.arraycopy(pcm, i * frame, chunk, 0, len);
        t.feed(chunk);
      }
      t.finish();
      infos = t.sessionInfos();
    }

    StringBuilder tsv = new StringBuilder();
    tsv.append("# session\taudio_start_s\taudio_end_s\tend_reason\tgoaway\tfinals\ttext\n");
    System.out.println("\n================ 会话汇总 ================");
    for (RotatingLiveTranscriber.SessionInfo s : infos) {
      String line =
          String.format(Locale.ROOT, "S%d\t%.1f\t%.1f\t%s\t%s\t%d\t%s", s.index,
              s.audioStartMs / 1000.0, s.audioEndMs / 1000.0, s.endReason.replace('\t', ' '),
              s.goAway, s.finals, s.emittedText.replace('\t', ' '));
      tsv.append(line).append('\n');
      System.out.printf(Locale.ROOT,
          "S%-3d %7.1fs ~ %7.1fs  Final=%-3d 软落定=%-3d 兜底=%d  GoAway=%-5s  原因=%s%n",
          s.index, s.audioStartMs / 1000.0, s.audioEndMs / 1000.0, s.finals, s.softFlushes,
          s.interimFallbacks, s.goAway, s.endReason);
    }
    List<String> all = new ArrayList<>();
    synchronized (finals) {
      for (TranscriptEvent e : finals) {
        all.add(e.text);
      }
    }
    tsv.append("MERGED\t\t\t\t\t\t").append(String.join(" ", all)).append('\n');
    Files.writeString(Path.of("logs", name + ".tsv"), tsv.toString(), StandardCharsets.UTF_8);
    events.close();
    System.out.println("结果：logs/" + name + ".tsv, logs/" + name + ".events.jsonl");
    System.exit(0);
  }

  private static void write(PrintWriter w, long t0, TranscriptEvent e) {
    ObjectNode n = JSON.createObjectNode();
    n.put("wallSec", (System.currentTimeMillis() - t0) / 1000.0);
    n.put("type", e.type.name());
    n.put("source", e.source.name());
    n.put("session", e.sessionIndex);
    n.put("startMs", e.startMs);
    n.put("endMs", e.endMs);
    n.put("text", e.text);
    synchronized (w) {
      w.println(n);
    }
  }
}
