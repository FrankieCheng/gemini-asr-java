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
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;

/**
 * 面向多语种远场会议数据集（0616nonrealtime_test）的四路同源流式评测器：
 * 同一条 16kHz 16-bit mono WAV 按 100ms 实时节奏同步推入：
 * <ul>
 *   <li><b>A_adk</b>：官方 Python ADK 参考实现（{@link AdkReferenceLiveTranscriber}，串行重连 + 默认 Auto-VAD）</li>
 *   <li><b>B_legacy8s</b>：原 PPT P6 / 传统方案 B（{@code legacyMethodB}，8s 客户端静音软刷 + 双连接抢切 + Auto-VAD）</li>
 *   <li><b>C_near</b>：方案 C 近场默认模式（{@code methodCOfficialFirst}，原生自动断句 + 24s 安全阀 + !pending 句末零排空切流）</li>
 *   <li><b>C_far</b>：方案 C 远场增强模式（{@code farFieldMode}，关闭服务端自动 VAD {@code manualVad=true} + 大窗安全阀补发 activityStart 回放 + 双连接热备切流）</li>
 * </ul>
 */
public final class DatasetAbcBenchmark {

  private static final ObjectMapper JSON =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private static final class LaneCollector implements TranscriptListener {
    final String laneId;
    final boolean cjk;
    final List<TranscriptEvent> finals = Collections.synchronizedList(new ArrayList<>());
    final List<String> sessionEvents = Collections.synchronizedList(new ArrayList<>());
    final List<String> errors = Collections.synchronizedList(new ArrayList<>());

    LaneCollector(String laneId, boolean cjk) {
      this.laneId = laneId;
      this.cjk = cjk;
    }

    @Override
    public void onFinal(TranscriptEvent event) {
      finals.add(event);
    }

    @Override
    public void onSessionEvent(String message) {
      sessionEvents.add(message);
    }

    @Override
    public void onError(Throwable error) {
      errors.add(String.valueOf(error));
    }

    String mergedText() {
      StringBuilder sb = new StringBuilder();
      synchronized (finals) {
        for (TranscriptEvent ev : finals) {
          if (ev.text == null || ev.text.isBlank()) {
            continue;
          }
          if (!cjk && sb.length() > 0) {
            sb.append(' ');
          }
          sb.append(ev.text.trim());
        }
      }
      return cjk ? sb.toString() : sb.toString().replaceAll("\\s+", " ").trim();
    }

    int countBySource(TranscriptEvent.Source src) {
      int n = 0;
      synchronized (finals) {
        for (TranscriptEvent ev : finals) {
          if (ev.source == src) {
            n++;
          }
        }
      }
      return n;
    }
  }

  public static void main(String[] args) throws Exception {
    Path dataDir = Path.of("/tmp/0616nonrealtime_test");
    Path outDir = Path.of("logs/bench_0616");
    String project = System.getenv().getOrDefault("GOOGLE_CLOUD_PROJECT", "cloud-llm-preview1");
    String location = System.getenv().getOrDefault("GOOGLE_CLOUD_REGION", "global");
    String model = "gemini-3.5-transcribe-live";
    long rotateAfterSec = 45L;
    int concurrency = 4;
    List<String> filterSamples = new ArrayList<>();

    for (String a : args) {
      String v = a.contains("=") ? a.substring(a.indexOf('=') + 1) : "";
      if (a.startsWith("--dataDir=")) {
        dataDir = Path.of(v);
      } else if (a.startsWith("--outDir=")) {
        outDir = Path.of(v);
      } else if (a.startsWith("--project=")) {
        project = v;
      } else if (a.startsWith("--location=")) {
        location = v;
      } else if (a.startsWith("--model=")) {
        model = v;
      } else if (a.startsWith("--rotateAfterSec=")) {
        rotateAfterSec = Long.parseLong(v);
      } else if (a.startsWith("--concurrency=")) {
        concurrency = Integer.parseInt(v);
      } else if (a.startsWith("--samples=")) {
        for (String s : v.split(",")) {
          if (!s.isBlank()) {
            filterSamples.add(s.trim());
          }
        }
      }
    }

    Files.createDirectories(outDir);
    File[] wavFiles =
        dataDir.toFile().listFiles((d, name) -> name.endsWith(".wav"));
    if (wavFiles == null || wavFiles.length == 0) {
      throw new IllegalArgumentException("目录中未找到 .wav 文件: " + dataDir);
    }
    Arrays.sort(wavFiles, Comparator.comparing(File::getName));

    List<Path> targets = new ArrayList<>();
    for (File f : wavFiles) {
      if (filterSamples.isEmpty() || filterSamples.contains(f.getName())) {
        targets.add(f.toPath());
      }
    }

    System.out.printf(
        Locale.ROOT,
        "[DatasetAbcBenchmark] 共 %d 条样本 | 并发度=%d | 模型=%s | rotateAfter=%ds | 输出=%s%n",
        targets.size(),
        concurrency,
        model,
        rotateAfterSec,
        outDir);

    ExecutorService pool = Executors.newFixedThreadPool(concurrency);
    List<Callable<Path>> tasks = new ArrayList<>();
    final String finalProject = project;
    final String finalLocation = location;
    final String finalModel = model;
    final long finalRotateSec = rotateAfterSec;
    final Path finalOutDir = outDir;

    for (Path wav : targets) {
      tasks.add(
          () ->
              runOneSample(
                  wav, finalOutDir, finalProject, finalLocation, finalModel, finalRotateSec));
    }

    long wallStart = System.currentTimeMillis();
    List<Future<Path>> futures = pool.invokeAll(tasks);
    pool.shutdown();
    for (Future<Path> f : futures) {
      f.get();
    }
    System.out.printf(
        Locale.ROOT,
        "[DatasetAbcBenchmark] 全部 %d 条样本评测完成，总耗时 %.1fs%n",
        targets.size(),
        (System.currentTimeMillis() - wallStart) / 1000.0);
  }

  private static String extractTrueLangCode(String fileName) {
    // 文件命名格式: <folder>_<idx>_<trueLang>_90s.wav，如 cmn-CN_1_cmn-CN_90s.wav, id-ID_1_hi-IN_90s.wav
    String stem = fileName.endsWith(".wav") ? fileName.substring(0, fileName.length() - 4) : fileName;
    String[] parts = stem.split("_");
    if (parts.length >= 3 && parts[2].contains("-")) {
      return parts[2];
    }
    return parts[0];
  }

  private static Path runOneSample(
      Path wavPath,
      Path outDir,
      String project,
      String location,
      String model,
      long rotateAfterSec)
      throws Exception {
    String fileName = wavPath.getFileName().toString();
    String stem = fileName.substring(0, fileName.length() - 4);
    String langCode = extractTrueLangCode(fileName);
    boolean cjk = langCode.startsWith("cmn") || langCode.startsWith("zh") || langCode.startsWith("th");

    byte[] pcm;
    try (AudioInputStream in = AudioSystem.getAudioInputStream(wavPath.toFile())) {
      AudioFormat f = in.getFormat();
      if ((int) f.getSampleRate() != LiveTranscriptionConfig.SAMPLE_RATE
          || f.getChannels() != 1
          || f.getSampleSizeInBits() != 16) {
        throw new IllegalArgumentException("需要 16kHz/mono/16-bit PCM WAV，实际: " + f);
      }
      pcm = in.readAllBytes();
    }
    double durationSec = pcm.length / 32000.0;
    System.out.printf(
        Locale.ROOT, ">>> 开始评测 %s (lang=%s, len=%.1fs)%n", fileName, langCode, durationSec);

    LiveTranscriptionConfig cfgA =
        LiveTranscriptionConfig.builder()
            .project(project)
            .location(location)
            .model(model)
            .languageCodes(List.of(langCode))
            .rotateAfterMs(rotateAfterSec * 1000L)
            .flushAfterMs(0L)
            .silenceDurationMs(500)
            .build();

    LiveTranscriptionConfig cfgB =
        LiveTranscriptionConfig.builder()
            .project(project)
            .location(location)
            .model(model)
            .languageCodes(List.of(langCode))
            .legacyMethodB()
            .rotateAfterMs(rotateAfterSec * 1000L)
            .flushAfterMs(8_000L)
            .maxReplayMs(60_000L)
            .silenceDurationMs(500)
            .build();

    LiveTranscriptionConfig cfgCNear =
        LiveTranscriptionConfig.builder()
            .project(project)
            .location(location)
            .model(model)
            .languageCodes(List.of(langCode))
            .methodCOfficialFirst()
            .rotateAfterMs(rotateAfterSec * 1000L)
            .flushAfterMs(24_000L)
            .preferServerCutoverMs(12_000L)
            .maxReplayMs(60_000L)
            .silenceDurationMs(500)
            .build();

    LiveTranscriptionConfig cfgCFar =
        LiveTranscriptionConfig.builder()
            .project(project)
            .location(location)
            .model(model)
            .languageCodes(List.of(langCode))
            .farFieldMode()
            .rotateAfterMs(rotateAfterSec * 1000L)
            .maxReplayMs(60_000L)
            .build();

    LaneCollector colA = new LaneCollector("A_adk", cjk);
    LaneCollector colB = new LaneCollector("B_legacy8s", cjk);
    LaneCollector colCNear = new LaneCollector("C_near", cjk);
    LaneCollector colCFar = new LaneCollector("C_far", cjk);

    ObjectNode root = JSON.createObjectNode();
    root.put("sample", fileName);
    root.put("stem", stem);
    root.put("langCode", langCode);
    root.put("durationSec", durationSec);
    root.put("rotateAfterSec", rotateAfterSec);
    ObjectNode lanesNode = root.putObject("lanes");

    try (AdkReferenceLiveTranscriber trA =
            new AdkReferenceLiveTranscriber(cfgA, 10_000L, colA);
        RotatingLiveTranscriber trB = new RotatingLiveTranscriber(cfgB, colB);
        RotatingLiveTranscriber trCNear = new RotatingLiveTranscriber(cfgCNear, colCNear);
        RotatingLiveTranscriber trCFar = new RotatingLiveTranscriber(cfgCFar, colCFar)) {

      trA.start();
      trB.start();
      trCNear.start();
      trCFar.start();

      int frameBytes = 3200; // 100ms
      long t0 = System.nanoTime();
      for (int i = 0; i * frameBytes < pcm.length; i++) {
        long waitNs = t0 + i * 100_000_000L - System.nanoTime();
        if (waitNs > 0) {
          Thread.sleep(waitNs / 1_000_000L, (int) (waitNs % 1_000_000L));
        }
        int len = Math.min(frameBytes, pcm.length - i * frameBytes);
        byte[] chunk = Arrays.copyOfRange(pcm, i * frameBytes, i * frameBytes + len);
        trA.feed(chunk);
        trB.feed(chunk);
        trCNear.feed(chunk);
        trCFar.feed(chunk);
      }

      // 并行执行 4 路 finish() 排空收尾，避免串行等待 4 次
      Thread fA = new Thread(() -> finishQuietly(trA));
      Thread fB = new Thread(() -> finishQuietly(trB));
      Thread fCNear = new Thread(() -> finishQuietly(trCNear));
      Thread fCFar = new Thread(() -> finishQuietly(trCFar));
      fA.start();
      fB.start();
      fCNear.start();
      fCFar.start();
      fA.join();
      fB.join();
      fCNear.join();
      fCFar.join();

      writeLaneJson(
          lanesNode.putObject("A_adk"),
          colA,
          trA.sessionInfos(),
          colA.countBySource(TranscriptEvent.Source.SERVER),
          0,
          0,
          trA.totalHandshakeGapMs(),
          trA.droppedAudioMs());
      writeLaneJson(
          lanesNode.putObject("B_legacy8s"),
          colB,
          trB.sessionInfos(),
          trB.serverNaturalFinalsCount(),
          trB.officialCleanCutCount(),
          trB.fallbackCutCount(),
          0L,
          0L);
      writeLaneJson(
          lanesNode.putObject("C_near"),
          colCNear,
          trCNear.sessionInfos(),
          trCNear.serverNaturalFinalsCount(),
          trCNear.officialCleanCutCount(),
          trCNear.fallbackCutCount(),
          0L,
          0L);
      writeLaneJson(
          lanesNode.putObject("C_far"),
          colCFar,
          trCFar.sessionInfos(),
          trCFar.serverNaturalFinalsCount(),
          trCFar.officialCleanCutCount(),
          trCFar.fallbackCutCount(),
          0L,
          0L);
    }

    Path outFile = outDir.resolve(stem + ".json");
    Files.writeString(outFile, JSON.writeValueAsString(root), StandardCharsets.UTF_8);
    System.out.printf(
        Locale.ROOT,
        "<<< 完成 %s | A=%d字(%d段,gap=%dms) | B=%d字(%d段) | C_near=%d字(%d段) | C_far=%d字(%d段)%n",
        fileName,
        colA.mergedText().length(),
        colA.finals.size(),
        root.path("lanes").path("A_adk").path("handshakeGapMs").asLong(),
        colB.mergedText().length(),
        colB.finals.size(),
        colCNear.mergedText().length(),
        colCNear.finals.size(),
        colCFar.mergedText().length(),
        colCFar.finals.size());
    return outFile;
  }

  private static void finishQuietly(AdkReferenceLiveTranscriber tr) {
    try {
      tr.finish();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void finishQuietly(RotatingLiveTranscriber tr) {
    try {
      tr.finish();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void writeLaneJson(
      ObjectNode node,
      LaneCollector col,
      List<RotatingLiveTranscriber.SessionInfo> sessions,
      int serverFinals,
      int officialCleanCuts,
      int fallbackCuts,
      long handshakeGapMs,
      long droppedAudioMs) {
    String merged = col.mergedText();
    node.put("mergedText", merged);
    node.put("mergedChars", merged.length());
    node.put("finalsCount", col.finals.size());
    node.put("serverFinals", serverFinals);
    node.put("softFlushFinals", col.countBySource(TranscriptEvent.Source.SOFT_FLUSH));
    node.put("interimFallbackFinals", col.countBySource(TranscriptEvent.Source.INTERIM_FALLBACK));
    node.put("sessionsCount", sessions.size());
    node.put("officialCleanCuts", officialCleanCuts);
    node.put("fallbackCuts", fallbackCuts);
    node.put("handshakeGapMs", handshakeGapMs);
    node.put("droppedAudioMs", droppedAudioMs);

    ArrayNode sessArr = node.putArray("sessions");
    for (RotatingLiveTranscriber.SessionInfo s : sessions) {
      ObjectNode sn = sessArr.addObject();
      sn.put("index", s.index);
      sn.put("audioStartSec", s.audioStartMs / 1000.0);
      sn.put("audioEndSec", s.audioEndMs / 1000.0);
      sn.put("state", s.state);
      sn.put("endReason", s.endReason);
      sn.put("goAway", s.goAway);
      sn.put("finals", s.finals);
      sn.put("softFlushes", s.softFlushes);
      sn.put("interimFallbacks", s.interimFallbacks);
      sn.put("text", s.emittedText);
    }

    ArrayNode finalsArr = node.putArray("finals");
    synchronized (col.finals) {
      for (TranscriptEvent ev : col.finals) {
        ObjectNode fn = finalsArr.addObject();
        fn.put("session", ev.sessionIndex);
        fn.put("source", ev.source.name());
        fn.put("startMs", ev.startMs);
        fn.put("endMs", ev.endMs);
        fn.put("text", ev.text);
      }
    }

    ArrayNode evArr = node.putArray("sessionEvents");
    synchronized (col.sessionEvents) {
      for (String msg : col.sessionEvents) {
        evArr.add(msg);
      }
    }

    ArrayNode errArr = node.putArray("errors");
    synchronized (col.errors) {
      for (String err : col.errors) {
        errArr.add(err);
      }
    }
  }
}
