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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
 * 面向「未知近场/远场输入」的五路同源实时流式评测器：
 * 在近场清晰语音（JamesCameron.wav）与远场多人会议语音（cmn-CN_1 / en-US_1）上同步对比：
 * <ol>
 *   <li><b>Baseline_Near</b>：默认近场模式（{@code methodCOfficialFirst}）</li>
 *   <li><b>Baseline_Far45s</b>：默认远场 45s 模式（{@code farFieldMode}）</li>
 *   <li><b>Plan1_UniversalFar15s</b>：方案一·15s 窗口全场景通吃模式（{@code universalFarField15s}）</li>
 *   <li><b>Plan2_TolerantAutoVad</b>：方案二·高容忍 Auto-VAD 折中参数（{@code tolerantAutoVadMode}）</li>
 *   <li><b>Plan3_AutoDetectHybrid</b>：方案三·声学+服务端反馈自适应热切换模式（{@code autoFieldMode}）</li>
 * </ol>
 */
public final class UnknownFieldBenchmark {

  private static final ObjectMapper JSON =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private static final class SampleSpec {
    final String id;
    final String sceneType;
    final Path wavPath;
    final String langCode;
    final int maxSec;
    final long rotateAfterSec;
    final String refText;

    SampleSpec(
        String id,
        String sceneType,
        Path wavPath,
        String langCode,
        int maxSec,
        long rotateAfterSec,
        String refText) {
      this.id = id;
      this.sceneType = sceneType;
      this.wavPath = wavPath;
      this.langCode = langCode;
      this.maxSec = maxSec;
      this.rotateAfterSec = rotateAfterSec;
      this.refText = refText;
    }
  }

  private static final class LaneCollector implements TranscriptListener {
    final String laneId;
    final boolean cjk;
    final long startWallMs;
    volatile long firstInterimLatencyMs = -1;
    volatile long firstFinalLatencyMs = -1;
    final List<Long> finalWallTimes = Collections.synchronizedList(new ArrayList<>());
    final List<TranscriptEvent> finals = Collections.synchronizedList(new ArrayList<>());
    final List<String> sessionEvents = Collections.synchronizedList(new ArrayList<>());
    final List<String> errors = Collections.synchronizedList(new ArrayList<>());

    LaneCollector(String laneId, boolean cjk, long startWallMs) {
      this.laneId = laneId;
      this.cjk = cjk;
      this.startWallMs = startWallMs;
    }

    @Override
    public void onInterim(TranscriptEvent event) {
      if (firstInterimLatencyMs < 0 && event.text != null && !event.text.isBlank()) {
        firstInterimLatencyMs = System.currentTimeMillis() - startWallMs;
      }
    }

    @Override
    public void onFinal(TranscriptEvent event) {
      if (event.text != null && !event.text.isBlank()) {
        long now = System.currentTimeMillis();
        if (firstFinalLatencyMs < 0) {
          firstFinalLatencyMs = now - startWallMs;
        }
        finalWallTimes.add(now);
      }
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

    double avgFinalIntervalSec(double totalDurationSec) {
      if (finals.isEmpty()) {
        return totalDurationSec;
      }
      return totalDurationSec / finals.size();
    }

    double maxFinalIntervalSec(double totalDurationSec) {
      synchronized (finalWallTimes) {
        if (finalWallTimes.isEmpty()) {
          return totalDurationSec;
        }
        long prev = startWallMs;
        long maxDiff = 0;
        for (long t : finalWallTimes) {
          maxDiff = Math.max(maxDiff, t - prev);
          prev = t;
        }
        return maxDiff / 1000.0;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String project = System.getenv().getOrDefault("GOOGLE_CLOUD_PROJECT", "cloud-llm-preview1");
    String location = System.getenv().getOrDefault("GOOGLE_CLOUD_REGION", "global");
    String model = "gemini-3.5-transcribe-live";
    Path outDir = Path.of("logs/unknown_field_bench");
    Files.createDirectories(outDir);

    // 1. JamesCameron.wav 前 60s 标准参考文本（来自 JamesCameron.ref.txt 0~60s）
    String cameronRef60s =
        "i grew up on a steady diet of science fiction in high school i i took a bus to school an"
            + " hour each way every day and i was always absorbed in a book science fiction book"
            + " which took my mind to other worlds and satisfied this in in a in a narrative form"
            + " this insatiable sense of curiosity that i had and and you know that curiosity also"
            + " manifested itself in in the fact that whenever i wasn't in school i was i was out"
            + " in the woods hiking and taking samples frogs and snakes and bugs and pond water"
            + " and bringing it back looking at it under the microscope i was you know i was a"
            + " real science geek but it was all about trying to understand understand the world"
            + " understand the the limits of of possibility and my you know love of of science"
            + " fiction actually seemed to be mirrored in the world around me because what was"
            + " happening this is in the late sixties";

    // 2. cmn-CN_1_cmn-CN_90s.wav (90s 远场六人会议) 标准参考文本
    String cmn1Ref90s =
        "各位，都坐。咱们15分钟高效落实预防盗窃专项工作，最近周边园区接连出现盗窃事件，咱们得提前防范、堵住漏洞。李总，先说说近期咱们排查出的风险点。"
            + "好的张总。上周联合安保部做了全公司排查，整体防控有基础，但确实发现不少漏洞。办公区、车间、仓库、财务室这几个关键区域，都有不同程度的风险隐患，尤其是仓库和车间外围，问题稍突出些。"
            + "仓库具体有哪些问题？那里存放着原材料和成品，可不能出岔子。"
            + "张总，我来说说。仓库西侧围墙有两处墙体老化，墙体偏低，容易被攀爬；另外，仓库内部监控有3个死角，夜间光线暗的时候，没法完全覆盖，这都是不小的隐患呢。"
            + "监控死角和围墙问题，怎么不早整改？"
            + "之前排查时只注意到监控角度问题，计划微调位置，没想到墙体也出现了老化。而且仓库近期货物堆积多，部分区域遮挡了监控，才形成了新的死角，是我们考虑不周全。";

    // 3. en-US_1_en-US_90s.wav (90s 远场英文噪声会议) 标准参考文本
    String en1Ref90s =
        "Are you free? Let's have a quick catch-up on our recent work. "
            + "Sure, I also want to update you on the latest situation. "
            + "The online campaign we're in charge of is nearly ready, and all preparations are drawing to a close. "
            + "Alright, have the promotional copy and poster been finalized yet? "
            + "The poster is already finished, and we're just fine-tuning the wording of the copy. "
            + "When will everything be fully confirmed? "
            + "We'll finalize everything before getting off work today, and it will go online on time tomorrow. "
            + "That's a good pace. It won't delay the original promotion schedule. "
            + "By the way, have we tested the registration channel for any loopholes? "
            + "I've tested it several times repeatedly, and the whole process runs smoothly. "
            + "That's a relief.";

    List<SampleSpec> specs =
        List.of(
            new SampleSpec(
                "NearField_Cameron_60s",
                "近场清晰演讲 (en-US)",
                Path.of("JamesCameron.wav"),
                "en-US",
                60,
                40L,
                cameronRef60s),
            new SampleSpec(
                "FarField_Mandarin_90s",
                "远场六人会议 (cmn-CN)",
                Path.of("/tmp/0616nonrealtime_test/cmn-CN_1_cmn-CN_90s.wav"),
                "cmn-CN",
                90,
                45L,
                cmn1Ref90s),
            new SampleSpec(
                "FarField_English_90s",
                "远场高噪会议 (en-US)",
                Path.of("/tmp/0616nonrealtime_test/en-US_1_en-US_90s.wav"),
                "en-US",
                90,
                45L,
                en1Ref90s));

    ExecutorService pool = Executors.newFixedThreadPool(specs.size());
    List<Callable<ObjectNode>> tasks = new ArrayList<>();
    for (SampleSpec spec : specs) {
      tasks.add(() -> runSample(spec, project, location, model, outDir));
    }

    long t0 = System.currentTimeMillis();
    List<Future<ObjectNode>> futures = pool.invokeAll(tasks);
    pool.shutdown();

    ArrayNode allResults = JSON.createArrayNode();
    for (Future<ObjectNode> f : futures) {
      allResults.add(f.get());
    }
    Path summaryPath = outDir.resolve("summary.json");
    Files.writeString(summaryPath, JSON.writeValueAsString(allResults), StandardCharsets.UTF_8);
    System.out.printf(
        Locale.ROOT,
        "%n=== 全部 3 组场景 × 5 种方案实测完成（总耗时 %.1fs），汇总已保存至 %s ===%n",
        (System.currentTimeMillis() - t0) / 1000.0,
        summaryPath);
  }

  private static ObjectNode runSample(
      SampleSpec spec, String project, String location, String model, Path outDir)
      throws Exception {
    boolean cjk = spec.langCode.startsWith("cmn") || spec.langCode.startsWith("zh");
    byte[] pcm;
    try (AudioInputStream in = AudioSystem.getAudioInputStream(spec.wavPath.toFile())) {
      AudioFormat f = in.getFormat();
      if ((int) f.getSampleRate() != LiveTranscriptionConfig.SAMPLE_RATE
          || f.getChannels() != 1
          || f.getSampleSizeInBits() != 16) {
        throw new IllegalArgumentException("需要 16kHz/mono/16-bit PCM WAV，实际: " + f);
      }
      int maxBytes = spec.maxSec * LiveTranscriptionConfig.SAMPLE_RATE * 2;
      byte[] all = in.readAllBytes();
      pcm = all.length > maxBytes ? Arrays.copyOf(all, maxBytes) : all;
    }
    double durationSec = pcm.length / 32000.0;
    System.out.printf(
        Locale.ROOT,
        ">>> 启动五路同源实测 [%s] (%s, %.1fs, rotate=%ds)%n",
        spec.id,
        spec.sceneType,
        durationSec,
        spec.rotateAfterSec);

    LiveTranscriptionConfig cfgNear =
        LiveTranscriptionConfig.builder()
            .project(project)
            .location(location)
            .model(model)
            .languageCodes(List.of(spec.langCode))
            .methodCOfficialFirst()
            .rotateAfterMs(spec.rotateAfterSec * 1000L)
            .build();

    LiveTranscriptionConfig cfgFar45 =
        LiveTranscriptionConfig.builder()
            .project(project)
            .location(location)
            .model(model)
            .languageCodes(List.of(spec.langCode))
            .farFieldMode()
            .rotateAfterMs(spec.rotateAfterSec * 1000L)
            .build();

    LiveTranscriptionConfig cfgPlan1 =
        LiveTranscriptionConfig.builder()
            .project(project)
            .location(location)
            .model(model)
            .languageCodes(List.of(spec.langCode))
            .universalFarField15s()
            .rotateAfterMs(spec.rotateAfterSec * 1000L)
            .build();

    LiveTranscriptionConfig cfgPlan2 =
        LiveTranscriptionConfig.builder()
            .project(project)
            .location(location)
            .model(model)
            .languageCodes(List.of(spec.langCode))
            .tolerantAutoVadMode()
            .rotateAfterMs(spec.rotateAfterSec * 1000L)
            .build();

    LiveTranscriptionConfig cfgPlan3 =
        LiveTranscriptionConfig.builder()
            .project(project)
            .location(location)
            .model(model)
            .languageCodes(List.of(spec.langCode))
            .autoFieldMode()
            .rotateAfterMs(spec.rotateAfterSec * 1000L)
            .build();

    long startWall = System.currentTimeMillis();
    LaneCollector colNear = new LaneCollector("Baseline_Near", cjk, startWall);
    LaneCollector colFar45 = new LaneCollector("Baseline_Far45s", cjk, startWall);
    LaneCollector colPlan1 = new LaneCollector("Plan1_UniversalFar15s", cjk, startWall);
    LaneCollector colPlan2 = new LaneCollector("Plan2_TolerantAutoVad", cjk, startWall);
    LaneCollector colPlan3 = new LaneCollector("Plan3_AutoDetectHybrid", cjk, startWall);

    ObjectNode root = JSON.createObjectNode();
    root.put("id", spec.id);
    root.put("sceneType", spec.sceneType);
    root.put("langCode", spec.langCode);
    root.put("durationSec", durationSec);
    root.put("refText", spec.refText);
    ObjectNode lanesNode = root.putObject("lanes");

    try (RotatingLiveTranscriber trNear = new RotatingLiveTranscriber(cfgNear, colNear);
        RotatingLiveTranscriber trFar45 = new RotatingLiveTranscriber(cfgFar45, colFar45);
        RotatingLiveTranscriber trPlan1 = new RotatingLiveTranscriber(cfgPlan1, colPlan1);
        RotatingLiveTranscriber trPlan2 = new RotatingLiveTranscriber(cfgPlan2, colPlan2);
        RotatingLiveTranscriber trPlan3 = new RotatingLiveTranscriber(cfgPlan3, colPlan3)) {

      trNear.start();
      trFar45.start();
      trPlan1.start();
      trPlan2.start();
      trPlan3.start();

      int frameBytes = 3200; // 100ms
      long t0 = System.nanoTime();
      for (int i = 0; i * frameBytes < pcm.length; i++) {
        long waitNs = t0 + i * 100_000_000L - System.nanoTime();
        if (waitNs > 0) {
          Thread.sleep(waitNs / 1_000_000L, (int) (waitNs % 1_000_000L));
        }
        int len = Math.min(frameBytes, pcm.length - i * frameBytes);
        byte[] chunk = Arrays.copyOfRange(pcm, i * frameBytes, i * frameBytes + len);
        trNear.feed(chunk);
        trFar45.feed(chunk);
        trPlan1.feed(chunk);
        trPlan2.feed(chunk);
        trPlan3.feed(chunk);
      }

      List<Thread> finishers =
          List.of(
              new Thread(() -> finishQuietly(trNear)),
              new Thread(() -> finishQuietly(trFar45)),
              new Thread(() -> finishQuietly(trPlan1)),
              new Thread(() -> finishQuietly(trPlan2)),
              new Thread(() -> finishQuietly(trPlan3)));
      for (Thread t : finishers) {
        t.start();
      }
      for (Thread t : finishers) {
        t.join();
      }

      recordLane(lanesNode.putObject("Baseline_Near"), colNear, trNear, spec.refText, cjk, durationSec);
      recordLane(lanesNode.putObject("Baseline_Far45s"), colFar45, trFar45, spec.refText, cjk, durationSec);
      recordLane(lanesNode.putObject("Plan1_UniversalFar15s"), colPlan1, trPlan1, spec.refText, cjk, durationSec);
      recordLane(lanesNode.putObject("Plan2_TolerantAutoVad"), colPlan2, trPlan2, spec.refText, cjk, durationSec);
      recordLane(lanesNode.putObject("Plan3_AutoDetectHybrid"), colPlan3, trPlan3, spec.refText, cjk, durationSec);
    }

    Path outFile = outDir.resolve(spec.id + ".json");
    Files.writeString(outFile, JSON.writeValueAsString(root), StandardCharsets.UTF_8);
    System.out.printf(Locale.ROOT, "<<< 完成 [%s] → %s%n", spec.id, outFile);
    return root;
  }

  private static void finishQuietly(RotatingLiveTranscriber tr) {
    try {
      tr.finish();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void recordLane(
      ObjectNode node,
      LaneCollector col,
      RotatingLiveTranscriber tr,
      String refText,
      boolean cjk,
      double durationSec) {
    String hyp = col.mergedText();
    double errorRate = cjk ? computeCer(refText, hyp) : computeWer(refText, hyp);
    double coverage = computeCoverage(refText, hyp, cjk);
    node.put("mergedText", hyp);
    node.put("metricType", cjk ? "CER" : "WER");
    node.put("errorRatePct", Math.round(errorRate * 100.0) / 100.0);
    node.put("coveragePct", Math.round(coverage * 100.0) / 100.0);
    node.put("finalsCount", col.finals.size());
    node.put("sessionsCount", tr.sessionInfos().size());
    node.put("avgFinalIntervalSec", Math.round(col.avgFinalIntervalSec(durationSec) * 10.0) / 10.0);
    node.put("maxFinalIntervalSec", Math.round(col.maxFinalIntervalSec(durationSec) * 10.0) / 10.0);
    node.put("firstInterimLatencyMs", col.firstInterimLatencyMs);
    node.put("firstFinalLatencyMs", col.firstFinalLatencyMs);

    ArrayNode evArr = node.putArray("sessionEvents");
    synchronized (col.sessionEvents) {
      for (String e : col.sessionEvents) {
        evArr.add(e);
      }
    }
    ArrayNode fnArr = node.putArray("finals");
    synchronized (col.finals) {
      for (TranscriptEvent ev : col.finals) {
        ObjectNode fn = fnArr.addObject();
        fn.put("session", ev.sessionIndex);
        fn.put("source", ev.source.name());
        fn.put("startMs", ev.startMs);
        fn.put("endMs", ev.endMs);
        fn.put("text", ev.text);
      }
    }
  }

  private static String normCjk(String s) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); ) {
      int cp = s.codePointAt(i);
      if (Character.isLetterOrDigit(cp)) {
        sb.appendCodePoint(Character.toLowerCase(cp));
      }
      i += Character.charCount(cp);
    }
    return sb.toString();
  }

  private static String[] normWords(String s) {
    String cleaned = s.toLowerCase(Locale.ROOT).replaceAll("[^\\p{L}\\p{N}\\s]", " ").trim();
    if (cleaned.isEmpty()) {
      return new String[0];
    }
    return cleaned.split("\\s+");
  }

  private static double computeCer(String ref, String hyp) {
    String r = normCjk(ref);
    String h = normCjk(hyp);
    if (r.isEmpty()) {
      return 0.0;
    }
    int dist = editDistanceChars(r, h);
    return dist * 100.0 / r.length();
  }

  private static double computeWer(String ref, String hyp) {
    String[] r = normWords(ref);
    String[] h = normWords(hyp);
    if (r.length == 0) {
      return 0.0;
    }
    int dist = editDistanceWords(r, h);
    return dist * 100.0 / r.length;
  }

  private static double computeCoverage(String ref, String hyp, boolean cjk) {
    if (cjk) {
      int r = normCjk(ref).length();
      int h = normCjk(hyp).length();
      return r == 0 ? 100.0 : h * 100.0 / r;
    } else {
      int r = normWords(ref).length;
      int h = normWords(hyp).length;
      return r == 0 ? 100.0 : h * 100.0 / r;
    }
  }

  private static int editDistanceChars(String a, String b) {
    int n = a.length();
    int m = b.length();
    int[] dp = new int[m + 1];
    for (int j = 0; j <= m; j++) {
      dp[j] = j;
    }
    for (int i = 1; i <= n; i++) {
      int prev = dp[0];
      dp[0] = i;
      for (int j = 1; j <= m; j++) {
        int temp = dp[j];
        int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
        dp[j] = Math.min(Math.min(dp[j] + 1, dp[j - 1] + 1), prev + cost);
        prev = temp;
      }
    }
    return dp[m];
  }

  private static int editDistanceWords(String[] a, String[] b) {
    int n = a.length;
    int m = b.length;
    int[] dp = new int[m + 1];
    for (int j = 0; j <= m; j++) {
      dp[j] = j;
    }
    for (int i = 1; i <= n; i++) {
      int prev = dp[0];
      dp[0] = i;
      for (int j = 1; j <= m; j++) {
        int temp = dp[j];
        int cost = a[i - 1].equals(b[j - 1]) ? 0 : 1;
        dp[j] = Math.min(Math.min(dp[j] + 1, dp[j - 1] + 1), prev + cost);
        prev = temp;
      }
    }
    return dp[m];
  }
}
