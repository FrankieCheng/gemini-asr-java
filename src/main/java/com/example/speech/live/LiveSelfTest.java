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

/**
 * 不依赖网络的离线自检（项目未引入 JUnit，保持零新增依赖）：
 * 去重、Duration 解析、setup JSON。
 *
 * <pre>mvn -q exec:java -Dexec.mainClass=com.example.speech.live.LiveSelfTest</pre>
 */
public final class LiveSelfTest {

  private static int failures = 0;

  public static void main(String[] args) throws Exception {
    String[] tail = "the live API maintains a persistent, low latency".split(" ");

    // 1) 新文本以已发出文本的结尾开头（强制切换重放 2s）
    check("prefix overlap",
        RotatingLiveTranscriber.dropOverlap(tail, "a persistent, low latency connection. Audio is"),
        "connection. Audio is");
    // 2) 新文本比已发出文本更早开始（断线后重放）
    check("replay from earlier",
        RotatingLiveTranscriber.dropOverlap(tail,
            "Unlike conventional HTTP APIs, the live API maintains a persistent, low latency connection."),
        "connection.");
    // 3) 已发出文本最后一个词被截断 / 写法不同
    String[] tail2 = "the live API maintains a persistent, low lat".split(" ");
    check("truncated last word",
        RotatingLiveTranscriber.dropOverlap(tail2,
            "the live API maintains a persistent, low latency connection."),
        "connection.");
    // 4) 没有重叠时原样返回
    check("no overlap",
        RotatingLiveTranscriber.dropOverlap(tail, "Completely different sentence here."),
        "Completely different sentence here.");
    // 5) 完全重复
    check("full duplicate",
        RotatingLiveTranscriber.dropOverlap(tail, "maintains a persistent, low latency"), "");
    // 6) 连字符写法差异（630s 强制切换实测回归用例）
    String[] tail3 = "five-minute chunks with a 500-millisecond overlap.".split(" ");
    check("hyphen variants",
        RotatingLiveTranscriber.dropOverlap(tail3, "millisecond overlap. This guarantees"),
        "This guarantees");
    // 7) 单词级边界重叠（静音切流预留 pre-roll 触及上一句尾词回归用例）
    String[] tail4 = "all kinds of things, but".split(" ");
    check("1-word boundary overlap",
        RotatingLiveTranscriber.dropOverlap(tail4, "but it struck me that what I had"),
        "it struck me that what I had");
    // 8) 中文无空格边界微回放重叠（cmn-CN_2 48.6s 实测回归用例：社零增6.3%。 + 6.3，但...）
    String[] tailCjk1 = new String[] {"对了，是弱复苏，社零增6.3%。"};
    check("CJK numeric boundary overlap",
        RotatingLiveTranscriber.dropOverlap(tailCjk1, "6.3，但可选项消费才涨3.5。"),
        "但可选项消费才涨3.5。");
    // 9) 中文无空格强制切流 2s 重叠锚点去重
    String[] tailCjk2 = new String[] {"仓库西侧围墙有两处墙体老化，墙体偏"};
    check("CJK clause anchor overlap",
        RotatingLiveTranscriber.dropOverlap(tailCjk2, "有两处墙体老化，墙体偏低，容易被攀爬。"),
        "低，容易被攀爬。");
    // 10) 中文正常句间单字同字不误删（"...做好。" + "好的张总..." 绝不能误删 "好"）
    String[] tailCjk3 = new String[] {"一定要把防范工作做好。"};
    check("CJK 1-char no false trim",
        RotatingLiveTranscriber.dropOverlap(tailCjk3, "好的张总，我马上安排。"),
        "好的张总，我马上安排。");

    // Duration 解析
    ObjectMapper m = new ObjectMapper();
    check("duration 574.800s",
        String.valueOf(LiveWsSession.parseDurationMs(m.readTree("\"574.800s\""))), "574800");
    check("duration 30s", String.valueOf(LiveWsSession.parseDurationMs(m.readTree("\"30s\""))),
        "30000");

    // setup JSON — 默认带 interimTranscriptTimestampEnabled 与官方 Python 默认 silenceDurationMs=500
    LiveTranscriptionConfig defaultCfg = LiveTranscriptionConfig.builder().project("p").build();
    String setup = LiveWsSession.buildSetup(defaultCfg);
    check("setup has interim ts", String.valueOf(setup.contains(
        "\"interimTranscriptTimestampEnabled\":true")), "true");
    check("setup default silence 500ms", String.valueOf(setup.contains(
        "\"automaticActivityDetection\":{\"silenceDurationMs\":500}")), "true");
    check("setup model path", String.valueOf(setup.contains(
        "projects/p/locations/global/publishers/google/models/gemini-3.5-transcribe-live")), "true");
    check("default tailReplayMs=500", String.valueOf(defaultCfg.effectiveTailReplayMs()), "500");

    // Configurable Endpointing + customVocabulary (对标 test_gemini_stt.py)
    LiveTranscriptionConfig customEpCfg =
        LiveTranscriptionConfig.builder()
            .project("p")
            .languageCodes(java.util.List.of("en-US"))
            .customVocabulary(java.util.List.of("ADK", "Cymbal Direct"))
            .startOfSpeechSensitivity(LiveTranscriptionConfig.StartSensitivity.HIGH)
            .endOfSpeechSensitivity(LiveTranscriptionConfig.EndSensitivity.LOW)
            .prefixPaddingMs(150)
            .silenceDurationMs(200)
            .build();
    String customSetup = LiveWsSession.buildSetup(customEpCfg);
    check("setup custom vocab", String.valueOf(customSetup.contains(
        "\"customVocabulary\":[\"ADK\",\"Cymbal Direct\"]")), "true");
    check("setup SOS HIGH", String.valueOf(customSetup.contains(
        "\"startOfSpeechSensitivity\":\"START_SENSITIVITY_HIGH\"")), "true");
    check("setup EOS LOW", String.valueOf(customSetup.contains(
        "\"endOfSpeechSensitivity\":\"END_SENSITIVITY_LOW\"")), "true");
    check("setup prefixPaddingMs 150", String.valueOf(customSetup.contains(
        "\"prefixPaddingMs\":150")), "true");
    check("setup silenceDurationMs 200", String.valueOf(customSetup.contains(
        "\"silenceDurationMs\":200")), "true");
    // 对标 test_short_silence_keeps_less_audio_after_a_final: min(500, 200) == 200
    check("short silence tailReplayMs=200", String.valueOf(customEpCfg.effectiveTailReplayMs()), "200");

    // Manual VAD (disabled=true) & farFieldMode() 一键预设
    LiveTranscriptionConfig farFieldCfg =
        LiveTranscriptionConfig.builder().project("p").farFieldMode().build();
    String manualSetup = LiveWsSession.buildSetup(farFieldCfg);
    check("setup manualVad disabled=true", String.valueOf(manualSetup.contains(
        "\"automaticActivityDetection\":{\"disabled\":true}")), "true");
    check("farField manualVad=true", String.valueOf(farFieldCfg.manualVad), "true");
    check("farField flushAfterMs 45s", String.valueOf(farFieldCfg.flushAfterMs), "45000");
    check("farField stallMs 90s", String.valueOf(farFieldCfg.stallMs), "90000");
    check("farField maxReplayMs 60s", String.valueOf(farFieldCfg.maxReplayMs), "60000");

    // Method C 默认参数与官方瞬态错误分类（对标 _gemini_stt.py _is_transient）
    check("Method C officialFirst default", String.valueOf(defaultCfg.officialFirst), "true");
    check("Method C flushAfterMs 24s", String.valueOf(defaultCfg.flushAfterMs), "24000");
    check("Method C preferServerCut 12s", String.valueOf(defaultCfg.preferServerCutoverMs), "12000");
    LiveTranscriptionConfig legacyB =
        LiveTranscriptionConfig.builder().project("p").legacyMethodB().build();
    check("Method B officialFirst=false", String.valueOf(legacyB.officialFirst), "false");
    check("Method B flushAfterMs 8s", String.valueOf(legacyB.flushAfterMs), "8000");
    check("transient ws 1006", String.valueOf(LiveWsSession.isTransientCode(1006, -1)), "true");
    check("transient ws 1011", String.valueOf(LiveWsSession.isTransientCode(1011, -1)), "true");
    check("non-transient ws 1008", String.valueOf(LiveWsSession.isTransientCode(1008, -1)), "false");
    check("transient http 503", String.valueOf(LiveWsSession.isTransientCode(-1, 503)), "true");
    check("non-transient http 403", String.valueOf(LiveWsSession.isTransientCode(-1, 403)), "false");

    // 未知近场/远场三大方案预设校验
    LiveTranscriptionConfig u15 =
        LiveTranscriptionConfig.builder().project("p").universalFarField15s().build();
    check("Plan1 manualVad=true", String.valueOf(u15.manualVad), "true");
    check("Plan1 flushAfterMs=15000", String.valueOf(u15.flushAfterMs), "15000");
    LiveTranscriptionConfig tol =
        LiveTranscriptionConfig.builder().project("p").tolerantAutoVadMode().build();
    check("Plan2 manualVad=false", String.valueOf(tol.manualVad), "false");
    check("Plan2 silenceDuration=800", String.valueOf(tol.silenceDurationMs), "800");
    check("Plan2 prefixPadding=500", String.valueOf(tol.prefixPaddingMs), "500");
    LiveTranscriptionConfig autoCfg =
        LiveTranscriptionConfig.builder().project("p").autoFieldMode().build();
    check("Plan3 autoFieldDetect=true", String.valueOf(autoCfg.autoFieldDetect), "true");
    check("Plan3 initial manualVad=false", String.valueOf(autoCfg.manualVad), "false");
    LiveTranscriptionConfig upgraded = autoCfg.withManualVad(true, 45_000L);
    check("Plan3 upgraded manualVad=true", String.valueOf(upgraded.manualVad), "true");
    check("Plan3 upgraded flush=45000", String.valueOf(upgraded.flushAfterMs), "45000");

    System.out.println(failures == 0 ? "ALL PASSED" : failures + " FAILED");
    System.exit(failures == 0 ? 0 : 1);
  }

  private static void check(String name, String actual, String expected) {
    boolean ok = expected.equals(actual);
    if (!ok) {
      failures++;
    }
    System.out.printf("%s %-24s actual=[%s]%s%n", ok ? "PASS" : "FAIL", name, actual,
        ok ? "" : " expected=[" + expected + "]");
  }
}
