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

    // Duration 解析
    ObjectMapper m = new ObjectMapper();
    check("duration 574.800s",
        String.valueOf(LiveWsSession.parseDurationMs(m.readTree("\"574.800s\""))), "574800");
    check("duration 30s", String.valueOf(LiveWsSession.parseDurationMs(m.readTree("\"30s\""))),
        "30000");

    // setup JSON
    String setup =
        LiveWsSession.buildSetup(LiveTranscriptionConfig.builder().project("p").build());
    check("setup has interim ts", String.valueOf(setup.contains(
        "\"realtimeInputConfig\":{\"interimTranscriptTimestampEnabled\":true}")), "true");
    check("setup model path", String.valueOf(setup.contains(
        "projects/p/locations/global/publishers/google/models/gemini-3.5-transcribe-live")), "true");

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
