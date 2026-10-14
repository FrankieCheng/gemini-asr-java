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

import java.util.Locale;

/**
 * 一条转写结果。时间戳是<b>全程连续</b>的流时间（毫秒，从第一帧音频算起），已加上所属会话的起始偏移；
 * 无法获知时为 -1。
 */
public final class TranscriptEvent {

  /** 事件类型。 */
  public enum Type {
    /** 实时中间结果（同一段内累积增长，会被后续 interim / Final 覆盖）。 */
    INTERIM,
    /** 落定结果（按时间顺序、跨会话去重后发出，可直接拼接）。 */
    FINAL
  }

  /** FINAL 的来源。 */
  public enum Source {
    /** 服务端返回的 inputTranscription。 */
    SERVER,
    /** 会话收尾时服务端未落定，用累计 interim 兜底。 */
    INTERIM_FALLBACK
  }

  public final Type type;
  public final Source source;
  public final String text;
  public final long startMs;
  public final long endMs;
  public final int sessionIndex;

  TranscriptEvent(Type type, Source source, String text, long startMs, long endMs, int sessionIndex) {
    this.type = type;
    this.source = source;
    this.text = text;
    this.startMs = startMs;
    this.endMs = endMs;
    this.sessionIndex = sessionIndex;
  }

  TranscriptEvent withText(String newText) {
    return new TranscriptEvent(type, source, newText, startMs, endMs, sessionIndex);
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "%s[S%d %s %.2fs-%.2fs] %s", type, sessionIndex,
        source, startMs / 1000.0, endMs / 1000.0, text);
  }
}
