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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * {@link RotatingLiveTranscriber} 的配置。默认值即经过 630s 实测验证的推荐配置。
 *
 * <pre>
 *   LiveTranscriptionConfig cfg = LiveTranscriptionConfig.builder()
 *       .project("my-project").location("global")
 *       .build();
 * </pre>
 */
public final class LiveTranscriptionConfig {

  /** 输入音频格式固定为 16kHz / mono / 16-bit little-endian PCM。 */
  public static final int SAMPLE_RATE = 16000;

  public final String project;
  public final String location;
  public final String model;
  public final String apiVersion;
  public final List<String> languageCodes;
  public final boolean interimTimestamps;

  /** 会话运行到该时长主动轮换（服务端约 560s 发 GoAway，约 590s 关闭）。 */
  public final long rotateAfterMs;
  /** 看门狗：有未落定 interim 且该段音频超过此时长仍无 Final 时轮换；0 = 关闭。 */
  public final long stallMs;
  /**
   * 软落定（soft flush）：当前段已累计 ≥ 该时长且客户端检测到停顿时，在<b>同一会话</b>发送
   * audioStreamEnd 迫使服务端立即输出 Final，随后继续推流（不重连）；0 = 关闭。
   */
  public final long flushAfterMs;
  /** 客户端能量 VAD：100ms 帧 RMS 低于该值视为静音（开启自适应时作为下限）。 */
  public final int silenceRms;
  /** 自适应静音阈值：按最近 10s 的噪声底和语音电平动态调整（嘈杂环境必需）。 */
  public final boolean adaptiveSilence;
  /** 连续静音达到该时长才视为可切点。 */
  public final long silenceMs;
  /** 新会话就绪后最多等多久找静音切点，超时强制切换。 */
  public final long maxCutWaitMs;
  /** 强制切换时重放给新会话的音频时长（文本按词去重）。 */
  public final long overlapMs;
  /** 旧会话收尾：最多等待多久让未落定 interim 变成 Final。 */
  public final long drainMaxMs;
  /** 旧会话收尾：无未落定 interim 且该时长内无新 Final 即认为收尾完成。 */
  public final long drainQuietMs;
  /** 连接意外断开时，最多重放多长的音频（从上一个 Final 结束处起）。 */
  public final long maxReplayMs;
  /** 建连超时。 */
  public final long connectTimeoutMs;

  private LiveTranscriptionConfig(Builder b) {
    this.project = Objects.requireNonNull(b.project, "project");
    this.location = b.location;
    this.model = b.model;
    this.apiVersion = b.apiVersion;
    this.languageCodes = List.copyOf(b.languageCodes);
    this.interimTimestamps = b.interimTimestamps;
    this.rotateAfterMs = b.rotateAfterMs;
    this.stallMs = b.stallMs;
    this.flushAfterMs = b.flushAfterMs;
    this.silenceRms = b.silenceRms;
    this.adaptiveSilence = b.adaptiveSilence;
    this.silenceMs = b.silenceMs;
    this.maxCutWaitMs = b.maxCutWaitMs;
    this.overlapMs = b.overlapMs;
    this.drainMaxMs = b.drainMaxMs;
    this.drainQuietMs = b.drainQuietMs;
    this.maxReplayMs = b.maxReplayMs;
    this.connectTimeoutMs = b.connectTimeoutMs;
    if (rotateAfterMs < 60_000 || rotateAfterMs > 540_000) {
      throw new IllegalArgumentException("rotateAfterMs 应在 60s~540s 之间（服务端约 560s 发 GoAway）");
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder。 */
  public static final class Builder {
    private String project;
    private String location = "global";
    private String model = "gemini-3.5-transcribe-live";
    private String apiVersion = "v1beta1";
    private final List<String> languageCodes = new ArrayList<>();
    private boolean interimTimestamps = true;
    private long rotateAfterMs = 480_000;
    private long stallMs = 35_000;
    private long flushAfterMs = 8_000;
    private int silenceRms = 300;
    private boolean adaptiveSilence = true;
    private long silenceMs = 300;
    private long maxCutWaitMs = 20_000;
    private long overlapMs = 2_000;
    private long drainMaxMs = 20_000;
    private long drainQuietMs = 1_500;
    private long maxReplayMs = 60_000;
    private long connectTimeoutMs = 30_000;

    public Builder project(String v) {
      project = v;
      return this;
    }

    public Builder location(String v) {
      location = v;
      return this;
    }

    public Builder model(String v) {
      model = v;
      return this;
    }

    public Builder apiVersion(String v) {
      apiVersion = v;
      return this;
    }

    public Builder languageCodes(List<String> v) {
      languageCodes.clear();
      languageCodes.addAll(v);
      return this;
    }

    public Builder interimTimestamps(boolean v) {
      interimTimestamps = v;
      return this;
    }

    public Builder rotateAfterMs(long v) {
      rotateAfterMs = v;
      return this;
    }

    public Builder stallMs(long v) {
      stallMs = v;
      return this;
    }

    public Builder flushAfterMs(long v) {
      flushAfterMs = v;
      return this;
    }

    public Builder silenceRms(int v) {
      silenceRms = v;
      return this;
    }

    public Builder adaptiveSilence(boolean v) {
      adaptiveSilence = v;
      return this;
    }

    public Builder silenceMs(long v) {
      silenceMs = v;
      return this;
    }

    public Builder maxCutWaitMs(long v) {
      maxCutWaitMs = v;
      return this;
    }

    public Builder overlapMs(long v) {
      overlapMs = v;
      return this;
    }

    public Builder drainMaxMs(long v) {
      drainMaxMs = v;
      return this;
    }

    public Builder drainQuietMs(long v) {
      drainQuietMs = v;
      return this;
    }

    public Builder maxReplayMs(long v) {
      maxReplayMs = v;
      return this;
    }

    public Builder connectTimeoutMs(long v) {
      connectTimeoutMs = v;
      return this;
    }

    public LiveTranscriptionConfig build() {
      return new LiveTranscriptionConfig(this);
    }
  }
}
