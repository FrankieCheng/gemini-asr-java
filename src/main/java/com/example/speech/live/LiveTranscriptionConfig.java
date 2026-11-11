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

  /** 语音起点检测（SOS）灵敏度档位，对应 AutomaticActivityDetection.StartSensitivity。 */
  public enum StartSensitivity {
    DEFAULT(null),
    HIGH("START_SENSITIVITY_HIGH"),
    LOW("START_SENSITIVITY_LOW");

    public final String wireValue;

    StartSensitivity(String wireValue) {
      this.wireValue = wireValue;
    }

    public static StartSensitivity parse(String s) {
      if (s == null || s.isBlank()) {
        return DEFAULT;
      }
      String u = s.trim().toUpperCase( java.util.Locale.ROOT);
      if (u.contains("HIGH")) {
        return HIGH;
      }
      if (u.contains("LOW")) {
        return LOW;
      }
      return DEFAULT;
    }
  }

  /** 语音终点检测（EOS）灵敏度档位，对应 AutomaticActivityDetection.EndSensitivity。 */
  public enum EndSensitivity {
    DEFAULT(null),
    HIGH("END_SENSITIVITY_HIGH"),
    LOW("END_SENSITIVITY_LOW");

    public final String wireValue;

    EndSensitivity(String wireValue) {
      this.wireValue = wireValue;
    }

    public static EndSensitivity parse(String s) {
      if (s == null || s.isBlank()) {
        return DEFAULT;
      }
      String u = s.trim().toUpperCase(java.util.Locale.ROOT);
      if (u.contains("HIGH")) {
        return HIGH;
      }
      if (u.contains("LOW")) {
        return LOW;
      }
      return DEFAULT;
    }
  }

  /** 输入音频格式固定为 16kHz / mono / 16-bit little-endian PCM。 */
  public static final int SAMPLE_RATE = 16000;

  /** 官方 Python ADK (_gemini_stt.py) 默认静音断句阈值：500ms（服务端原生默认为 880ms）。 */
  public static final int DEFAULT_SILENCE_DURATION_MS = 500;

  /** 官方 Python ADK (_gemini_stt.py) 默认切流/定稿保留尾随静音时长：500ms。 */
  public static final long DEFAULT_TAIL_REPLAY_MS = 500;

  /** 官方 Python ADK (_gemini_stt.py) 在 audioStreamEnd 后追加的静音帧余量：200ms。 */
  public static final long DEFAULT_FLUSH_SILENCE_MARGIN_MS = 200;

  /** 远场长会议模式（manualVad=true）默认大窗口闭合周期：45s（兼顾长上下文抗噪与 60s 断网重放上限）。 */
  public static final long DEFAULT_FAR_FIELD_FLUSH_MS = 45_000;

  public final String project;
  public final String location;
  public final String model;
  public final String apiVersion;
  public final List<String> languageCodes;
  public final List<String> customVocabulary;
  public final boolean interimTimestamps;

  // --- Configurable Endpointing (gemini-3.5-transcribe-live GA) ---
  /** 是否关闭服务端自动 VAD（改由客户端发 activityStart / activityEnd）。 */
  public final boolean manualVad;
  /** 自动端点检测：语音起点灵敏度（Default / High / Low）。 */
  public final StartSensitivity startOfSpeechSensitivity;
  /** 自动端点检测：语音终点灵敏度（Default / High / Low）。 */
  public final EndSensitivity endOfSpeechSensitivity;
  /**
   * 自动端点检测：判定一句话结束所需的持续静音毫秒数（对应服务端 encoder_ep_eos_wait_time）。
   * 官方 Python 参考实现默认设为 500ms；设为 ≤0 则使用服务端默认（880ms）。
   * 调优建议：16kHz 音频 ≥400ms，8kHz 电话音频 ≥700ms。
   */
  public final int silenceDurationMs;
  /** 自动端点检测：SOS 触发前包含进当前句子的前导缓冲毫秒数（≤0 表示使用服务端默认 300ms）。 */
  public final int prefixPaddingMs;

  // --- Continuous Long-Form Streaming & Session Rotation ---
  /** 会话运行到该时长主动轮换（官方 Python 参考实现 _SESSION_ROTATION_S = 480s；服务端约 560s 发 GoAway）。 */
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
  /**
   * 静音切点或 Final 定稿后仍保留给新会话回放的尾随静音时长（官方 Python _gemini_stt.py _REPLAY_MS = 500ms，
   * 实际取 {@code min(tailReplayMs, effectiveSilenceDurationMs)}），确保切流瞬间刚开口的首音节不丢失且不会重复转写上一句。
   */
  public final long tailReplayMs;
  /**
   * 会话收尾（_flush / drain）发送 audioStreamEnd 后，额外补发的全零 PCM 静音余量毫秒数
   * （官方 Python _gemini_stt.py _FLUSH_SILENCE_MARGIN_MS = 200ms，总补发 {@code effectiveSilenceDurationMs + flushSilenceMarginMs}）。
   */
  public final long flushSilenceMarginMs;
  /** 旧会话收尾：最多等待多久让未落定 interim 变成 Final（官方 Python _FLUSH_TIMEOUT_S = 5s，默认取 20s 兼容长尾）。 */
  public final long drainMaxMs;
  /** 旧会话收尾：无未落定 interim 且该时长内无新消息即认为收尾完成（官方 Python _FLUSH_QUIET_S = 0.5s）。 */
  public final long drainQuietMs;
  /** 连接意外断开或句中 GoAway 时，最多重放多长的未定稿音频（从上一个 Final 结束处起）。 */
  public final long maxReplayMs;
  /** 建连超时。 */
  public final long connectTimeoutMs;
  /**
   * Method C（官方优先混合模式）：为 {@code true} 时：
   * <ol>
   *   <li>轮换切点在 {@link #preferServerCutoverMs} 窗口内严格优先采用官方 {@code !pending}（服务端句末确认），
   *       仅在超长紧凑独白超时或 {@code GoAway} 逼近时才降级启用客户端静音/重叠切流；</li>
   *   <li>日常断句完全交由服务端 Configurable Endpointing，客户端软刷仅作为超长独白安全阀（默认 20s）；</li>
   *   <li>断连重连遵循官方 {@code _TRANSIENT_CLOSE_CODES} / {@code _TRANSIENT_STATUS_CODES}，非瞬态错误快速失败。</li>
   * </ol>
   */
  public final boolean officialFirst;
  /**
   * Method C 模式下，新会话后台预热就绪后优先等待官方 {@code !pending} 句末切点的窗口时长（毫秒，默认 12s）。
   */
  public final long preferServerCutoverMs;
  /**
   * 是否开启「近场/远场自适应探测与热切换」（方案三）：
   * 初始以高容忍 Auto-VAD 启动，流式过程中通过 3s 声学信噪比探针（P80 语音电平与 P80/P10 动态比）
   * 及服务端空话轮反馈实时判别；若检出远场/高混响特征，立即后台 Make-Before-Break 热切换至
   * {@code manualVad = true} 并从环形缓冲区无损回放未定稿音频。
   */
  public final boolean autoFieldDetect;
  /**
   * 是否开启「单一全场景自适应定稿模式」（{@code manualVad = true} + 实时滑动窗口 SNR 双档动态定稿）：
   * 底层固定 {@code manualVad = true} 彻底杜绝远场吞字且无需跨连接重建；
   * 上层根据最近 10s 滑动窗口的实时声学信噪比（{@code P80} 与 {@code SNR = P80/P10}）动态切换定稿节奏：
   * 近场高信噪比时遇到自然句间停顿（≥600ms，已说 ≥3.0s）即刻定稿出 Final；
   * 远场低信噪比时屏蔽短停顿误切，攒满 15s 窗口或遇到 ≥1.5s 明确话轮交替停顿时定稿。
   */
  public final boolean unifiedAdaptive;

  private LiveTranscriptionConfig(Builder b) {
    this.project = Objects.requireNonNull(b.project, "project");
    this.location = b.location;
    this.model = b.model;
    this.apiVersion = b.apiVersion;
    this.languageCodes = List.copyOf(b.languageCodes);
    this.customVocabulary = List.copyOf(b.customVocabulary);
    this.interimTimestamps = b.interimTimestamps;
    this.manualVad = b.manualVad;
    this.startOfSpeechSensitivity = Objects.requireNonNull(b.startOfSpeechSensitivity);
    this.endOfSpeechSensitivity = Objects.requireNonNull(b.endOfSpeechSensitivity);
    this.silenceDurationMs = b.silenceDurationMs;
    this.prefixPaddingMs = b.prefixPaddingMs;
    this.rotateAfterMs = b.rotateAfterMs;
    this.stallMs = b.stallMs;
    this.flushAfterMs = b.flushAfterMs;
    this.silenceRms = b.silenceRms;
    this.adaptiveSilence = b.adaptiveSilence;
    this.silenceMs = b.silenceMs;
    this.maxCutWaitMs = b.maxCutWaitMs;
    this.overlapMs = b.overlapMs;
    this.tailReplayMs = b.tailReplayMs;
    this.flushSilenceMarginMs = b.flushSilenceMarginMs;
    this.drainMaxMs = b.drainMaxMs;
    this.drainQuietMs = b.drainQuietMs;
    this.maxReplayMs = b.maxReplayMs;
    this.connectTimeoutMs = b.connectTimeoutMs;
    this.officialFirst = b.officialFirst;
    this.preferServerCutoverMs = b.preferServerCutoverMs;
    this.autoFieldDetect = b.autoFieldDetect;
    this.unifiedAdaptive = b.unifiedAdaptive;
    if (rotateAfterMs < 15_000 || rotateAfterMs > 540_000) {
      throw new IllegalArgumentException("rotateAfterMs 应在 15s~540s 之间（服务端约 560s 发 GoAway）");
    }
  }

  /** 基于当前配置派生指定 {@code manualVad} 与 {@code flushAfterMs} 的新配置（供自适应热切换建连使用）。 */
  public LiveTranscriptionConfig withManualVad(boolean newManualVad, long newFlushAfterMs) {
    Builder b = new Builder();
    b.project = this.project;
    b.location = this.location;
    b.model = this.model;
    b.apiVersion = this.apiVersion;
    b.languageCodes.addAll(this.languageCodes);
    b.customVocabulary.addAll(this.customVocabulary);
    b.interimTimestamps = this.interimTimestamps;
    b.manualVad = newManualVad;
    b.startOfSpeechSensitivity = this.startOfSpeechSensitivity;
    b.endOfSpeechSensitivity = this.endOfSpeechSensitivity;
    b.silenceDurationMs = this.silenceDurationMs;
    b.prefixPaddingMs = this.prefixPaddingMs;
    b.rotateAfterMs = this.rotateAfterMs;
    b.stallMs = newManualVad ? Math.max(this.stallMs, 90_000L) : this.stallMs;
    b.flushAfterMs = newFlushAfterMs;
    b.silenceRms = this.silenceRms;
    b.adaptiveSilence = this.adaptiveSilence;
    b.silenceMs = this.silenceMs;
    b.maxCutWaitMs = this.maxCutWaitMs;
    b.overlapMs = this.overlapMs;
    b.tailReplayMs = this.tailReplayMs;
    b.flushSilenceMarginMs = this.flushSilenceMarginMs;
    b.drainMaxMs = this.drainMaxMs;
    b.drainQuietMs = this.drainQuietMs;
    b.maxReplayMs = this.maxReplayMs;
    b.connectTimeoutMs = this.connectTimeoutMs;
    b.officialFirst = this.officialFirst;
    b.preferServerCutoverMs = this.preferServerCutoverMs;
    b.autoFieldDetect = this.autoFieldDetect;
    b.unifiedAdaptive = this.unifiedAdaptive;
    return new LiveTranscriptionConfig(b);
  }

  /** 实际生效的静音断句毫秒数（若未显式指定则按服务端默认 880ms 估算）。 */
  public int effectiveSilenceDurationMs() {
    return silenceDurationMs > 0 ? silenceDurationMs : 880;
  }

  /**
   * 对应官方 Python {@code _AudioStream(tail_ms=min(_REPLAY_MS, self._silence_duration_ms))}：
   * 在 Final 定稿或句间切流时保留的静音尾随音频时长（毫秒）。
   */
  public long effectiveTailReplayMs() {
    if (tailReplayMs <= 0) {
      return 0;
    }
    return Math.min(tailReplayMs, effectiveSilenceDurationMs());
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder（默认即采用 Method C「官方优先 + Make-Before-Break 兜底」推荐策略）。 */
  public static final class Builder {
    private String project;
    private String location = "global";
    private String model = "gemini-3.5-transcribe-live";
    private String apiVersion = "v1beta1";
    private final List<String> languageCodes = new ArrayList<>();
    private final List<String> customVocabulary = new ArrayList<>();
    private boolean interimTimestamps = true;
    private boolean manualVad = true;
    private StartSensitivity startOfSpeechSensitivity = StartSensitivity.DEFAULT;
    private EndSensitivity endOfSpeechSensitivity = EndSensitivity.DEFAULT;
    private int silenceDurationMs = DEFAULT_SILENCE_DURATION_MS;
    private int prefixPaddingMs = -1;
    private long rotateAfterMs = 480_000;
    private long stallMs = 60_000;
    private long flushAfterMs = 15_000;
    private int silenceRms = 300;
    private boolean adaptiveSilence = true;
    private long silenceMs = 250;
    private long maxCutWaitMs = 20_000;
    private long overlapMs = 2_000;
    private long tailReplayMs = DEFAULT_TAIL_REPLAY_MS;
    private long flushSilenceMarginMs = DEFAULT_FLUSH_SILENCE_MARGIN_MS;
    private long drainMaxMs = 20_000;
    private long drainQuietMs = 500;
    private long maxReplayMs = 60_000;
    private long connectTimeoutMs = 30_000;
    private boolean officialFirst = true;
    private long preferServerCutoverMs = 12_000;
    private boolean autoFieldDetect = false;
    private boolean unifiedAdaptive = true;

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

    public Builder customVocabulary(List<String> v) {
      customVocabulary.clear();
      customVocabulary.addAll(v);
      return this;
    }

    public Builder interimTimestamps(boolean v) {
      interimTimestamps = v;
      return this;
    }

    public Builder manualVad(boolean v) {
      manualVad = v;
      if (!v) {
        unifiedAdaptive = false;
      }
      return this;
    }

    public Builder startOfSpeechSensitivity(StartSensitivity v) {
      startOfSpeechSensitivity = Objects.requireNonNull(v);
      return this;
    }

    public Builder endOfSpeechSensitivity(EndSensitivity v) {
      endOfSpeechSensitivity = Objects.requireNonNull(v);
      return this;
    }

    public Builder silenceDurationMs(int v) {
      silenceDurationMs = v;
      return this;
    }

    public Builder prefixPaddingMs(int v) {
      prefixPaddingMs = v;
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

    public Builder tailReplayMs(long v) {
      tailReplayMs = v;
      return this;
    }

    public Builder flushSilenceMarginMs(long v) {
      flushSilenceMarginMs = v;
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

    public Builder officialFirst(boolean v) {
      officialFirst = v;
      return this;
    }

    public Builder preferServerCutoverMs(long v) {
      preferServerCutoverMs = v;
      return this;
    }

    public Builder autoFieldDetect(boolean v) {
      autoFieldDetect = v;
      return this;
    }

    public Builder unifiedAdaptive(boolean v) {
      unifiedAdaptive = v;
      return this;
    }

    /** 一键配置 Method C（官方 ADK 优先 + Make-Before-Break 兜底 + 24s 长独白安全阀）。 */
    public Builder methodCOfficialFirst() {
      this.manualVad = false;
      this.autoFieldDetect = false;
      this.unifiedAdaptive = false;
      this.officialFirst = true;
      this.preferServerCutoverMs = 12_000;
      this.flushAfterMs = 24_000;
      this.stallMs = 35_000;
      this.maxReplayMs = 60_000;
      return this;
    }

    /**
     * 一键配置「远场长会议模式」（面向会议室桌面全向麦、大屏阵列等多人远场拾音）：
     * <ol>
     *   <li>关闭服务端自动断句（{@code manualVad = true}，对应 {@code automaticActivityDetection.disabled = true}），
     *       消除远场混响导致服务端自动 VAD 吞掉句首及频繁清空跨句上下文的问题；</li>
     *   <li>开启 {@code flushAfterMs = 45s} 大窗口定期闭合（趁微停顿发送 {@code activityEnd} 等待落定后再发
     *       {@code activityStart} 并微回放衔接帧），兼顾整段长上下文高识别率与 {@code 60s} 断网重放上限、
     *       避开服务端 {@code 220s} 单话轮硬上限；</li>
     *   <li>配合 {@code rotateAfterMs = 480s} 双会话后台预热（Make-Before-Break），实现跨 10 分钟无损接力。</li>
     * </ol>
     */
    public Builder farFieldMode() {
      this.manualVad = true;
      this.autoFieldDetect = false;
      this.unifiedAdaptive = false;
      this.officialFirst = true;
      this.flushAfterMs = DEFAULT_FAR_FIELD_FLUSH_MS;
      this.stallMs = 90_000;
      this.maxReplayMs = 60_000;
      return this;
    }

    /**
     * 方案一：全场景通吃 15s 快速定稿远场模式（{@code manualVad = true} + {@code flushAfterMs = 15s}）。
     * 彻底避免远场 Auto-VAD 吞字，同时在近场与远场每满 15s 遇到 250ms 停顿即输出带完整标点的 Final。
     */
    public Builder universalFarField15s() {
      this.manualVad = true;
      this.autoFieldDetect = false;
      this.unifiedAdaptive = false;
      this.officialFirst = true;
      this.flushAfterMs = 15_000;
      this.silenceMs = 250;
      this.stallMs = 60_000;
      this.maxReplayMs = 60_000;
      return this;
    }

    /**
     * 方案二：高容忍度 Auto-VAD 折中模式（{@code manualVad = false} + 高灵敏起音 + 低灵敏断句 + 500ms 前导缓冲）。
     * 适用于必须按句秒级触发 {@code onFinal} 的对话式 Agent 场景。
     */
    public Builder tolerantAutoVadMode() {
      this.manualVad = false;
      this.autoFieldDetect = false;
      this.unifiedAdaptive = false;
      this.officialFirst = true;
      this.startOfSpeechSensitivity = StartSensitivity.HIGH;
      this.endOfSpeechSensitivity = EndSensitivity.LOW;
      this.silenceDurationMs = 800;
      this.prefixPaddingMs = 500;
      this.flushAfterMs = 16_000;
      this.stallMs = 35_000;
      this.maxReplayMs = 60_000;
      return this;
    }

    /**
     * 方案三：声学 + 服务端反馈自适应探测热切换模式（{@code autoFieldDetect = true}）。
     * 初始以高容忍 Auto-VAD 启动 S1（若是近场清晰语音则全程享受服务端按句秒级 Final）；
     * 一旦前 3~5 秒声学探针检出远场/高混响特征（{@code P80 < 1350} 或 {@code SNR < 4.5x}）或服务端连续吞字，
     * 立即后台 Make-Before-Break 热切换至 {@code manualVad = true}（远场模式）并从环形缓冲区无损重放未定稿音频。
     */
    public Builder autoFieldMode() {
      tolerantAutoVadMode();
      this.autoFieldDetect = true;
      return this;
    }

    /**
     * 统一全场景自适应模式（{@code unifiedAdaptive = true}）：
     * 底层固定 {@code manualVad = true}（100% 杜绝远场吞字，同一场会议内近场/远场说话人交替也无需重连 WebSocket）；
     * 上层由客户端根据最近 10s 滑动窗口的实时声学信噪比（{@code P80} 与 {@code SNR = P80/P10}）动态切换定稿节奏：
     * <ul>
     *   <li><b>近场高信噪比档</b>（{@code P80 >= 1350 && SNR >= 4.5x}）：说话满 3.0s 且停顿 ≥600ms 即刻按句定稿出 Final，
     *       连续快语速满 15s 且微停顿 ≥250ms 兜底定稿；</li>
     *   <li><b>远场低信噪比抗噪档</b>（{@code P80 < 1350 || SNR < 4.5x}）：屏蔽短停顿防切碎，攒满 15s 且微停顿 ≥250ms
     *       （或遇到 ≥1.8s 明确话轮交替停顿且已说满 5s）定稿出 Final。</li>
     * </ul>
     */
    public Builder unifiedAdaptiveMode() {
      this.manualVad = true;
      this.autoFieldDetect = false;
      this.unifiedAdaptive = true;
      this.officialFirst = true;
      this.flushAfterMs = 15_000;
      this.silenceMs = 250;
      this.stallMs = 60_000;
      this.maxReplayMs = 60_000;
      return this;
    }

    /** 一键配置传统 Method B（客户端 8s 高频自适应软刷 + 客户端静音抢切 + 60s 重放）。 */
    public Builder legacyMethodB() {
      this.officialFirst = false;
      this.autoFieldDetect = false;
      this.unifiedAdaptive = false;
      this.preferServerCutoverMs = 0;
      this.flushAfterMs = 8_000;
      this.maxReplayMs = 60_000;
      return this;
    }

    public LiveTranscriptionConfig build() {
      return new LiveTranscriptionConfig(this);
    }
  }
}
