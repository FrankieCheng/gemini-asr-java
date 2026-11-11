# Gemini 3.5 Transcribe & Chirp 3 Java Demos

This repository provides Java implementations for Google Cloud speech transcription:

1. **[`Gemini35TranscribeDemo.java`](src/main/java/com/example/speech/Gemini35TranscribeDemo.java)** — Java equivalent of [`gemini_3_5_transcribe.ipynb`](https://github.com/GoogleCloudPlatform/generative-ai/blob/main/audio/speech/getting-started/gemini_3_5_transcribe.ipynb), using the **Google Gen AI SDK for Java** (`com.google.genai:google-genai`):
   - **Synchronous (`gemini-3.5-transcribe-preview`)** via `client.models.generateContent(...)`
   - **Streaming (`gemini-3.5-transcribe-live`)** via `client.async.live.connect(...)`

2. **[`Chirp3TranscribeDemo.java`](src/main/java/com/example/speech/Chirp3TranscribeDemo.java)** — Java equivalent of [`get_started_with_chirp_3_transcription.ipynb`](https://github.com/GoogleCloudPlatform/generative-ai/blob/main/audio/speech/getting-started/get_started_with_chirp_3_transcription.ipynb), using the **Google Cloud Speech-to-Text V2 SDK for Java** (`com.google.cloud:google-cloud-speech`) with model `chirp_3`.

3. **[`com.example.speech.live.RotatingLiveTranscriber`](src/main/java/com/example/speech/live/RotatingLiveTranscriber.java)** — Production-grade client-side session rotation & soft-flush library for **>10-minute real-time audio streams** on `gemini-3.5-transcribe-live` (see full engineering guide in [`docs/GEMINI_LIVE_LONG_AUDIO_GUIDE.md`](docs/GEMINI_LIVE_LONG_AUDIO_GUIDE.md)).

---

## Prerequisites

1. Java 17+ and Apache Maven 3.9+
2. Google Cloud Application Default Credentials configured:
   ```bash
   export GOOGLE_CLOUD_PROJECT="your-project-id"
   export GOOGLE_CLOUD_REGION="global"
   export STT_LOCATION="us"
   gcloud auth application-default login
   gcloud auth application-default set-quota-project "$GOOGLE_CLOUD_PROJECT"
   ```

---

## Build & Run

### Compile & Run Self-Tests
```bash
mvn test-compile exec:java -Dexec.mainClass="com.example.speech.live.LiveSelfTest"
```

### Run Demo 1: `Gemini35TranscribeDemo` (`gemini-3.5-transcribe-preview` & `gemini-3.5-transcribe-live`)
```bash
mvn exec:java -Dexec.mainClass="com.example.speech.Gemini35TranscribeDemo"
```

### Run Demo 2: `Chirp3TranscribeDemo` (`chirp_3`)
```bash
mvn exec:java -Dexec.mainClass="com.example.speech.Chirp3TranscribeDemo"
```

---

## 超过 10 分钟的无损实时流转写：`com.example.speech.live.RotatingLiveTranscriber`（默认 ✨ Method C）

完整根因分析、A/B/C 三方案架构演进与真实 Vertex AI 基准评测报告请参阅：
- [`docs/GEMINI_35_LIVE_OFFICIAL_REFERENCE_AND_ENHANCEMENT.md`](docs/GEMINI_35_LIVE_OFFICIAL_REFERENCE_AND_ENHANCEMENT.md)（10 分钟官方标注溯源、官方 GitHub Python 参考实现原理/局限性拆解、以及混合增强方案 Method C 设计详解）
- [`docs/GEMINI_LIVE_LONG_AUDIO_GUIDE.md`](docs/GEMINI_LIVE_LONG_AUDIO_GUIDE.md)（原生 WebSocket 抓包分析与长语音全量基准评测报告）

默认启用 **🌟 统一全场景自适应模式（`unifiedAdaptiveMode()`，零配置通吃近场/远场与超 10 分钟长流）**：
- **底层全拾音防吞字（`manualVad = true`）**：关闭在远场混响下易吞掉句首的服务端自动断句，同一场会议内近场/远场说话人交替也无需断开重建 WebSocket；
- **10s 滑动窗实时 SNR 双档动态定稿**：
  - **⚡ 近场高信噪比档**（`P80 >= 1350 && SNR >= 4.5x`）：说话满 `3.5s` 且遇到 `>=600ms` 自然句间停顿即刻输出 `Final`（首句 `3.84s` 即定稿，平均 `6.7s` 一句），连续快语速满 `15s` 微停顿兜底；
  - **🛡️ 远场低信噪比抗噪档**（`P80 < 1350 || SNR < 4.5x`）：自动屏蔽混响造成的假停顿，攒满 `15s` 上下文或遇到 `>=1.8s` 话轮交替停顿输出高精度 `Final`；
- **两段式零盲区轮换 + `60s` 环形缓冲重放**：满 8 分钟后台提前建连预热（`0ms` 握手空窗），配合中/英/东南亚语种词级与字符级消重，支持无限时长连续推流。

```java
LiveTranscriptionConfig cfg = LiveTranscriptionConfig.builder()
    .project("my-project")
    .location("global")
    .languageCodes(List.of("cmn-CN", "en-US")) // 可选：推荐显式传入语种
    .build(); // 默认已开启 unifiedAdaptiveMode()：零配置自动适配近场、远场与远近交替场景

try (RotatingLiveTranscriber t = new RotatingLiveTranscriber(cfg, new TranscriptListener() {
      @Override public void onInterim(TranscriptEvent e) { /* 实时字幕预览 */ }
      @Override public void onFinal(TranscriptEvent e)   { /* 有序、去重、全程时间戳，区分 SERVER / SOFT_FLUSH */ }
      @Override public void onSessionEvent(String m)     { /* official_gap 句末切流 / GoAway / 断线恢复日志 */ }
    })) {
  t.start();
  while (source.hasAudio()) {
    t.feed(source.read());   // 16kHz mono 16-bit PCM，任意长度，按实时节奏
  }
  t.finish();
}
```

### 启动三方案（Method A / B / C）实时对决与工程工作台（`LiveWorkbenchServer`）
```bash
mvn compile exec:java -Dexec.mainClass="com.example.speech.live.LiveWorkbenchServer" -Dexec.args="8090"
# 本地访问: http://localhost:8090（#view-compare 可一键查看三组真实 Vertex AI 实测快照或启动三路同源实时对决）
```

**从 Mac 浏览器远程访问（已避开 Cloudtop `8080` 端口冲突）：**
1. **Jetski 右侧面板直连（免 SSH，100% 实时后端）**：在 Jetski Web 对话中直接点击 `sidecar://gemini-transcribe-workbench/`（由 `~/.gemini/config/sidecars/gemini-transcribe-workbench/` 自动守护并反代 `127.0.0.1:8090`）；
2. **Corp HTTPS 免隧道静态镜像（Mac / 移动端直接全屏打开）**：`https://xuejiechen.users.x20web.corp.google.com/www/gemini-transcribe/index.html`（完整预载三组真实 Vertex AI 实测快照、982s 原声回放与消重沙盒）；
3. **Mac 全屏浏览器 + 100% 实时 WebSocket 推流**：Mac 终端执行 `ssh -N -L 8090:127.0.0.1:8090 xuejiechen.c.googlers.com` 后访问 `http://localhost:8090`。

### 运行命令行演示程序（`RotatingLiveTranscriberDemo`）
```bash
# 1. 默认 Method C 标准长音频测试（官方 !pending 句末优先切流 + 24s 安全阀 + 60s 缓冲）
mvn exec:java -Dexec.mainClass="com.example.speech.live.RotatingLiveTranscriberDemo" \
  -Dexec.args="--audio=JamesCameron.wav --maxSec=120 --rotateAfterSec=45 --name=live_method_c"

# 2. 切换为传统 Method B（客户端 8s 高频软刷 + 声学静音抢切）对比
mvn exec:java -Dexec.mainClass="com.example.speech.live.RotatingLiveTranscriberDemo" \
  -Dexec.args="--audio=JamesCameron.wav --maxSec=120 --rotateAfterSec=45 --legacyMethodB --name=live_method_b"

# 3. 模拟句中网络闪断与 60s 自动回放恢复
mvn exec:java -Dexec.mainClass="com.example.speech.live.RotatingLiveTranscriberDemo" \
  -Dexec.args="--audio=JamesCameron.wav --startSec=60 --maxSec=35 --endSensitivity=LOW --silenceDurationMs=900 --killAtSec=22 --name=live_kill22"

# 4. 未知近场/远场输入：方案一「15s 全场景通吃模式」（推荐，近场 WER 1.78% + 远场 CER 44.27% + 跨 10 分钟无损接力）
mvn exec:java -Dexec.mainClass="com.example.speech.live.RotatingLiveTranscriberDemo" \
  -Dexec.args="--audio=JamesCameron.wav --maxSec=120 --universal15s --name=live_universal15s"

# 5. 未知近场/远场输入：方案三「3s 声学 SNR + 服务端反馈自适应探测热切换模式」
mvn exec:java -Dexec.mainClass="com.example.speech.live.RotatingLiveTranscriberDemo" \
  -Dexec.args="--audio=JamesCameron.wav --maxSec=120 --autoField --name=live_autofield"

# 6. 运行五路同源未知声场实测基准（UnknownFieldBenchmark）
mvn exec:java -Dexec.mainClass="com.example.speech.live.UnknownFieldBenchmark"
```

