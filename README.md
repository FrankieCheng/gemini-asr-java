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

## 超过 10 分钟的实时转写：`com.example.speech.live.RotatingLiveTranscriber`

完整根因分析、架构设计与公开数据集评测报告请参阅：[`docs/GEMINI_LIVE_LONG_AUDIO_GUIDE.md`](docs/GEMINI_LIVE_LONG_AUDIO_GUIDE.md)。

```java
LiveTranscriptionConfig cfg = LiveTranscriptionConfig.builder()
    .project("my-project")
    .location("global") // 其余参数默认即推荐值（480s 轮换 + 8s 软刷）
    .build();

try (RotatingLiveTranscriber t = new RotatingLiveTranscriber(cfg, new TranscriptListener() {
      @Override public void onInterim(TranscriptEvent e) { /* 实时字幕预览 */ }
      @Override public void onFinal(TranscriptEvent e)   { /* 有序、去重、全程时间戳，可直接拼接 */ }
      @Override public void onSessionEvent(String m)     { /* 轮换 / GoAway / 断线恢复日志 */ }
    })) {
  t.start();
  while (source.hasAudio()) {
    t.feed(source.read());   // 16kHz mono 16-bit PCM，任意长度，按实时节奏
  }
  t.finish();
}
```

### 运行演示程序（`RotatingLiveTranscriberDemo`）
```bash
# 1. 标准长音频测试（480s 轮换 + 8s 软刷）
mvn exec:java -Dexec.mainClass="com.example.speech.live.RotatingLiveTranscriberDemo" \
  -Dexec.args="--audio=test_video_630s.wav --name=live_630s"

# 2. 第 300s 模拟网络闪断与自动回放恢复
mvn exec:java -Dexec.mainClass="com.example.speech.live.RotatingLiveTranscriberDemo" \
  -Dexec.args="--audio=test_video_630s.wav --name=live_kill300 --killAtSec=300"

# 3. 强制无静音切换 + 边界重叠消重验证
mvn exec:java -Dexec.mainClass="com.example.speech.live.RotatingLiveTranscriberDemo" \
  -Dexec.args="--audio=test_video_630s.wav --name=live_forced200 --rotateAfterSec=200 --silenceRms=0"
```
