# Gemini 3.5 Transcribe & Chirp 3 Java Demos

This repository provides the Java equivalents of the two Google Cloud speech transcription Jupyter notebooks:

1. **[`Gemini35TranscribeDemo.java`](src/main/java/com/example/speech/Gemini35TranscribeDemo.java)** — Java equivalent of [`gemini_3_5_transcribe.ipynb`](https://github.com/GoogleCloudPlatform/generative-ai/blob/main/audio/speech/getting-started/gemini_3_5_transcribe.ipynb), using the **Google Gen AI SDK for Java** (`com.google.genai:google-genai`):
   - **Synchronous (`gemini-3.5-transcribe-preview`)** via `client.models.generateContent(...)`:
     - Automatic language recognition and word-level timestamps (`wordTimestamp(true)`)
     - Transcription with explicit language codes (`languageCodes("es-ES")`) and Cloud Storage `gs://` URI (`Part.fromUri(...)`)
     - Speaker diarization (`diarization(true)`)
     - Biasing transcription output with custom vocabulary (`customVocabulary("oatmilk", "oz")`)
     - Batch transcription for long audio files (up to 1 hour)
   - **Streaming (`gemini-3.5-transcribe-live-preview`)** via `client.async.live.connect(...)`:
     - Streaming 16-bit mono PCM audio in 100ms chunks (`sendRealtimeInput`) and handling both `interimInputTranscription` and `inputTranscription`
     - Streaming transcription with automatic language recognition
     - Streaming transcription with language codes (`languageCodes("ko-KR", "en-US")`)
     - Streaming transcription with custom vocabulary (`customVocabulary("oatmilk", "oz")`)
     - Long audio streaming with overlapping session chunking (2.5-minute chunks with 500ms boundary overlap)
     - Real-time streaming speech recognition directly from a microphone (`TargetDataLine`)

2. **[`Chirp3TranscribeDemo.java`](src/main/java/com/example/speech/Chirp3TranscribeDemo.java)** — Java equivalent of [`get_started_with_chirp_3_transcription.ipynb`](https://github.com/GoogleCloudPlatform/generative-ai/blob/main/audio/speech/getting-started/get_started_with_chirp_3_transcription.ipynb), using the **Google Cloud Speech-to-Text V2 SDK for Java** (`com.google.cloud:google-cloud-speech`) with model `chirp_3`:
   - Online / Synchronous speech recognition (`client.recognize` with `AutoDetectDecodingConfig` and `languageCodes=["en-US"]`)
   - Language-agnostic transcription (`languageCodes=["auto"]` with a Cloud Storage `gs://` URI)
   - Speaker diarization with Batch Recognition (`client.batchRecognizeAsync` with `SpeakerDiarizationConfig` and speaker utterance grouping)
   - Streaming speech recognition (`client.streamingRecognizeCallable` streaming 3200-byte chunks with `languageCodes=["auto"]`)

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

### Compile both demos
```bash
mvn compile
```

### Run Demo 1: `Gemini35TranscribeDemo` (`gemini-3.5-transcribe-preview` & `gemini-3.5-transcribe-live-preview`)
```bash
mvn exec:java -Dexec.mainClass="com.example.speech.Gemini35TranscribeDemo"
```

### Run Demo 2: `Chirp3TranscribeDemo` (`chirp_3`)
```bash
mvn exec:java -Dexec.mainClass="com.example.speech.Chirp3TranscribeDemo"
```
