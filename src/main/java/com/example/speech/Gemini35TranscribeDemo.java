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
package com.example.speech;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.genai.AsyncSession;
import com.google.genai.Client;
import com.google.genai.JsonSerializable;
import com.google.genai.types.AudioTranscriptionConfig;
import com.google.genai.types.Blob;
import com.google.genai.types.Content;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.GenerateContentResponse;
import com.google.genai.types.LiveConnectConfig;
import com.google.genai.types.LiveSendRealtimeInputParameters;
import com.google.genai.types.LiveServerContent;
import com.google.genai.types.LiveServerMessage;
import com.google.genai.types.Part;
import com.google.genai.types.Transcription;
import com.google.genai.types.WordInfo;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.sound.sampled.AudioFileFormat;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.DataLine;
import javax.sound.sampled.TargetDataLine;

/**
 * Java equivalent of {@code gemini_3_5_transcribe.ipynb}.
 *
 * <p>Demonstrates Gemini 3.5 Transcribe using the Google Gen AI SDK for Java:
 * <ul>
 *   <li><b>Synchronous ({@code gemini-3.5-transcribe-preview})</b> via {@code client.models.generateContent}:
 *     <ol>
 *       <li>Automatic language recognition and word-level timestamps</li>
 *       <li>Transcription using explicit language codes and a Cloud Storage ({@code gs://}) URI</li>
 *       <li>Transcription with speaker diarization</li>
 *       <li>Biasing transcription output with custom vocabulary</li>
 *       <li>Transcribing long audio files (batch up to 1 hour)</li>
 *     </ol>
 *   </li>
 *   <li><b>Streaming ({@code gemini-3.5-transcribe-live})</b> via {@code client.async.live.connect}:
 *     <ol>
 *       <li>Streaming transcription with automatic language recognition</li>
 *       <li>Streaming transcription using language codes</li>
 *       <li>Biasing streaming transcription with custom vocabulary</li>
 *       <li>Streaming transcription for long audio files with overlapping session chunking</li>
 *       <li>Real-time streaming transcription from a microphone</li>
 *     </ol>
 *   </li>
 * </ul>
 */
public class Gemini35TranscribeDemo implements AutoCloseable {

  public static final String MODEL_ID = "gemini-3.5-transcribe-preview";
  public static final String MODEL_ID_LIVE =
      System.getProperty("model", "gemini-3.5-transcribe-live");

  private static final Set<String> ALLOWED_SAMPLE_HOSTS =
      Set.of("storage.googleapis.com");

  static {
    // Support fractional-second Duration strings (e.g. "0.200s") returned by Live API voiceActivity
    SimpleModule fractionalDurationModule = new SimpleModule();
    fractionalDurationModule.addDeserializer(
        Duration.class,
        new StdDeserializer<>(Duration.class) {
          @Override
          public Duration deserialize(JsonParser p, DeserializationContext ctxt)
              throws IOException {
            String text = p.getText();
            if (text == null || text.isBlank()) {
              return null;
            }
            String trimmed = text.trim();
            if (trimmed.endsWith("s") && !trimmed.startsWith("PT")) {
              double seconds = Double.parseDouble(trimmed.substring(0, trimmed.length() - 1));
              return Duration.ofNanos(Math.round(seconds * 1_000_000_000L));
            }
            return Duration.parse(trimmed);
          }
        });
    JsonSerializable.objectMapper().registerModule(fractionalDurationModule);
  }

  private final Client client;

  public Gemini35TranscribeDemo(String projectId, String location) {
    // Equivalent to: client = genai.Client(enterprise=True, project=PROJECT_ID, location=LOCATION)
    this.client =
        Client.builder()
            .enterprise(true)
            .project(projectId)
            .location(location)
            .build();
  }

  // =========================================================================
  // 1. SYNCHRONOUS TRANSCRIPTION (gemini-3.5-transcribe-preview)
  // =========================================================================

  /**
   * 1.1 Transcription with automatic language recognition and word-level timestamps.
   */
  public void transcribeWithTimestamps(Path audioFile) throws IOException {
    System.out.println("\n=== [Sync 1] Automatic Language Recognition & Word Timestamps ===");
    byte[] audioBytes = Files.readAllBytes(audioFile);

    GenerateContentConfig config =
        GenerateContentConfig.builder()
            .audioTranscriptionConfig(
                AudioTranscriptionConfig.builder()
                    .wordTimestamp(true)
                    .build())
            .build();

    Content content =
        Content.fromParts(Part.fromBytes(audioBytes, "audio/wav"));

    GenerateContentResponse response =
        client.models.generateContent(MODEL_ID, content, config);

    List<Part> parts = response.parts();
    if (parts != null && !parts.isEmpty()) {
      parts.get(0)
          .audioTranscription()
          .flatMap(Transcription::words)
          .orElse(Collections.emptyList())
          .forEach(
              (WordInfo w) ->
                  System.out.printf(
                      "[%s - %s] %s%n",
                      w.startOffset().orElse(""),
                      w.endOffset().orElse(""),
                      w.word().orElse("")));

      String fullText =
          parts.stream()
              .map(p -> p.text().orElse(""))
              .filter(t -> !t.isEmpty())
              .collect(Collectors.joining());
      if (!fullText.isEmpty()) {
        System.out.println("\nFull Transcript:\n" + fullText);
      }
    }
  }

  /**
   * 1.2 Transcription with language codes using a Cloud Storage (gs://) URI.
   */
  public void transcribeWithLanguageCodesFromGcs(String gcsUri) {
    System.out.println("\n=== [Sync 2] Transcription with Language Codes (gs:// URI) ===");

    GenerateContentConfig config =
        GenerateContentConfig.builder()
            .audioTranscriptionConfig(
                AudioTranscriptionConfig.builder()
                    .languageCodes("es-ES")
                    .build())
            .build();

    Content content =
        Content.fromParts(Part.fromUri(gcsUri, "audio/wav"));

    GenerateContentResponse response =
        client.models.generateContent(MODEL_ID, content, config);

    printPartsText(response);
  }

  /**
   * 1.3 Transcription with speaker diarization.
   */
  public void transcribeWithSpeakerDiarization(Path audioFile) throws IOException {
    System.out.println("\n=== [Sync 3] Transcription with Speaker Diarization ===");
    byte[] audioBytes = Files.readAllBytes(audioFile);

    GenerateContentConfig config =
        GenerateContentConfig.builder()
            .audioTranscriptionConfig(
                AudioTranscriptionConfig.builder()
                    .diarization(true)
                    .build())
            .build();

    Content content =
        Content.fromParts(Part.fromBytes(audioBytes, "audio/wav"));

    GenerateContentResponse response =
        client.models.generateContent(MODEL_ID, content, config);

    List<Part> parts = response.parts();
    if (parts != null) {
      for (Part part : parts) {
        String speaker =
            part.audioTranscription()
                .flatMap(Transcription::speakerLabel)
                .orElse("UNKNOWN");
        String text =
            part.text()
                .or(() -> part.audioTranscription().flatMap(Transcription::text))
                .orElse("");
        if (!text.isEmpty()) {
          System.out.printf("**%s**: %s%n", speaker, text);
        }
      }
    }
  }

  /**
   * 1.4 Bias transcription output with custom vocabulary.
   */
  public void transcribeWithCustomVocabulary(Path audioFile) throws IOException {
    System.out.println("\n=== [Sync 4] Transcription with Custom Vocabulary ===");
    byte[] audioBytes = Files.readAllBytes(audioFile);

    GenerateContentConfig config =
        GenerateContentConfig.builder()
            .audioTranscriptionConfig(
                AudioTranscriptionConfig.builder()
                    .languageCodes("en-US")
                    .customVocabulary("oatmilk", "oz")
                    .build())
            .build();

    Content content =
        Content.fromParts(Part.fromBytes(audioBytes, "audio/wav"));

    GenerateContentResponse response =
        client.models.generateContent(MODEL_ID, content, config);

    printPartsText(response);
  }

  /**
   * 1.5 Long audio files for batch transcription (supports up to 1 hour).
   */
  public void transcribeLongAudioBatch(Path audioFile) throws IOException {
    System.out.println("\n=== [Sync 5] Long Audio Batch Transcription ===");
    byte[] audioBytes = Files.readAllBytes(audioFile);

    GenerateContentConfig config =
        GenerateContentConfig.builder()
            .audioTranscriptionConfig(AudioTranscriptionConfig.builder().build())
            .build();

    Content content =
        Content.fromParts(Part.fromBytes(audioBytes, "audio/wav"));

    GenerateContentResponse response =
        client.models.generateContent(MODEL_ID, content, config);

    printPartsText(response);
  }

  private void printPartsText(GenerateContentResponse response) {
    List<Part> parts = response.parts();
    if (parts != null) {
      String text =
          parts.stream()
              .map(p -> p.text().orElse(""))
              .filter(t -> !t.isEmpty())
              .collect(Collectors.joining());
      if (!text.isEmpty()) {
        System.out.println(text);
      }
    }
  }

  // =========================================================================
  // 2. STREAMING TRANSCRIPTION (gemini-3.5-transcribe-live)
  // =========================================================================

  /**
   * Reads a WAV audio file in 100ms blocks, downmixes multi-channel audio to 16-bit mono PCM,
   * and streams each block via {@code session.sendRealtimeInput}, pacing at real-time speed.
   */
  public void sendStreamingAudio(AsyncSession session, Path audioFile) {
    try (AudioInputStream rawInput = AudioSystem.getAudioInputStream(audioFile.toFile())) {
      AudioFormat sourceFormat = rawInput.getFormat();
      int sampleRate = (int) sourceFormat.getSampleRate();
      int channels = sourceFormat.getChannels();

      // Ensure 16-bit signed little-endian PCM frames
      AudioFormat pcm16Format =
          new AudioFormat(
              AudioFormat.Encoding.PCM_SIGNED,
              sampleRate,
              16,
              channels,
              channels * 2,
              sampleRate,
              false);

      try (AudioInputStream pcmStream = AudioSystem.getAudioInputStream(pcm16Format, rawInput)) {
        double chunkDurationSec = 0.1; // 100ms chunks
        int chunkFrames = (int) (sampleRate * chunkDurationSec);
        int bytesPerFrame = channels * 2;
        byte[] buffer = new byte[chunkFrames * bytesPerFrame];
        String mimeType = "audio/pcm;rate=" + sampleRate;

        int bytesRead;
        while ((bytesRead = pcmStream.read(buffer)) != -1) {
          byte[] monoPcmBytes =
              (channels > 1)
                  ? downmixToMono16BitLe(buffer, bytesRead, channels)
                  : copyBytes(buffer, bytesRead);

          LiveSendRealtimeInputParameters params =
              LiveSendRealtimeInputParameters.builder()
                  .audio(
                      Blob.builder()
                          .data(monoPcmBytes)
                          .mimeType(mimeType)
                          .build())
                  .build();

          session.sendRealtimeInput(params).join();
          Thread.sleep((long) (chunkDurationSec * 1000));
        }
      }
    } catch (Exception e) {
      System.err.println("Error reading/streaming WAV file: " + e.getMessage());
    } finally {
      // Signal end of audio stream
      session
          .sendRealtimeInput(
              LiveSendRealtimeInputParameters.builder().audioStreamEnd(true).build())
          .join();
    }
  }

  /**
   * Processes incoming {@link LiveServerMessage} frames from the Live session, printing
   * {@code interimInputTranscription} updates and appending finalized {@code inputTranscription}
   * segments to {@code transcriptBuffer}.
   */
  public void handleStreamingMessage(
      LiveServerMessage message, List<String> transcriptBuffer) {
    // Optionally dump every raw server message (set LIVE_DEBUG=true)
    if (Boolean.parseBoolean(System.getenv().getOrDefault("LIVE_DEBUG", "false"))) {
      System.out.println("\n[Raw LiveServerMessage] " + message.toJson());
    }
    if (message.serverContent().isEmpty()) {
      return;
    }
    LiveServerContent serverContent = message.serverContent().get();

    // Track the active interim segment
    serverContent
        .interimInputTranscription()
        .flatMap(Transcription::text)
        .filter(t -> !t.isEmpty())
        .ifPresent(
            interimText -> {
              String activeView =
                  String.join(" ", transcriptBuffer)
                      + (transcriptBuffer.isEmpty() ? "" : " ")
                      + interimText;
              System.out.print("\r[Interim] " + activeView);
            });

    // Save final transcript segment and clear interim
    serverContent
        .inputTranscription()
        .flatMap(Transcription::text)
        .filter(t -> !t.isEmpty())
        .ifPresent(
            finalText -> {
              transcriptBuffer.add(finalText);
              String activeView = String.join(" ", transcriptBuffer);
              System.out.println("\r[Final]   " + activeView);
            });
  }

  /**
   * Opens a Live session with {@code gemini-3.5-transcribe-live}, verifies
   * {@code setupComplete}, streams the audio file, and collects the transcript.
   */
  public String streamingMain(Path audioFile, LiveConnectConfig config) throws Exception {
    List<String> transcriptBuffer = Collections.synchronizedList(new ArrayList<>());

    AsyncSession session =
        client.async.live.connect(MODEL_ID_LIVE, config).get(30, TimeUnit.SECONDS);
    try {
      if (session.setupComplete() == null) {
        System.err.println("No setup_complete received from server.");
        return "";
      }

      CompletableFuture<Void> recvTask =
          session.receive(msg -> handleStreamingMessage(msg, transcriptBuffer));
      CompletableFuture<Void> sendTask =
          CompletableFuture.runAsync(() -> sendStreamingAudio(session, audioFile));

      sendTask.join();
      try {
        // Give receiver up to 5 seconds to process trailing server messages
        recvTask.get(5, TimeUnit.SECONDS);
      } catch (Exception ignored) {
        // Expected timeout once trailing messages finish arriving
      }
    } finally {
      session.close().join();
    }

    return String.join(" ", transcriptBuffer);
  }

  /**
   * 2.1 Streaming transcription with automatic language recognition.
   */
  public void streamAutomaticLanguageRecognition(Path audioFile) throws Exception {
    System.out.println("\n=== [Stream 1] Streaming with Automatic Language Recognition ===");
    LiveConnectConfig config =
        LiveConnectConfig.builder()
            .responseModalities("TEXT")
            .inputAudioTranscription(AudioTranscriptionConfig.builder().build())
            .build();
    streamingMain(audioFile, config);
  }

  /**
   * 2.2 Streaming transcription with specific language codes (e.g., Korean and English).
   */
  public void streamWithLanguageCodes(Path audioFile) throws Exception {
    System.out.println("\n=== [Stream 2] Streaming with Language Codes (ko-KR, en-US) ===");
    LiveConnectConfig config =
        LiveConnectConfig.builder()
            .responseModalities("TEXT")
            .inputAudioTranscription(
                AudioTranscriptionConfig.builder()
                    .languageCodes("ko-KR", "en-US")
                    .build())
            .build();
    streamingMain(audioFile, config);
  }

  /**
   * 2.3 Streaming transcription with custom vocabulary.
   */
  public void streamWithCustomVocabulary(Path audioFile) throws Exception {
    System.out.println("\n=== [Stream 3] Streaming with Custom Vocabulary ===");
    LiveConnectConfig config =
        LiveConnectConfig.builder()
            .responseModalities("TEXT")
            .inputAudioTranscription(
                AudioTranscriptionConfig.builder()
                    .languageCodes("en-US")
                    .customVocabulary("oatmilk", "oz")
                    .build())
            .build();
    streamingMain(audioFile, config);
  }

  /**
   * 2.4 Transcribing long audio with overlapping session chunking (2.5-minute chunks with 500ms overlap).
   */
  public String transcribeLongAudioStreamingChunked(Path wavFile, LiveConnectConfig config)
      throws Exception {
    System.out.println("\n=== [Stream 4] Streaming Long Audio with Session Chunking ===");
    long chunkLengthMs = 150_000L; // 2.5 minutes
    long overlapMs = 500L;         // 500ms overlap
    Path tempChunkPath = Files.createTempFile("temp_chunk_", ".wav");
    List<String> fullTranscript = new ArrayList<>();

    try (AudioInputStream rawInput = AudioSystem.getAudioInputStream(wavFile.toFile())) {
      AudioFormat format = rawInput.getFormat();
      byte[] allBytes = rawInput.readAllBytes();
      double bytesPerMs = (format.getFrameRate() * format.getFrameSize()) / 1000.0;
      long totalDurationMs = (long) (allBytes.length / bytesPerMs);

      int chunkIndex = 0;
      for (long chunkStartMs = 0; chunkStartMs < totalDurationMs; chunkStartMs += chunkLengthMs) {
        long actualStartMs = (chunkIndex > 0) ? Math.max(0, chunkStartMs - overlapMs) : chunkStartMs;
        long chunkEndMs = Math.min(chunkStartMs + chunkLengthMs, totalDurationMs);

        int startByte = alignToFrame((int) (actualStartMs * bytesPerMs), format.getFrameSize());
        int endByte =
            Math.min(
                allBytes.length,
                alignToFrame((int) (chunkEndMs * bytesPerMs), format.getFrameSize()));
        int lengthBytes = endByte - startByte;
        long frameLength = lengthBytes / format.getFrameSize();

        try (ByteArrayInputStream bais = new ByteArrayInputStream(allBytes, startByte, lengthBytes);
            AudioInputStream chunkStream = new AudioInputStream(bais, format, frameLength)) {
          AudioSystem.write(chunkStream, AudioFileFormat.Type.WAVE, tempChunkPath.toFile());
        }

        chunkIndex++;
        System.out.printf("Streaming Chunk %d (%d ms - %d ms)...%n", chunkIndex, actualStartMs, chunkEndMs);
        String chunkTranscript = streamingMain(tempChunkPath, config);
        fullTranscript.add(chunkTranscript);
      }
    } finally {
      Files.deleteIfExists(tempChunkPath);
    }

    return String.join("\n\n", fullTranscript);
  }

  /**
   * 2.5 Real-time streaming speech recognition directly from a microphone for {@code durationSeconds}.
   */
  public void streamFromMicrophone(int durationSeconds) throws Exception {
    System.out.println("\n=== [Stream 5] Real-Time Microphone Streaming ===");
    int sampleRate = 16000;
    AudioFormat format = new AudioFormat(sampleRate, 16, 1, true, false);
    DataLine.Info info = new DataLine.Info(TargetDataLine.class, format);

    if (!AudioSystem.isLineSupported(info)) {
      System.out.println("No compatible microphone line found on this machine; skipping live mic capture.");
      return;
    }

    LiveConnectConfig config =
        LiveConnectConfig.builder()
            .responseModalities("TEXT")
            .inputAudioTranscription(AudioTranscriptionConfig.builder().build())
            .build();

    List<String> transcriptBuffer = Collections.synchronizedList(new ArrayList<>());
    AsyncSession session =
        client.async.live.connect(MODEL_ID_LIVE, config).get(30, TimeUnit.SECONDS);

    try (TargetDataLine microphone = (TargetDataLine) AudioSystem.getLine(info)) {
      microphone.open(format);
      microphone.start();

      CompletableFuture<Void> recvTask =
          session.receive(msg -> handleStreamingMessage(msg, transcriptBuffer));

      byte[] buffer = new byte[(int) (sampleRate * 0.1) * 2]; // 100ms of 16-bit mono PCM
      String mimeType = "audio/pcm;rate=" + sampleRate;
      long endTimeMs = System.currentTimeMillis() + (durationSeconds * 1000L);

      System.out.printf("Recording from microphone for %d seconds...%n", durationSeconds);
      while (System.currentTimeMillis() < endTimeMs) {
        int read = microphone.read(buffer, 0, buffer.length);
        if (read > 0) {
          session
              .sendRealtimeInput(
                  LiveSendRealtimeInputParameters.builder()
                      .audio(
                          Blob.builder()
                              .data(copyBytes(buffer, read))
                              .mimeType(mimeType)
                              .build())
                      .build())
              .join();
        }
      }

      session
          .sendRealtimeInput(
              LiveSendRealtimeInputParameters.builder().audioStreamEnd(true).build())
          .join();
      try {
        recvTask.get(5, TimeUnit.SECONDS);
      } catch (Exception ignored) {
      }
    } finally {
      session.close().join();
    }
  }

  // =========================================================================
  // UTILITIES
  // =========================================================================

  private static byte[] downmixToMono16BitLe(byte[] stereoBytes, int length, int channels) {
    int frameSize = channels * 2;
    int numFrames = length / frameSize;
    ByteBuffer inBuf = ByteBuffer.wrap(stereoBytes, 0, length).order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer outBuf = ByteBuffer.allocate(numFrames * 2).order(ByteOrder.LITTLE_ENDIAN);

    for (int f = 0; f < numFrames; f++) {
      int sum = 0;
      for (int c = 0; c < channels; c++) {
        sum += inBuf.getShort();
      }
      outBuf.putShort((short) (sum / channels));
    }
    return outBuf.array();
  }

  private static byte[] copyBytes(byte[] src, int length) {
    byte[] copy = new byte[length];
    System.arraycopy(src, 0, copy, 0, length);
    return copy;
  }

  private static int alignToFrame(int byteOffset, int frameSize) {
    return (byteOffset / frameSize) * frameSize;
  }

  public static Path downloadSampleAudio(String httpsUrl, String localFileName)
      throws IOException, InterruptedException {
    URI uri = URI.create(httpsUrl);
    if (!"https".equalsIgnoreCase(uri.getScheme())
        || !ALLOWED_SAMPLE_HOSTS.contains(uri.getHost())) {
      throw new IllegalArgumentException("Untrusted sample audio URL: " + httpsUrl);
    }
    Path safeTarget = Path.of(localFileName).getFileName();
    HttpClient httpClient =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
    HttpRequest request = HttpRequest.newBuilder(uri).GET().build();
    HttpResponse<InputStream> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
    if (response.statusCode() != 200) {
      throw new IOException("Failed to download " + httpsUrl + ": HTTP " + response.statusCode());
    }
    try (InputStream in = response.body()) {
      Files.copy(in, safeTarget, StandardCopyOption.REPLACE_EXISTING);
    }
    return safeTarget;
  }

  @Override
  public void close() {
    if (client != null) {
      client.close();
    }
  }

  public static void main(String[] args) throws Exception {
    String projectId =
        System.getenv().getOrDefault("GOOGLE_CLOUD_PROJECT", "[your-project-id]");
    if (projectId.isBlank() || "[your-project-id]".equals(projectId)) {
      throw new IllegalStateException(
          "Please set the GOOGLE_CLOUD_PROJECT environment variable before running.");
    }
    String location =
        System.getenv().getOrDefault("GOOGLE_CLOUD_REGION", "global");

    // 1. Download sample audio files used in gemini_3_5_transcribe.ipynb
    Path storyWav =
        downloadSampleAudio(
            "https://storage.googleapis.com/cloud-samples-data/generative-ai/audio/tell-a-story.wav",
            "tell-a-story.wav");
    Path hobbiesWav =
        downloadSampleAudio(
            "https://storage.googleapis.com/cloud-samples-data/generative-ai/audio/hobbies.wav",
            "hobbies.wav");
    Path coffeeWav =
        downloadSampleAudio(
            "https://storage.googleapis.com/cloud-samples-data/generative-ai/audio/coffee_order.wav",
            "coffee_order.wav");
    Path koreanEnglishWav =
        downloadSampleAudio(
            "https://storage.googleapis.com/cloud-samples-data/generative-ai/audio/korean-english.wav",
            "korean-english.wav");

    try (Gemini35TranscribeDemo demo = new Gemini35TranscribeDemo(projectId, location)) {
      // 2. Run Synchronous Examples (gemini-3.5-transcribe-preview)
      demo.transcribeWithTimestamps(storyWav);
      demo.transcribeWithLanguageCodesFromGcs(
          "gs://cloud-samples-data/generative-ai/audio/spanish.wav");
      demo.transcribeWithSpeakerDiarization(hobbiesWav);
      demo.transcribeWithCustomVocabulary(coffeeWav);

      // 3. Run Streaming Examples (gemini-3.5-transcribe-live)
      demo.streamAutomaticLanguageRecognition(storyWav);
      demo.streamWithLanguageCodes(koreanEnglishWav);
      demo.streamWithCustomVocabulary(coffeeWav);

      LiveConnectConfig defaultLiveConfig =
          LiveConnectConfig.builder()
              .responseModalities("TEXT")
              .inputAudioTranscription(AudioTranscriptionConfig.builder().build())
              .build();
      demo.transcribeLongAudioStreamingChunked(storyWav, defaultLiveConfig);
    }
  }
}
