/*
 * Copyright 2025 Google LLC
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.ClientStream;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.speech.v2.AutoDetectDecodingConfig;
import com.google.cloud.speech.v2.BatchRecognizeFileMetadata;
import com.google.cloud.speech.v2.BatchRecognizeFileResult;
import com.google.cloud.speech.v2.BatchRecognizeRequest;
import com.google.cloud.speech.v2.BatchRecognizeResponse;
import com.google.cloud.speech.v2.GcsOutputConfig;
import com.google.cloud.speech.v2.InlineOutputConfig;
import com.google.cloud.speech.v2.OperationMetadata;
import com.google.cloud.speech.v2.RecognitionConfig;
import com.google.cloud.speech.v2.RecognitionFeatures;
import com.google.cloud.speech.v2.RecognitionOutputConfig;
import com.google.cloud.speech.v2.RecognizeRequest;
import com.google.cloud.speech.v2.RecognizeResponse;
import com.google.cloud.speech.v2.RecognizerName;
import com.google.cloud.speech.v2.SpeakerDiarizationConfig;
import com.google.cloud.speech.v2.SpeechClient;
import com.google.cloud.speech.v2.SpeechRecognitionAlternative;
import com.google.cloud.speech.v2.SpeechRecognitionResult;
import com.google.cloud.speech.v2.SpeechSettings;
import com.google.cloud.speech.v2.StreamingRecognitionConfig;
import com.google.cloud.speech.v2.StreamingRecognitionResult;
import com.google.cloud.speech.v2.StreamingRecognizeRequest;
import com.google.cloud.speech.v2.StreamingRecognizeResponse;
import com.google.cloud.speech.v2.WordInfo;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Java equivalent of {@code get_started_with_chirp_3_transcription.ipynb}.
 *
 * <p>Demonstrates Google Cloud Speech-to-Text V2 with the {@code chirp_3} model:
 * <ol>
 *   <li>Online / Synchronous speech recognition ({@code client.recognize} with local audio bytes)</li>
 *   <li>Language-agnostic transcription ({@code languageCodes=["auto"]} with a Cloud Storage URI)</li>
 *   <li>Speaker diarization with Batch Recognition ({@code client.batchRecognizeAsync}) &amp;
 *       grouping utterances by speaker</li>
 *   <li>Streaming speech recognition ({@code client.streamingRecognizeCallable}) with chunked audio</li>
 * </ol>
 */
public class Chirp3TranscribeDemo implements AutoCloseable {

  public static final String MODEL = "chirp_3";
  public static final long MAX_AUDIO_LENGTH_SECS = 8L * 60L * 60L;
  public static final int CHUNK_SIZE = 3200;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final SpeechClient client;
  private final String recognizer;

  public Chirp3TranscribeDemo(String projectId, String sttLocation) throws IOException {
    // Equivalent to:
    // client = SpeechClient(client_options=ClientOptions(api_endpoint=f"{STT_LOCATION}-speech.googleapis.com"))
    // recognizer = client.recognizer_path(PROJECT_ID, STT_LOCATION, "_")
    SpeechSettings settings =
        SpeechSettings.newBuilder()
            .setEndpoint(sttLocation + "-speech.googleapis.com:443")
            .build();
    this.client = SpeechClient.create(settings);
    this.recognizer = RecognizerName.of(projectId, sttLocation, "_").toString();
  }

  // =========================================================================
  // HELPER FUNCTIONS (equivalent to Cell 17)
  // =========================================================================

  /**
   * Prints the primary transcript alternative for each recognition result.
   */
  public static void printTranscript(List<SpeechRecognitionResult> results) {
    for (SpeechRecognitionResult result : results) {
      if (result.getAlternativesCount() > 0) {
        System.out.println(result.getAlternatives(0).getTranscript());
      }
    }
  }

  /**
   * Splits raw audio byte data into smaller chunks of {@code chunkSize} bytes.
   */
  public static List<byte[]> generateAudioChunks(byte[] audioContent, int chunkSize) {
    List<byte[]> chunks = new ArrayList<>();
    for (int startIndex = 0; startIndex < audioContent.length; startIndex += chunkSize) {
      int endIndex = Math.min(startIndex + chunkSize, audioContent.length);
      chunks.add(Arrays.copyOfRange(audioContent, startIndex, endIndex));
    }
    return chunks;
  }

  /**
   * Reads a BatchRecognize JSON output file containing transcribed words and groups them
   * into sentences spoken by each speaker (matching {@code group_utterances_by_speaker_from_file}).
   */
  public static String groupUtterancesBySpeakerFromFile(Path jsonFilePath) throws IOException {
    String jsonDataString = Files.readString(jsonFilePath);
    Pattern wordsPattern = Pattern.compile("\"words\":\\s*(\\[.*?\\])", Pattern.DOTALL);
    Matcher matcher = wordsPattern.matcher(jsonDataString);
    if (!matcher.find()) {
      return "{\"dialogue\": []}";
    }

    JsonNode wordsList = OBJECT_MAPPER.readTree(matcher.group(1));
    ObjectNode root = OBJECT_MAPPER.createObjectNode();
    ArrayNode dialogue = root.putArray("dialogue");

    if (wordsList.isArray() && !wordsList.isEmpty()) {
      String currentSpeaker = wordsList.get(0).path("speakerLabel").asText("");
      List<String> currentUtteranceWords = new ArrayList<>();

      for (JsonNode item : wordsList) {
        String word = item.path("word").asText("");
        String speaker = item.path("speakerLabel").asText("");
        if (!speaker.equals(currentSpeaker)) {
          ObjectNode entry = dialogue.addObject();
          entry.put("speaker", currentSpeaker);
          entry.put("text", String.join(" ", currentUtteranceWords));
          currentSpeaker = speaker;
          currentUtteranceWords = new ArrayList<>();
          currentUtteranceWords.add(word);
        } else {
          currentUtteranceWords.add(word);
        }
      }

      if (currentSpeaker != null && !currentUtteranceWords.isEmpty()) {
        ObjectNode entry = dialogue.addObject();
        entry.put("speaker", currentSpeaker);
        entry.put("text", String.join(" ", currentUtteranceWords));
      }
    }

    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root);
  }

  /**
   * Groups words by speaker directly from in-memory {@link SpeechRecognitionResult} objects.
   */
  public static String groupUtterancesBySpeaker(List<SpeechRecognitionResult> results)
      throws IOException {
    ObjectNode root = OBJECT_MAPPER.createObjectNode();
    ArrayNode dialogue = root.putArray("dialogue");

    String currentSpeaker = null;
    List<String> currentUtteranceWords = new ArrayList<>();

    for (SpeechRecognitionResult result : results) {
      if (result.getAlternativesCount() == 0) {
        continue;
      }
      SpeechRecognitionAlternative alt = result.getAlternatives(0);
      for (WordInfo wordInfo : alt.getWordsList()) {
        String word = wordInfo.getWord();
        String speaker = wordInfo.getSpeakerLabel();
        if (currentSpeaker == null) {
          currentSpeaker = speaker;
        }
        if (!speaker.equals(currentSpeaker)) {
          ObjectNode entry = dialogue.addObject();
          entry.put("speaker", currentSpeaker);
          entry.put("text", String.join(" ", currentUtteranceWords));
          currentSpeaker = speaker;
          currentUtteranceWords = new ArrayList<>();
          currentUtteranceWords.add(word);
        } else {
          currentUtteranceWords.add(word);
        }
      }
    }

    if (currentSpeaker != null && !currentUtteranceWords.isEmpty()) {
      ObjectNode entry = dialogue.addObject();
      entry.put("speaker", currentSpeaker);
      entry.put("text", String.join(" ", currentUtteranceWords));
    }

    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root);
  }

  // =========================================================================
  // 1. ONLINE / SYNCHRONOUS SPEECH RECOGNITION (Cells 20-22)
  // =========================================================================

  /**
   * Performs online (synchronous) speech recognition on a local audio file (< 1 minute)
   * using {@code chirp_3} and {@code en-US}.
   */
  public void recognizeSynchronous(Path audioFile) throws IOException {
    System.out.println("\n=== [Chirp 3 - 1] Online / Synchronous Speech Recognition ===");
    byte[] audioContent = Files.readAllBytes(audioFile);

    RecognitionConfig config =
        RecognitionConfig.newBuilder()
            .setAutoDecodingConfig(AutoDetectDecodingConfig.getDefaultInstance())
            .setModel(MODEL)
            .addLanguageCodes("en-US")
            .build();

    RecognizeRequest request =
        RecognizeRequest.newBuilder()
            .setRecognizer(recognizer)
            .setConfig(config)
            .setContent(ByteString.copyFrom(audioContent))
            .build();

    RecognizeResponse response = client.recognize(request);
    printTranscript(response.getResultsList());
  }

  // =========================================================================
  // 2. LANGUAGE-AGNOSTIC TRANSCRIPTION (Cells 24-26)
  // =========================================================================

  /**
   * Performs a language-agnostic transcription ({@code language_codes=["auto"]}) on an audio
   * file stored in Google Cloud Storage.
   */
  public void recognizeLanguageAgnostic(String audioGcsUri) {
    System.out.println("\n=== [Chirp 3 - 2] Language-Agnostic Transcription (language_codes=[\"auto\"]) ===");

    RecognitionConfig config =
        RecognitionConfig.newBuilder()
            .setAutoDecodingConfig(AutoDetectDecodingConfig.getDefaultInstance())
            .setModel(MODEL)
            .addLanguageCodes("auto")
            .build();

    RecognizeRequest request =
        RecognizeRequest.newBuilder()
            .setRecognizer(recognizer)
            .setConfig(config)
            .setUri(audioGcsUri)
            .build();

    RecognizeResponse response = client.recognize(request);
    printTranscript(response.getResultsList());
  }

  // =========================================================================
  // 3. SPEAKER DIARIZATION WITH BATCH RECOGNITION (Cells 28-30)
  // =========================================================================

  /**
   * Performs batch recognition with speaker diarization enabled on a Cloud Storage audio URI.
   *
   * <p>If {@code gcsOutputFolder} is provided (non-empty and not a placeholder), outputs results
   * to GCS via {@link GcsOutputConfig} just like Cell 30 of the notebook. Otherwise uses
   * {@link InlineOutputConfig} so speaker-grouped JSON can be printed immediately without a bucket.
   */
  public void batchRecognizeWithDiarization(String audioGcsUri, String gcsOutputFolder)
      throws Exception {
    System.out.println("\n=== [Chirp 3 - 3] Speaker Diarization (Batch Recognition) ===");

    RecognitionConfig config =
        RecognitionConfig.newBuilder()
            .setAutoDecodingConfig(AutoDetectDecodingConfig.getDefaultInstance())
            .setFeatures(
                RecognitionFeatures.newBuilder()
                    .setDiarizationConfig(SpeakerDiarizationConfig.getDefaultInstance())
                    .build())
            .setModel(MODEL)
            .addLanguageCodes("en-US")
            .build();

    BatchRecognizeFileMetadata fileMetadata =
        BatchRecognizeFileMetadata.newBuilder().setUri(audioGcsUri).build();

    boolean useGcsOutput =
        gcsOutputFolder != null
            && !gcsOutputFolder.isBlank()
            && !gcsOutputFolder.contains("[your-bucket-path]");

    RecognitionOutputConfig outputConfig =
        useGcsOutput
            ? RecognitionOutputConfig.newBuilder()
                .setGcsOutputConfig(GcsOutputConfig.newBuilder().setUri(gcsOutputFolder).build())
                .build()
            : RecognitionOutputConfig.newBuilder()
                .setInlineResponseConfig(InlineOutputConfig.getDefaultInstance())
                .build();

    BatchRecognizeRequest request =
        BatchRecognizeRequest.newBuilder()
            .setRecognizer(recognizer)
            .setConfig(config)
            .addFiles(fileMetadata)
            .setRecognitionOutputConfig(outputConfig)
            .build();

    OperationFuture<BatchRecognizeResponse, OperationMetadata> operation =
        client.batchRecognizeAsync(request);
    BatchRecognizeResponse response =
        operation.get(MAX_AUDIO_LENGTH_SECS, TimeUnit.SECONDS);

    BatchRecognizeFileResult fileResult = response.getResultsMap().get(audioGcsUri);
    if (fileResult != null) {
      if (useGcsOutput && fileResult.hasCloudStorageResult()) {
        String transcriptGcsUri = fileResult.getCloudStorageResult().getUri();
        System.out.println("Batch transcript saved to GCS URI: " + transcriptGcsUri);
      } else if (fileResult.hasInlineResult()) {
        String dialogueJson =
            groupUtterancesBySpeaker(fileResult.getInlineResult().getTranscript().getResultsList());
        System.out.println(dialogueJson);
      }
    }
  }

  // =========================================================================
  // 4. STREAMING SPEECH RECOGNITION (Cells 35-36)
  // =========================================================================

  /**
   * Streams an audio file in {@code CHUNK_SIZE} (3200-byte) chunks to {@code chirp_3} with
   * automatic language detection ({@code language_codes=["auto"]}), printing each streaming
   * transcript and the final combined transcript.
   */
  public String streamingRecognize(Path audioFilePath) throws Exception {
    System.out.println("\n=== [Chirp 3 - 4] Streaming Speech Recognition ===");

    RecognitionConfig recognitionConfig =
        RecognitionConfig.newBuilder()
            .setAutoDecodingConfig(AutoDetectDecodingConfig.getDefaultInstance())
            .addLanguageCodes("auto")
            .setModel(MODEL)
            .build();

    StreamingRecognitionConfig streamingConfig =
        StreamingRecognitionConfig.newBuilder()
            .setConfig(recognitionConfig)
            .build();

    List<String> allTranscripts = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch doneLatch = new CountDownLatch(1);

    ResponseObserver<StreamingRecognizeResponse> responseObserver =
        new ResponseObserver<>() {
          @Override
          public void onStart(StreamController controller) {
            System.out.println("Streaming transcripts:");
          }

          @Override
          public void onResponse(StreamingRecognizeResponse response) {
            for (StreamingRecognitionResult result : response.getResultsList()) {
              if (result.getAlternativesCount() > 0) {
                String transcript = result.getAlternatives(0).getTranscript();
                System.out.println(transcript);
                allTranscripts.add(transcript);
              }
            }
          }

          @Override
          public void onError(Throwable t) {
            System.err.println("Streaming recognition error: " + t.getMessage());
            doneLatch.countDown();
          }

          @Override
          public void onComplete() {
            doneLatch.countDown();
          }
        };

    ClientStream<StreamingRecognizeRequest> clientStream =
        client.streamingRecognizeCallable().splitCall(responseObserver);

    // a. First, send the initial configuration request.
    StreamingRecognizeRequest configRequest =
        StreamingRecognizeRequest.newBuilder()
            .setRecognizer(recognizer)
            .setStreamingConfig(streamingConfig)
            .build();
    clientStream.send(configRequest);

    // b. Second, read the audio file and stream it in 3200-byte chunks.
    byte[] audioContent = Files.readAllBytes(audioFilePath);
    for (byte[] chunk : generateAudioChunks(audioContent, CHUNK_SIZE)) {
      StreamingRecognizeRequest audioRequest =
          StreamingRecognizeRequest.newBuilder()
              .setAudio(ByteString.copyFrom(chunk))
              .build();
      clientStream.send(audioRequest);
    }

    clientStream.closeSend();
    doneLatch.await(2, TimeUnit.MINUTES);

    String finalTranscript = String.join(" ", allTranscripts);
    System.out.println("\n--- Final Combined Transcript ---\n" + finalTranscript);
    return finalTranscript;
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
    String sttLocation =
        System.getenv().getOrDefault("STT_LOCATION", "us");
    String gcsOutputFolder =
        System.getenv().getOrDefault("GCS_OUTPUT_FOLDER", "gs://[your-bucket-path]");

    // Download sample audio file used in get_started_with_chirp_3_transcription.ipynb
    Path cleanEnergyMp3 =
        Gemini35TranscribeDemo.downloadSampleAudio(
            "https://storage.googleapis.com/cloud-samples-data/generative-ai/audio/audio_summary_clean_energy_short.mp3",
            "audio_summary_clean_energy_short.mp3");

    try (Chirp3TranscribeDemo demo = new Chirp3TranscribeDemo(projectId, sttLocation)) {
      // 1. Online/Synchronous speech recognition (< 1 minute local audio)
      demo.recognizeSynchronous(cleanEnergyMp3);

      // 2. Language-agnostic transcription (language_codes=["auto"] with Cloud Storage URI)
      demo.recognizeLanguageAgnostic(
          "gs://cloud-samples-data/generative-ai/audio/spanish.wav");

      // 3. Speaker Diarization with Batch Recognition
      demo.batchRecognizeWithDiarization(
          "gs://cloud-samples-data/generative-ai/audio/Chirp-3-Docs-Dive.mp3",
          gcsOutputFolder);

      // 4. Streaming speech recognition with 3200-byte chunks
      demo.streamingRecognize(cleanEnergyMp3);
    }
  }
}
