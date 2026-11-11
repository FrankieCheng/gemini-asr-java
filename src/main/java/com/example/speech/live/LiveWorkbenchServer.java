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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;

/**
 * 交互式长语音工程工作台 HTTP + SSE 服务端。
 *
 * <p>功能：
 * <ol>
 *   <li>提供 982.4s 实测日志（{@code logs/*.events.jsonl} 与 {@code logs/*.tsv}）及支持 HTTP Range 拖拽的 WAV 音频流；</li>
 *   <li>直接在页面配置并启动 {@link RotatingLiveTranscriber} 实时流转写，通过 SSE 推送 100ms 帧级 RMS/P10/P80 遥测与双会话状态；</li>
 *   <li>支持运行中一键注入【会话内软刷】【Make-Before-Break 轮换】【模拟网络闪断】；</li>
 *   <li>暴露 {@link RotatingLiveTranscriber#dropOverlap} 真实词级去重 API 沙盒。</li>
 * </ol>
 */
public final class LiveWorkbenchServer {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Path WEB_ROOT = Path.of("src/main/resources/web");
  private static final Path LOGS_DIR = Path.of("logs");

  private final List<OutputStream> sseClients = new CopyOnWriteArrayList<>();
  private final VadDiagnosticLab vadLab = new VadDiagnosticLab(this::broadcastSse);
  private volatile RotatingLiveTranscriber activeTranscriber;
  private volatile Thread liveRunnerThread;
  private volatile boolean stopRequested = false;

  private volatile AdkReferenceLiveTranscriber activeCompareAdk;
  private volatile RotatingLiveTranscriber activeCompareMbb;
  private volatile RotatingLiveTranscriber activeCompareHyb;
  private volatile Thread compareRunnerThread;
  private volatile boolean compareStopRequested = false;

  public static void main(String[] args) throws Exception {
    if (args.length > 0 && args[0].startsWith("--generate-preset=")) {
      String preset = args[0].substring("--generate-preset=".length());
      LiveWorkbenchServer srv = new LiveWorkbenchServer();
      srv.generatePresetSnapshotBlocking(preset);
      return;
    }
    int port = Integer.parseInt(System.getProperty("port", "8080"));
    if (args.length > 0) {
      port = Integer.parseInt(args[0]);
    }
    LiveWorkbenchServer srv = new LiveWorkbenchServer();
    srv.start(port);
  }

  public void start(int port) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
    server.setExecutor(Executors.newCachedThreadPool());

    server.createContext("/", this::handleStatic);
    server.createContext("/api/logs", this::handleLogsList);
    server.createContext("/api/log-detail", this::handleLogDetail);
    server.createContext("/api/audio", this::handleAudioRange);
    server.createContext("/api/drop-overlap", this::handleDropOverlap);
    server.createContext("/api/live/events", this::handleSse);
    server.createContext("/api/live/start", this::handleLiveStart);
    server.createContext("/api/live/action", this::handleLiveAction);
    server.createContext("/api/vad-lab/run", this::handleVadLabRun);
    server.createContext("/api/vad-lab/step", this::handleVadLabStep);
    server.createContext("/api/compare/start", this::handleCompareStart);
    server.createContext("/api/compare/action", this::handleCompareAction);
    server.createContext("/api/compare/snapshot", this::handleCompareSnapshot);

    server.start();
    String host = InetAddress.getLocalHost().getHostName();
    if (!host.endsWith(".googlers.com")) {
      host = host + ".c.googlers.com";
    }
    System.out.printf(
        Locale.ROOT,
        "====================================================================%n"
            + "Gemini 3.5 Live 长语音工程工作台已启动:%n"
            + "  本地访问:   http://localhost:%d%n"
            + "  Cloudtop:   http://%s:%d%n"
            + "====================================================================%n",
        port,
        host,
        port);
  }

  // ===============================================================================================
  // 1. 静态资源服务
  // ===============================================================================================

  private void handleStatic(HttpExchange ex) throws IOException {
    String path = ex.getRequestURI().getPath();
    if ("/".equals(path) || path.isBlank()) {
      path = "/index.html";
    }
    String safeName = Path.of(path).getFileName().toString();
    Path file = WEB_ROOT.resolve(safeName);
    if (!Files.isRegularFile(file)) {
      sendJson(ex, 404, "{\"error\":\"Not found\"}");
      return;
    }
    String contentType =
        safeName.endsWith(".css")
            ? "text/css; charset=utf-8"
            : safeName.endsWith(".js")
                ? "application/javascript; charset=utf-8"
                : "text/html; charset=utf-8";
    byte[] bytes = Files.readAllBytes(file);
    ex.getResponseHeaders().set("Content-Type", contentType);
    ex.getResponseHeaders().set("Cache-Control", "no-store");
    ex.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }

  // ===============================================================================================
  // 2. 实测日志与 WAV Range 流式回放
  // ===============================================================================================

  private void handleLogsList(HttpExchange ex) throws IOException {
    ObjectNode root = JSON.createObjectNode();
    ArrayNode arr = root.putArray("logs");
    if (Files.isDirectory(LOGS_DIR)) {
      try (DirectoryStream<Path> ds = Files.newDirectoryStream(LOGS_DIR, "*.events.jsonl")) {
        for (Path p : ds) {
          String fn = p.getFileName().toString();
          arr.add(fn.substring(0, fn.length() - ".events.jsonl".length()));
        }
      }
    }
    sendJson(ex, 200, root.toString());
  }

  private void handleLogDetail(HttpExchange ex) throws IOException {
    Map<String, String> q = parseQuery(ex.getRequestURI());
    String name = Path.of(q.getOrDefault("name", "ted_cameron")).getFileName().toString();
    Path tsvPath = LOGS_DIR.resolve(name + ".tsv");
    Path evPath = LOGS_DIR.resolve(name + ".events.jsonl");

    ObjectNode root = JSON.createObjectNode();
    root.put("name", name);
    ArrayNode sessionsArr = root.putArray("sessions");
    double maxSec = 0.0;

    if (Files.isRegularFile(tsvPath)) {
      for (String line : Files.readAllLines(tsvPath, StandardCharsets.UTF_8)) {
        if (line.startsWith("#") || line.startsWith("MERGED\t") || line.isBlank()) {
          continue;
        }
        String[] parts = line.split("\t", 7);
        if (parts.length >= 6) {
          ObjectNode s = sessionsArr.addObject();
          double startSec = Double.parseDouble(parts[1]);
          double endSec = Double.parseDouble(parts[2]);
          maxSec = Math.max(maxSec, endSec);
          s.put("id", parts[0]);
          s.put("audioStartSec", startSec);
          s.put("audioEndSec", endSec);
          s.put("endReason", parts[3]);
          s.put("goAway", Boolean.parseBoolean(parts[4]));
          s.put("finals", Integer.parseInt(parts[5]));
        }
      }
    }

    ArrayNode eventsArr = root.putArray("events");
    if (Files.isRegularFile(evPath)) {
      for (String line : Files.readAllLines(evPath, StandardCharsets.UTF_8)) {
        if (line.isBlank()) {
          continue;
        }
        try {
          JsonNode n = JSON.readTree(line);
          eventsArr.add(n);
          if (n.has("endMs") && n.get("endMs").asLong() > 0) {
            maxSec = Math.max(maxSec, n.get("endMs").asLong() / 1000.0);
          }
        } catch (Exception ignored) {
        }
      }
    }
    root.put("totalDurationSec", maxSec > 0 ? maxSec : 982.4);
    sendJson(ex, 200, root.toString());
  }

  private void handleAudioRange(HttpExchange ex) throws IOException {
    Map<String, String> q = parseQuery(ex.getRequestURI());
    String fileName = Path.of(q.getOrDefault("file", "JamesCameron.wav")).getFileName().toString();
    Path wavPath = Path.of(fileName);
    if (!Files.isRegularFile(wavPath)) {
      sendJson(ex, 404, "{\"error\":\"Audio file not found\"}");
      return;
    }

    long fileLen = Files.size(wavPath);
    String rangeHeader = ex.getRequestHeaders().getFirst("Range");
    long start = 0;
    long end = fileLen - 1;
    int statusCode = 200;

    if (rangeHeader != null && rangeHeader.startsWith("bytes=")) {
      String[] spec = rangeHeader.substring(6).split("-", 2);
      try {
        if (!spec[0].isEmpty()) {
          start = Long.parseLong(spec[0]);
        }
        if (spec.length > 1 && !spec[1].isEmpty()) {
          end = Long.parseLong(spec[1]);
        }
        end = Math.min(end, fileLen - 1);
        if (start <= end) {
          statusCode = 206;
        }
      } catch (NumberFormatException ignored) {
      }
    }

    long contentLen = end - start + 1;
    boolean isHead = "HEAD".equalsIgnoreCase(ex.getRequestMethod());
    ex.getResponseHeaders().set("Content-Type", "audio/wav");
    ex.getResponseHeaders().set("Accept-Ranges", "bytes");
    ex.getResponseHeaders().set("Content-Length", String.valueOf(contentLen));
    if (statusCode == 206) {
      ex.getResponseHeaders()
          .set("Content-Range", String.format(Locale.ROOT, "bytes %d-%d/%d", start, end, fileLen));
    }
    ex.sendResponseHeaders(statusCode, isHead ? -1 : contentLen);
    if (isHead) {
      ex.close();
      return;
    }

    try (RandomAccessFile raf = new RandomAccessFile(wavPath.toFile(), "r");
        OutputStream os = ex.getResponseBody()) {
      raf.seek(start);
      byte[] buf = new byte[65536];
      long rem = contentLen;
      while (rem > 0) {
        int r = raf.read(buf, 0, (int) Math.min(buf.length, rem));
        if (r < 0) {
          break;
        }
        os.write(buf, 0, r);
        rem -= r;
      }
    } catch (IOException ignored) {
      // Client closed stream early during seek
    }
  }

  // ===============================================================================================
  // 3. 词级重叠消重沙盒 API
  // ===============================================================================================

  private void handleDropOverlap(HttpExchange ex) throws IOException {
    if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
      sendJson(ex, 405, "{\"error\":\"POST required\"}");
      return;
    }
    JsonNode body = JSON.readTree(ex.getRequestBody());
    String tail = body.path("tail").asText("");
    String text = body.path("text").asText("");
    String[] tailWords = tail.isBlank() ? new String[0] : tail.trim().split("\\s+");
    String trimmed = RotatingLiveTranscriber.dropOverlap(tailWords, text);
    ObjectNode res = JSON.createObjectNode();
    res.put("trimmed", trimmed);
    sendJson(ex, 200, res.toString());
  }

  // ===============================================================================================
  // 4. 实时流转写控制与 SSE 遥测广播
  // ===============================================================================================

  private void handleSse(HttpExchange ex) throws IOException {
    ex.getResponseHeaders().set("Content-Type", "text/event-stream; charset=utf-8");
    ex.getResponseHeaders().set("Cache-Control", "no-cache");
    ex.getResponseHeaders().set("Connection", "keep-alive");
    ex.sendResponseHeaders(200, 0);
    OutputStream os = ex.getResponseBody();
    sseClients.add(os);

    ObjectNode init = JSON.createObjectNode();
    init.put("kind", "status");
    init.put("running", activeTranscriber != null);
    sendSseTo(os, init.toString());

    ObjectNode cmpInit = JSON.createObjectNode();
    cmpInit.put("kind", "compare");
    cmpInit.put("subkind", "status");
    cmpInit.put(
        "running",
        activeCompareAdk != null || activeCompareMbb != null || activeCompareHyb != null);
    sendSseTo(os, cmpInit.toString());
  }

  private void broadcastSse(JsonNode node) {
    String payload = node.toString();
    for (OutputStream os : sseClients) {
      if (!sendSseTo(os, payload)) {
        sseClients.remove(os);
      }
    }
  }

  private boolean sendSseTo(OutputStream os, String json) {
    try {
      byte[] data = ("data: " + json + "\n\n").getBytes(StandardCharsets.UTF_8);
      synchronized (os) {
        os.write(data);
        os.flush();
      }
      return true;
    } catch (IOException e) {
      try {
        os.close();
      } catch (IOException ignored) {
      }
      return false;
    }
  }

  private synchronized void handleLiveStart(HttpExchange ex) throws IOException {
    if (activeTranscriber != null) {
      sendJson(ex, 409, "{\"ok\":false,\"error\":\"当前已有正在运行的实时转写任务\"}");
      return;
    }
    JsonNode body = JSON.readTree(ex.getRequestBody());
    String project = body.path("project").asText("cloud-llm-preview1");
    String audioFile = Path.of(body.path("audio").asText("JamesCameron.wav")).getFileName().toString();
    int startSec = Math.max(0, body.path("startSec").asInt(460));
    int durationSec = Math.max(5, body.path("durationSec").asInt(45));
    long rotateAfterSec = Math.max(15, Math.min(540, body.path("rotateAfterSec").asLong(480)));
    long flushAfterSec = Math.max(0, body.path("flushAfterSec").asLong(24));
    boolean officialFirst = body.path("officialFirst").asBoolean(true);
    int silenceRms = Math.max(0, body.path("silenceRms").asInt(300));
    boolean adaptiveSilence = body.path("adaptiveSilence").asBoolean(true);
    int silenceDurationMs = body.path("silenceDurationMs").asInt(500);
    int prefixPaddingMs = body.path("prefixPaddingMs").asInt(-1);
    LiveTranscriptionConfig.StartSensitivity startSensitivity =
        LiveTranscriptionConfig.StartSensitivity.parse(
            body.path("startOfSpeechSensitivity").asText("DEFAULT"));
    LiveTranscriptionConfig.EndSensitivity endSensitivity =
        LiveTranscriptionConfig.EndSensitivity.parse(
            body.path("endOfSpeechSensitivity").asText("DEFAULT"));

    Path wavPath = Path.of(audioFile);
    if (!Files.isRegularFile(wavPath)) {
      sendJson(ex, 400, "{\"ok\":false,\"error\":\"找不到音频文件: " + audioFile + "\"}");
      return;
    }

    stopRequested = false;
    liveRunnerThread =
        new Thread(
            () ->
                runLiveSession(
                    project,
                    wavPath,
                    startSec,
                    durationSec,
                    rotateAfterSec,
                    flushAfterSec,
                    officialFirst,
                    silenceRms,
                    adaptiveSilence,
                    silenceDurationMs,
                    prefixPaddingMs,
                    startSensitivity,
                    endSensitivity),
            "workbench-live-runner");
    liveRunnerThread.setDaemon(true);
    liveRunnerThread.start();

    sendJson(ex, 200, "{\"ok\":true}");
  }

  private void handleLiveAction(HttpExchange ex) throws IOException {
    JsonNode body = JSON.readTree(ex.getRequestBody());
    String action = body.path("action").asText("");
    RotatingLiveTranscriber t = activeTranscriber;
    if (t == null) {
      sendJson(ex, 200, "{\"ok\":false,\"error\":\"当前无运行中的转写实例\"}");
      return;
    }
    switch (action) {
      case "flush" -> t.forceSoftFlushForTest();
      case "rotate" -> t.forceRotateForTest();
      case "abort" -> t.abortCurrentSessionForTest();
      case "stop" -> stopRequested = true;
      default -> {}
    }
    sendJson(ex, 200, "{\"ok\":true}");
  }

  private void runLiveSession(
      String project,
      Path wavPath,
      int startSec,
      int durationSec,
      long rotateAfterSec,
      long flushAfterSec,
      boolean officialFirst,
      int silenceRms,
      boolean adaptiveSilence,
      int silenceDurationMs,
      int prefixPaddingMs,
      LiveTranscriptionConfig.StartSensitivity startSensitivity,
      LiveTranscriptionConfig.EndSensitivity endSensitivity) {
    final long[] lastBoundaryMs = {0L};
    try {
      byte[] allPcm;
      try (AudioInputStream in = AudioSystem.getAudioInputStream(wavPath.toFile())) {
        allPcm = in.readAllBytes();
      }
      int bytesPerSec = LiveTranscriptionConfig.SAMPLE_RATE * 2;
      int startByte = Math.min(allPcm.length, startSec * bytesPerSec);
      int endByte = Math.min(allPcm.length, startByte + durationSec * bytesPerSec);
      byte[] slice = Arrays.copyOfRange(allPcm, startByte, endByte);

      LiveTranscriptionConfig.Builder cfgBuilder =
          LiveTranscriptionConfig.builder()
              .project(project)
              .location("global");
      if (officialFirst) {
        cfgBuilder.methodCOfficialFirst();
      } else {
        cfgBuilder.legacyMethodB();
      }
      LiveTranscriptionConfig cfg =
          cfgBuilder
              .rotateAfterMs(rotateAfterSec * 1000L)
              .flushAfterMs(flushAfterSec * 1000L)
              .silenceRms(silenceRms)
              .adaptiveSilence(adaptiveSilence)
              .silenceDurationMs(silenceDurationMs)
              .prefixPaddingMs(prefixPaddingMs)
              .startOfSpeechSensitivity(startSensitivity)
              .endOfSpeechSensitivity(endSensitivity)
              .build();

      TranscriptListener listener =
          new TranscriptListener() {
            @Override
            public void onInterim(TranscriptEvent ev) {
              ObjectNode n = JSON.createObjectNode();
              n.put("kind", "interim");
              n.put("session", ev.sessionIndex);
              n.put("text", ev.text);
              n.put("startMs", ev.startMs);
              n.put("endMs", ev.endMs);
              if (ev.rawJson != null && !ev.rawJson.isBlank()) {
                n.put("rawJson", ev.rawJson);
              }
              broadcastSse(n);
            }

            @Override
            public void onFinal(TranscriptEvent ev) {
              lastBoundaryMs[0] = Math.max(lastBoundaryMs[0], ev.endMs > 0 ? ev.endMs : 0);
              ObjectNode n = JSON.createObjectNode();
              n.put("kind", "final");
              n.put("session", ev.sessionIndex);
              n.put("source", ev.source.name());
              n.put("text", ev.text);
              n.put("startMs", ev.startMs);
              n.put("endMs", ev.endMs);
              if (ev.rawJson != null && !ev.rawJson.isBlank()) {
                n.put("rawJson", ev.rawJson);
              }
              RotatingLiveTranscriber tr = activeTranscriber;
              if (tr != null) {
                putSessions(n, tr.sessionInfos());
              }
              broadcastSse(n);
            }

            @Override
            public void onSessionEvent(String message) {
              ObjectNode n = JSON.createObjectNode();
              n.put("kind", "session_event");
              n.put("message", message);
              RotatingLiveTranscriber tr = activeTranscriber;
              if (tr != null) {
                putSessions(n, tr.sessionInfos());
              }
              broadcastSse(n);
            }

            @Override
            public void onError(Throwable error) {
              ObjectNode n = JSON.createObjectNode();
              n.put("kind", "session_event");
              n.put("message", "ERROR: " + error);
              broadcastSse(n);
            }
          };

      ObjectNode startMsg = JSON.createObjectNode();
      startMsg.put("kind", "status");
      startMsg.put("running", true);
      startMsg.put(
          "message",
          String.format(
              Locale.ROOT,
              "启动实时推流: %s [%ds ~ %ds] (rotateAfter=%ds, flushAfter=%ds, silenceDurationMs=%d, SOS=%s, EOS=%s)",
              wavPath.getFileName(),
              startSec,
              startSec + (slice.length / bytesPerSec),
              rotateAfterSec,
              flushAfterSec,
              cfg.silenceDurationMs,
              cfg.startOfSpeechSensitivity,
              cfg.endOfSpeechSensitivity));
      broadcastSse(startMsg);

      int[] levelRing = new int[100];
      int levelCount = 0;
      int silentFrames = 0;

      try (RotatingLiveTranscriber t = new RotatingLiveTranscriber(cfg, listener)) {
        activeTranscriber = t;
        t.start();

        int frameBytes = 3200; // 100ms
        long t0 = System.nanoTime();
        for (int i = 0; i * frameBytes < slice.length && !stopRequested; i++) {
          long wait = t0 + i * 100_000_000L - System.nanoTime();
          if (wait > 0) {
            Thread.sleep(wait / 1_000_000L, (int) (wait % 1_000_000L));
          }
          int len = Math.min(frameBytes, slice.length - i * frameBytes);
          byte[] chunk = Arrays.copyOfRange(slice, i * frameBytes, i * frameBytes + len);
          t.feed(chunk);

          // 计算实时声学遥测（与 RotatingLiveTranscriber 内部公式完全一致）
          int rms = computeRms(chunk);
          levelRing[levelCount % levelRing.length] = rms;
          levelCount++;
          int n = Math.min(levelCount, levelRing.length);
          int[] sorted = Arrays.copyOf(levelRing, n);
          Arrays.sort(sorted);
          int p10 = sorted[(int) (0.10 * (n - 1))];
          int p80 = sorted[(int) (0.80 * (n - 1))];
          int threshold =
              adaptiveSilence
                  ? Math.max(silenceRms, p10 + (int) (0.15 * Math.max(0, p80 - p10)))
                  : silenceRms;
          silentFrames = rms < threshold ? silentFrames + 1 : 0;

          if (i % 2 == 0) {
            long streamMs = (i + 1) * 100L;
            ObjectNode tel = JSON.createObjectNode();
            tel.put("kind", "telemetry");
            tel.put("streamSec", streamMs / 1000.0);
            tel.put("sourceAudioSec", startSec + (streamMs / 1000.0));
            tel.put("rms", rms);
            tel.put("p10", p10);
            tel.put("p80", p80);
            tel.put("threshold", threshold);
            tel.put("silentMs", silentFrames * 100);
            tel.put("segSec", Math.max(0, (streamMs - lastBoundaryMs[0]) / 1000.0));
            tel.put("replaySec", Math.min(60.0, streamMs / 1000.0));
            putSessions(tel, t.sessionInfos());
            broadcastSse(tel);
          }
        }
        t.finish();
        ObjectNode doneNode = JSON.createObjectNode();
        doneNode.put("kind", "session_event");
        doneNode.put("message", "实时流推流与排空收尾全部完成 (finish)");
        putSessions(doneNode, t.sessionInfos());
        broadcastSse(doneNode);
      }
    } catch (Exception e) {
      ObjectNode err = JSON.createObjectNode();
      err.put("kind", "session_event");
      err.put("message", "运行异常: " + e.getMessage());
      broadcastSse(err);
    } finally {
      activeTranscriber = null;
      ObjectNode endStatus = JSON.createObjectNode();
      endStatus.put("kind", "status");
      endStatus.put("running", false);
      broadcastSse(endStatus);
    }
  }

  private static void putSessions(ObjectNode parent, List<RotatingLiveTranscriber.SessionInfo> list) {
    ArrayNode arr = parent.putArray("sessions");
    for (RotatingLiveTranscriber.SessionInfo s : list) {
      ObjectNode n = arr.addObject();
      n.put("index", s.index);
      n.put("audioStartMs", s.audioStartMs);
      n.put("audioEndMs", s.audioEndMs);
      n.put("state", s.state);
      n.put("endReason", s.endReason);
      n.put("goAway", s.goAway);
      n.put("finals", s.finals);
      n.put("softFlushes", s.softFlushes);
      n.put("interimFallbacks", s.interimFallbacks);
      n.put("emittedText", s.emittedText);
    }
  }

  private static int computeRms(byte[] pcm) {
    ByteBuffer bb = ByteBuffer.wrap(pcm).order(ByteOrder.LITTLE_ENDIAN);
    long sum = 0;
    int n = pcm.length / 2;
    for (int i = 0; i < n; i++) {
      int v = bb.getShort();
      sum += (long) v * v;
    }
    return (int) Math.sqrt((double) sum / Math.max(1, n));
  }

  private static int countWords(String text) {
    if (text == null || text.isBlank()) {
      return 0;
    }
    return text.trim().split("\\s+").length;
  }

  // ===============================================================================================
  // 5. 三方法同源实时对决台 (Method A: 官方 ADK vs Method B: 传统8s双会话 vs Method C: 官方优先+MBB兜底)
  // ===============================================================================================

  private record CompareRequest(
      String presetId,
      String project,
      String audioFile,
      int startSec,
      int durationSec,
      long rotateAfterSec,
      long flushAfterSec,
      long hybFlushAfterSec,
      long adkMaxReplaySec,
      int silenceDurationMs,
      int prefixPaddingMs,
      LiveTranscriptionConfig.StartSensitivity startSensitivity,
      LiveTranscriptionConfig.EndSensitivity endSensitivity,
      int autoKillAtSec) {}

  private synchronized void handleCompareStart(HttpExchange ex) throws IOException {
    if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
      sendJson(ex, 405, "{\"ok\":false,\"error\":\"POST required\"}");
      return;
    }
    if (activeCompareAdk != null || activeCompareMbb != null || activeCompareHyb != null) {
      sendJson(ex, 409, "{\"ok\":false,\"error\":\"当前已有正在运行的三路同源对决任务\"}");
      return;
    }
    JsonNode body = JSON.readTree(ex.getRequestBody());
    CompareRequest req =
        new CompareRequest(
            body.path("presetId").asText("custom").replaceAll("[^a-zA-Z0-9_\\-]", ""),
            body.path("project").asText("cloud-llm-preview1"),
            Path.of(body.path("audio").asText("JamesCameron.wav")).getFileName().toString(),
            Math.max(0, body.path("startSec").asInt(450)),
            Math.max(8, body.path("durationSec").asInt(42)),
            Math.max(15, Math.min(540, body.path("rotateAfterSec").asLong(20))),
            Math.max(0, body.path("flushAfterSec").asLong(8)),
            Math.max(0, body.path("hybFlushAfterSec").asLong(24)),
            Math.max(2, Math.min(60, body.path("adkMaxReplaySec").asLong(10))),
            body.path("silenceDurationMs").asInt(500),
            body.path("prefixPaddingMs").asInt(-1),
            LiveTranscriptionConfig.StartSensitivity.parse(
                body.path("startOfSpeechSensitivity").asText("DEFAULT")),
            LiveTranscriptionConfig.EndSensitivity.parse(
                body.path("endOfSpeechSensitivity").asText("DEFAULT")),
            Math.max(0, body.path("autoKillAtSec").asInt(0)));

    Path wavPath = Path.of(req.audioFile());
    if (!Files.isRegularFile(wavPath)) {
      sendJson(ex, 400, "{\"ok\":false,\"error\":\"找不到音频文件: " + req.audioFile() + "\"}");
      return;
    }

    compareStopRequested = false;
    compareRunnerThread =
        new Thread(() -> runCompareArena(req, wavPath), "workbench-compare-arena");
    compareRunnerThread.setDaemon(true);
    compareRunnerThread.start();
    sendJson(ex, 200, "{\"ok\":true}");
  }

  private void handleCompareAction(HttpExchange ex) throws IOException {
    if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
      sendJson(ex, 405, "{\"ok\":false,\"error\":\"POST required\"}");
      return;
    }
    JsonNode body = JSON.readTree(ex.getRequestBody());
    String action = body.path("action").asText("");
    AdkReferenceLiveTranscriber adk = activeCompareAdk;
    RotatingLiveTranscriber mbb = activeCompareMbb;
    RotatingLiveTranscriber hyb = activeCompareHyb;
    if ("stop".equals(action)) {
      compareStopRequested = true;
      sendJson(ex, 200, "{\"ok\":true}");
      return;
    }
    if (adk == null && mbb == null && hyb == null) {
      sendJson(ex, 200, "{\"ok\":false,\"error\":\"当前无运行中的三路对决实例\"}");
      return;
    }
    switch (action) {
      case "rotate" -> {
        if (adk != null) adk.forceRotateForTest();
        if (mbb != null) mbb.forceRotateForTest();
        if (hyb != null) hyb.forceRotateForTest();
      }
      case "abort" -> {
        if (adk != null) adk.abortCurrentSessionForTest();
        if (mbb != null) mbb.abortCurrentSessionForTest();
        if (hyb != null) hyb.abortCurrentSessionForTest();
      }
      case "flush_mbb" -> {
        if (mbb != null) mbb.forceSoftFlushForTest();
        if (hyb != null) hyb.forceSoftFlushForTest();
      }
      default -> {}
    }
    sendJson(ex, 200, "{\"ok\":true}");
  }

  private void handleCompareSnapshot(HttpExchange ex) throws IOException {
    Map<String, String> q = parseQuery(ex.getRequestURI());
    String name =
        q.getOrDefault("name", "scene1_rotation").replaceAll("[^a-zA-Z0-9_\\-]", "");
    Path snapFile = LOGS_DIR.resolve("compare_" + name + ".json");
    if (!Files.isRegularFile(snapFile)) {
      sendJson(ex, 404, "{\"ok\":false,\"error\":\"尚未生成快照: " + name + "\"}");
      return;
    }
    sendJson(ex, 200, Files.readString(snapFile, StandardCharsets.UTF_8));
  }

  public void generatePresetSnapshotBlocking(String preset) throws IOException {
    CompareRequest req =
        switch (preset) {
          case "scene0_normal_talk" ->
              new CompareRequest(
                  "scene0_normal_talk",
                  "cloud-llm-preview1",
                  "JamesCameron.wav",
                  0,
                  40,
                  20,
                  8,
                  24,
                  10,
                  500,
                  -1,
                  LiveTranscriptionConfig.StartSensitivity.DEFAULT,
                  LiveTranscriptionConfig.EndSensitivity.DEFAULT,
                  0);
          case "scene2_long_drop" ->
              new CompareRequest(
                  "scene2_long_drop",
                  "cloud-llm-preview1",
                  "JamesCameron.wav",
                  60,
                  35,
                  480,
                  8,
                  24,
                  10,
                  900,
                  300,
                  LiveTranscriptionConfig.StartSensitivity.LOW,
                  LiveTranscriptionConfig.EndSensitivity.LOW,
                  22);
          default ->
              new CompareRequest(
                  "scene1_rotation",
                  "cloud-llm-preview1",
                  "JamesCameron.wav",
                  450,
                  42,
                  20,
                  8,
                  24,
                  10,
                  500,
                  -1,
                  LiveTranscriptionConfig.StartSensitivity.DEFAULT,
                  LiveTranscriptionConfig.EndSensitivity.DEFAULT,
                  0);
        };
    runCompareArena(req, Path.of(req.audioFile()));
  }

  private void runCompareArena(CompareRequest req, Path wavPath) {
    ObjectNode snapshot = JSON.createObjectNode();
    snapshot.put("ok", true);
    snapshot.put("presetId", req.presetId());
    ObjectNode paramsNode = snapshot.putObject("params");
    paramsNode.put("audio", req.audioFile());
    paramsNode.put("startSec", req.startSec());
    paramsNode.put("durationSec", req.durationSec());
    paramsNode.put("rotateAfterSec", req.rotateAfterSec());
    paramsNode.put("flushAfterSec", req.flushAfterSec());
    paramsNode.put("hybFlushAfterSec", req.hybFlushAfterSec());
    paramsNode.put("adkMaxReplaySec", req.adkMaxReplaySec());
    paramsNode.put("silenceDurationMs", req.silenceDurationMs());
    paramsNode.put("prefixPaddingMs", req.prefixPaddingMs());
    paramsNode.put("startOfSpeechSensitivity", req.startSensitivity().name());
    paramsNode.put("endOfSpeechSensitivity", req.endSensitivity().name());
    paramsNode.put("autoKillAtSec", req.autoKillAtSec());

    ArrayNode adkFinalsArr = snapshot.putArray("adkFinals");
    ArrayNode mbbFinalsArr = snapshot.putArray("mbbFinals");
    ArrayNode hybFinalsArr = snapshot.putArray("hybFinals");
    ArrayNode adkEventsArr = snapshot.putArray("adkEvents");
    ArrayNode mbbEventsArr = snapshot.putArray("mbbEvents");
    ArrayNode hybEventsArr = snapshot.putArray("hybEvents");

    final int[] adkWords = {0};
    final int[] mbbWords = {0};
    final int[] hybWords = {0};
    final int[] adkFinalCnt = {0};
    final int[] mbbFinalCnt = {0};
    final int[] hybFinalCnt = {0};
    final double[] adkLastFinalStreamSec = {0.0};
    final double[] mbbLastFinalStreamSec = {0.0};
    final double[] hybLastFinalStreamSec = {0.0};
    final double[] adkMaxUnfinalizedSec = {0.0};
    final double[] mbbMaxUnfinalizedSec = {0.0};
    final double[] hybMaxUnfinalizedSec = {0.0};
    final double[] currentStreamSec = {0.0};

    try {
      byte[] allPcm;
      try (AudioInputStream in = AudioSystem.getAudioInputStream(wavPath.toFile())) {
        allPcm = in.readAllBytes();
      }
      int bytesPerSec = LiveTranscriptionConfig.SAMPLE_RATE * 2;
      int startByte = Math.min(allPcm.length, req.startSec() * bytesPerSec);
      int endByte = Math.min(allPcm.length, startByte + req.durationSec() * bytesPerSec);
      byte[] slice = Arrays.copyOfRange(allPcm, startByte, endByte);
      double actualDurSec = slice.length / (double) bytesPerSec;
      snapshot.put("totalDurationSec", actualDurSec);

      // Lane A: 官方 Python ADK 参考模式（无客户端 RMS 软刷 flushAfterMs=0，单连接串行轮换，10s 重放上限）
      LiveTranscriptionConfig adkCfg =
          LiveTranscriptionConfig.builder()
              .project(req.project())
              .location("global")
              .rotateAfterMs(req.rotateAfterSec() * 1000L)
              .flushAfterMs(0L)
              .silenceDurationMs(req.silenceDurationMs())
              .prefixPaddingMs(req.prefixPaddingMs())
              .startOfSpeechSensitivity(req.startSensitivity())
              .endOfSpeechSensitivity(req.endSensitivity())
              .build();

      // Lane B: 传统双会话模式 (Legacy Method B: 8s 激进客户端 RMS 软刷 + 声学停顿即切 + 60s 环形重放)
      LiveTranscriptionConfig mbbCfg =
          LiveTranscriptionConfig.builder()
              .project(req.project())
              .location("global")
              .legacyMethodB()
              .rotateAfterMs(req.rotateAfterSec() * 1000L)
              .flushAfterMs(req.flushAfterSec() * 1000L)
              .maxReplayMs(60_000L)
              .silenceDurationMs(req.silenceDurationMs())
              .prefixPaddingMs(req.prefixPaddingMs())
              .startOfSpeechSensitivity(req.startSensitivity())
              .endOfSpeechSensitivity(req.endSensitivity())
              .build();

      // Lane C: ✨ 融合方案 Method C（官方优先 + MBB 兜底：原生 SDK Types + 优先等 !pending 句末切流 + 20s 安全阀 + 60s 重放）
      LiveTranscriptionConfig hybCfg =
          LiveTranscriptionConfig.builder()
              .project(req.project())
              .location("global")
              .methodCOfficialFirst()
              .rotateAfterMs(req.rotateAfterSec() * 1000L)
              .flushAfterMs(req.hybFlushAfterSec() * 1000L)
              .preferServerCutoverMs(12_000L)
              .maxReplayMs(60_000L)
              .silenceDurationMs(req.silenceDurationMs())
              .prefixPaddingMs(req.prefixPaddingMs())
              .startOfSpeechSensitivity(req.startSensitivity())
              .endOfSpeechSensitivity(req.endSensitivity())
              .build();

      TranscriptListener adkListener =
          createCompareLaneListener(
              "adk",
              snapshot,
              adkFinalsArr,
              adkEventsArr,
              adkWords,
              adkFinalCnt,
              adkLastFinalStreamSec,
              adkMaxUnfinalizedSec,
              currentStreamSec);

      TranscriptListener mbbListener =
          createCompareLaneListener(
              "mbb",
              snapshot,
              mbbFinalsArr,
              mbbEventsArr,
              mbbWords,
              mbbFinalCnt,
              mbbLastFinalStreamSec,
              mbbMaxUnfinalizedSec,
              currentStreamSec);

      TranscriptListener hybListener =
          createCompareLaneListener(
              "hyb",
              snapshot,
              hybFinalsArr,
              hybEventsArr,
              hybWords,
              hybFinalCnt,
              hybLastFinalStreamSec,
              hybMaxUnfinalizedSec,
              currentStreamSec);

      ObjectNode startEvt = JSON.createObjectNode();
      startEvt.put("kind", "compare");
      startEvt.put("subkind", "status");
      startEvt.put("running", true);
      startEvt.put("totalDurationSec", actualDurSec);
      startEvt.put(
          "message",
          String.format(
              Locale.ROOT,
              "已启动同源三路对决: %s [%ds ~ %ds] · rotate=%ds · A(官方ADK %ds窗) vs B(8s软刷双会话) vs C(官方优先+%ds安全阀+MBB)%s",
              req.audioFile(),
              req.startSec(),
              req.startSec() + (int) actualDurSec,
              req.rotateAfterSec(),
              req.adkMaxReplaySec(),
              req.hybFlushAfterSec(),
              req.autoKillAtSec() > 0
                  ? String.format(Locale.ROOT, " · 将在 %.0fs 自动同步注入断线", (double) req.autoKillAtSec())
                  : ""));
      broadcastSse(startEvt);

      try (AdkReferenceLiveTranscriber adk =
              new AdkReferenceLiveTranscriber(adkCfg, req.adkMaxReplaySec() * 1000L, adkListener);
          RotatingLiveTranscriber mbb = new RotatingLiveTranscriber(mbbCfg, mbbListener);
          RotatingLiveTranscriber hyb = new RotatingLiveTranscriber(hybCfg, hybListener)) {
        activeCompareAdk = adk;
        activeCompareMbb = mbb;
        activeCompareHyb = hyb;
        adk.start();
        mbb.start();
        hyb.start();

        int frameBytes = 3200; // 100ms
        boolean autoKilled = false;
        long t0 = System.nanoTime();

        for (int i = 0; i * frameBytes < slice.length && !compareStopRequested; i++) {
          long wait = t0 + i * 100_000_000L - System.nanoTime();
          if (wait > 0) {
            Thread.sleep(wait / 1_000_000L, (int) (wait % 1_000_000L));
          }
          int len = Math.min(frameBytes, slice.length - i * frameBytes);
          byte[] chunk = Arrays.copyOfRange(slice, i * frameBytes, i * frameBytes + len);

          long streamMs = (i + 1) * 100L;
          double streamSec = streamMs / 1000.0;
          currentStreamSec[0] = streamSec;

          // 同一毫秒向 A / B / C 三路喂入完全相同的 100ms PCM 帧
          adk.feed(chunk);
          mbb.feed(chunk);
          hyb.feed(chunk);

          if (!autoKilled && req.autoKillAtSec() > 0 && streamSec >= req.autoKillAtSec()) {
            autoKilled = true;
            adk.abortCurrentSessionForTest();
            mbb.abortCurrentSessionForTest();
            hyb.abortCurrentSessionForTest();
          }

          if (i % 2 == 0) {
            synchronized (snapshot) {
              adkMaxUnfinalizedSec[0] =
                  Math.max(adkMaxUnfinalizedSec[0], streamSec - adkLastFinalStreamSec[0]);
              mbbMaxUnfinalizedSec[0] =
                  Math.max(mbbMaxUnfinalizedSec[0], streamSec - mbbLastFinalStreamSec[0]);
              hybMaxUnfinalizedSec[0] =
                  Math.max(hybMaxUnfinalizedSec[0], streamSec - hybLastFinalStreamSec[0]);
            }
            ObjectNode tel =
                buildCompareTelemetryNode(
                    streamSec,
                    actualDurSec,
                    req.startSec() + streamSec,
                    computeRms(chunk),
                    adk,
                    mbb,
                    hyb,
                    adkWords[0],
                    mbbWords[0],
                    hybWords[0],
                    adkFinalCnt[0],
                    mbbFinalCnt[0],
                    hybFinalCnt[0],
                    adkMaxUnfinalizedSec[0],
                    mbbMaxUnfinalizedSec[0],
                    hybMaxUnfinalizedSec[0]);
            broadcastSse(tel);
          }
        }

        Thread finishAdk =
            new Thread(
                () -> {
                  try {
                    adk.finish();
                  } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                  }
                },
                "finish-adk");
        Thread finishMbb =
            new Thread(
                () -> {
                  try {
                    mbb.finish();
                  } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                  }
                },
                "finish-mbb");
        Thread finishHyb =
            new Thread(
                () -> {
                  try {
                    hyb.finish();
                  } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                  }
                },
                "finish-hyb");
        finishAdk.start();
        finishMbb.start();
        finishHyb.start();
        finishAdk.join();
        finishMbb.join();
        finishHyb.join();

        ObjectNode finalSummary =
            buildCompareTelemetryNode(
                actualDurSec,
                actualDurSec,
                req.startSec() + actualDurSec,
                0,
                adk,
                mbb,
                hyb,
                adkWords[0],
                mbbWords[0],
                hybWords[0],
                adkFinalCnt[0],
                mbbFinalCnt[0],
                hybFinalCnt[0],
                adkMaxUnfinalizedSec[0],
                mbbMaxUnfinalizedSec[0],
                hybMaxUnfinalizedSec[0]);
        finalSummary.put("subkind", "done");
        broadcastSse(finalSummary);

        synchronized (snapshot) {
          snapshot.set("summary", finalSummary);
          Files.createDirectories(LOGS_DIR);
          Path latestFile = LOGS_DIR.resolve("compare_latest.json");
          Files.writeString(
              latestFile,
              JSON.writerWithDefaultPrettyPrinter().writeValueAsString(snapshot),
              StandardCharsets.UTF_8);
          if (!req.presetId().isBlank() && !"custom".equals(req.presetId())) {
            Path presetFile = LOGS_DIR.resolve("compare_" + req.presetId() + ".json");
            Files.writeString(
                presetFile,
                JSON.writerWithDefaultPrettyPrinter().writeValueAsString(snapshot),
                StandardCharsets.UTF_8);
          }
        }
      }
    } catch (Exception e) {
      ObjectNode err = JSON.createObjectNode();
      err.put("kind", "compare");
      err.put("subkind", "session_event");
      err.put("lane", "all");
      err.put("message", "对决运行异常: " + e.getMessage());
      broadcastSse(err);
    } finally {
      activeCompareAdk = null;
      activeCompareMbb = null;
      activeCompareHyb = null;
      ObjectNode endEvt = JSON.createObjectNode();
      endEvt.put("kind", "compare");
      endEvt.put("subkind", "status");
      endEvt.put("running", false);
      broadcastSse(endEvt);
    }
  }

  private TranscriptListener createCompareLaneListener(
      String lane,
      ObjectNode snapshotLock,
      ArrayNode finalsArr,
      ArrayNode eventsArr,
      int[] wordsOut,
      int[] finalsOut,
      double[] lastFinalStreamSec,
      double[] maxUnfinalizedSec,
      double[] currentStreamSec) {
    return new TranscriptListener() {
      @Override
      public void onInterim(TranscriptEvent ev) {
        ObjectNode n = JSON.createObjectNode();
        n.put("kind", "compare");
        n.put("subkind", "interim");
        n.put("lane", lane);
        n.put("session", ev.sessionIndex);
        n.put("text", ev.text);
        n.put("startMs", ev.startMs);
        n.put("endMs", ev.endMs);
        broadcastSse(n);
      }

      @Override
      public void onFinal(TranscriptEvent ev) {
        synchronized (snapshotLock) {
          finalsOut[0]++;
          wordsOut[0] += countWords(ev.text);
          double nowSec = Math.max(currentStreamSec[0], ev.endMs > 0 ? ev.endMs / 1000.0 : 0.0);
          maxUnfinalizedSec[0] =
              Math.max(maxUnfinalizedSec[0], Math.max(0.0, nowSec - lastFinalStreamSec[0]));
          lastFinalStreamSec[0] = nowSec;

          ObjectNode fObj = finalsArr.addObject();
          fObj.put("session", ev.sessionIndex);
          fObj.put("source", ev.source.name());
          fObj.put("text", ev.text);
          fObj.put("startMs", ev.startMs);
          fObj.put("endMs", ev.endMs);
        }
        ObjectNode n = JSON.createObjectNode();
        n.put("kind", "compare");
        n.put("subkind", "final");
        n.put("lane", lane);
        n.put("session", ev.sessionIndex);
        n.put("source", ev.source.name());
        n.put("text", ev.text);
        n.put("startMs", ev.startMs);
        n.put("endMs", ev.endMs);
        broadcastSse(n);
      }

      @Override
      public void onSessionEvent(String message) {
        synchronized (snapshotLock) {
          eventsArr.add(message);
        }
        ObjectNode n = JSON.createObjectNode();
        n.put("kind", "compare");
        n.put("subkind", "session_event");
        n.put("lane", lane);
        n.put("message", message);
        broadcastSse(n);
      }

      @Override
      public void onError(Throwable error) {
        onSessionEvent("ERROR: " + error.getMessage());
      }
    };
  }

  private static ObjectNode buildCompareTelemetryNode(
      double streamSec,
      double totalDurationSec,
      double sourceAudioSec,
      int rms,
      AdkReferenceLiveTranscriber adk,
      RotatingLiveTranscriber mbb,
      RotatingLiveTranscriber hyb,
      int adkWords,
      int mbbWords,
      int hybWords,
      int adkFinals,
      int mbbFinals,
      int hybFinals,
      double adkMaxGapSec,
      double mbbMaxGapSec,
      double hybMaxGapSec) {
    ObjectNode tel = JSON.createObjectNode();
    tel.put("kind", "compare");
    tel.put("subkind", "telemetry");
    tel.put("streamSec", streamSec);
    tel.put("totalDurationSec", totalDurationSec);
    tel.put("sourceAudioSec", sourceAudioSec);
    tel.put("rms", rms);

    List<RotatingLiveTranscriber.SessionInfo> adkSessions = adk.sessionInfos();
    List<RotatingLiveTranscriber.SessionInfo> mbbSessions = mbb.sessionInfos();
    List<RotatingLiveTranscriber.SessionInfo> hybSessions = hyb.sessionInfos();

    ObjectNode adkNode = tel.putObject("adk");
    adkNode.put("words", adkWords);
    adkNode.put("finals", adkFinals);
    adkNode.put("maxUnfinalizedSec", adkMaxGapSec);
    adkNode.put("handshakeGapMs", adk.totalHandshakeGapMs());
    adkNode.put("droppedAudioSec", adk.droppedAudioMs() / 1000.0);
    adkNode.put("bufferedSec", adk.bufferedUnfinalizedMs() / 1000.0);
    adkNode.put("waitingUtteranceEnd", adk.isWaitingUtteranceEndForRotate());
    putSessions(adkNode, adkSessions);

    populateRotatingLaneNode(tel.putObject("mbb"), mbb, mbbSessions, mbbWords, mbbFinals, mbbMaxGapSec);
    populateRotatingLaneNode(tel.putObject("hyb"), hyb, hybSessions, hybWords, hybFinals, hybMaxGapSec);

    return tel;
  }

  private static void populateRotatingLaneNode(
      ObjectNode node,
      RotatingLiveTranscriber tr,
      List<RotatingLiveTranscriber.SessionInfo> sessions,
      int words,
      int finals,
      double maxGapSec) {
    int totalSoftFlushes = 0;
    int totalFallbacks = 0;
    for (RotatingLiveTranscriber.SessionInfo s : sessions) {
      totalSoftFlushes += s.softFlushes;
      totalFallbacks += s.interimFallbacks;
    }
    node.put("words", words);
    node.put("finals", finals);
    node.put("serverFinals", tr.serverNaturalFinalsCount());
    node.put("maxUnfinalizedSec", maxGapSec);
    node.put("softFlushes", totalSoftFlushes);
    node.put("officialCleanCuts", tr.officialCleanCutCount());
    node.put("fallbackCuts", tr.fallbackCutCount());
    node.put("fallbacks", totalFallbacks);
    node.put("bufferedSec", tr.bufferedReplayMs() / 1000.0);
    putSessions(node, sessions);
  }

  private static Map<String, String> parseQuery(URI uri) {
    Map<String, String> out = new HashMap<>();
    String raw = uri.getRawQuery();
    if (raw == null || raw.isBlank()) {
      return out;
    }
    for (String pair : raw.split("&")) {
      int idx = pair.indexOf('=');
      if (idx > 0) {
        out.put(
            URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8),
            URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8));
      }
    }
    return out;
  }

  private void handleVadLabRun(HttpExchange ex) throws IOException {
    if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
      sendJson(ex, 405, "{\"ok\":false,\"error\":\"POST required\"}");
      return;
    }
    JsonNode body = JSON.readTree(ex.getRequestBody());
    String scenario = body.path("scenario").asText("sol_c_manual");
    String project = body.path("project").asText("cloud-llm-preview1");
    String setupJson = body.path("setupJson").asText("");
    if ("stop".equals(scenario)) {
      vadLab.stopScenario();
    } else {
      vadLab.startScenario(scenario, project, setupJson);
    }
    sendJson(ex, 200, "{\"ok\":true}");
  }

  private void handleVadLabStep(HttpExchange ex) throws IOException {
    if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
      sendJson(ex, 405, "{\"ok\":false,\"error\":\"POST required\"}");
      return;
    }
    JsonNode body = JSON.readTree(ex.getRequestBody());
    String action = body.path("action").asText("");
    String project = body.path("project").asText("cloud-llm-preview1");
    String setupJson = body.path("setupJson").asText("");
    vadLab.handleStepAction(action, project, setupJson);
    sendJson(ex, 200, "{\"ok\":true}");
  }

  private static void sendJson(HttpExchange ex, int status, String json) throws IOException {
    byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
    ex.getResponseHeaders().set("Cache-Control", "no-store");
    ex.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }
}
