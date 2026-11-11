# `gemini-3.5-transcribe-live` 10 分钟限制、官方 Python 参考实现与生产级增强方案

**核心结论速览**：
1. **单连接 10 分钟硬限制未取消**：`gemini-3.5-transcribe-live` 单条 WebSocket 会话最长运行 **10 分钟**（约 `564s` 下发 `GoAway`，约 `594s` 强制关闭）。官方 GA 推出的“连续长时流式转写”本质是通过 **客户端流重启（Stream-Restarting）** 跨越会话边界。
2. **官方 Python 参考代码存在串行盲区与长句丢词短板**：官方示例采用 **“先断旧流 → 串行重连新流 → 回放未结音频块”** 的单连接模型，实测存在 **`503ms~1,070ms` 握手盲区**、紧凑快语速 **`pending=True` 死锁阻塞**，以及默认 **`10s` 缓冲上限导致长句闪断丢前文** 等问题。
3. **生产推荐采用双会话热备增强方案（Make-Before-Break）**：保留官方服务端原生 VAD 断句与 `500ms` 静音前导设计，结合 **后台并行预热建连（`0ms` 盲区）+ `24s` 长独白安全阀 + `60s` 环形缓冲词级消重**，实现数小时无损转写。

> 📦 **生产级参考代码包（Java 独立 Maven 工程，含源码、离线自检与演示 CLI）**：
> 👉 **[点击下载 `gemini-3.5-transcribe-live-mbb-java.zip`（Google Drive）](https://drive.google.com/file/d/1QF-ns21fJ0dMmhy9oIIt5ga9s2mhmzHy/view?usp=drivesdk&resourcekey=0-i7y8ZS5Fe705OTUC7AdPLA)**

---

## 1. 单会话 10 分钟硬限制与官方标注

### 1.1 官方四重权威来源对照

`gemini-3.5-transcribe-live` 基于双向流式 WebSocket（`BidiGenerateContent`），服务端对单条连接强制执行 **10 分钟生命周期上限**：

| 来源类型 | 官方位置 | 官方标注摘要 | 关键说明 |
| :--- | :--- | :--- | :--- |
| **Google Cloud 产品文档** | [Gemini 3.5 Transcribe 文档](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini/3-5-transcribe) | `Max Audio Duration` — Live Streaming: **`Up to 10 minutes`** | Vertex AI 单条实时流硬上限 10 分钟（同步接口为 15 分钟）。 |
| **Google AI Studio 指南** | [Live transcription 指南](https://aistudio.google.com/docs/live-api/live-transcribe#limitations) | `Live transcription sessions support continuous streaming for up to 10 minutes.` | 开发者 API 通道同样受 10 分钟单连接限制。 |
| **官方 Python ADK 源码** | [`_gemini_stt.py` (L77–81)](https://github.com/google/adk-python/blob/main/src/google/adk/live/_gemini_stt.py#L77-L81) | `A transcription session streams for at most 10 minutes, and the server sends GoAway a minute before it closes one.` | 服务端关闭前提前下发 `GoAway`，需由客户端主动轮换连接。 |
| **Cloud AI GA 发布说明** | Gemini 3.5 Transcribe GA (2026-10-22) | `Supports uninterrupted, long-duration audio sessions via new client-side stream-restarting reference patterns (Python SDK/samples at launch)...` | 明确跨 10 分钟连续转写依赖**客户端流重启参考模式**，同时开放可配置端点检测（VAD）。 |

### 1.2 可配置端点检测（Configurable Endpointing）参数

GA 同步在 `RealtimeInputConfig.AutomaticActivityDetection` 中开放了自动断句调优参数（详见 [Vertex AI RPC 文档](https://docs.cloud.google.com/gemini-enterprise-agent-platform/reference/rpc/google.cloud.aiplatform.v1#google.cloud.aiplatform.v1.RealtimeInputConfig.AutomaticActivityDetection)）：

| 字段名 | 默认值 / 可选档位 | 生产调优建议 |
| :--- | :--- | :--- |
| `silence_duration_ms` | 服务端默认 `880ms`；**官方 ADK 默认 `500ms`** | **最核心延迟旋钮**：判定句末所需持续静音时长。16kHz 音频建议 `500ms~800ms`；8kHz 电话音频建议 `≥700ms` 防误切。 |
| `prefix_padding_ms` | 服务端默认 `300ms` | 触发开口（SOS）前向前追溯保留的前导缓冲，切勿设为 `0` 以免吞首字。 |
| `start_of_speech_sensitivity` | `DEFAULT` / `HIGH` / `LOW` | 语音起点灵敏度（`HIGH` 更容易触发开口判定）。 |
| `end_of_speech_sensitivity` | `DEFAULT` / `HIGH` / `LOW` | 语音终点灵敏度（实测断句延迟对 `silence_duration_ms` 更敏感，优先调时长）。 |
| `disabled` | 默认 `false` | 设为 `true` 时关闭服务端自动 VAD，改由客户端手动发 `activityStart` / `activityEnd`。 |

### 1.3 原生协议层 630s 实测断连时间线

按 1:1 实时速率向单条连接持续推送 630 秒音频，服务端真实协议行为如下：

- **`+0.47s`（握手就绪）**：返回 `{"setupComplete":{}}`（**必须等收到该消息后再推音频帧**，否则首段易丢失或断连）。
- **`+2.9s ~ +563.9s`（流式转写）**：实时返回 `interimInputTranscription`，句末停顿时返回 `inputTranscription (finished=true)` 与 `generationComplete`；**全生命周期绝不返回 `turnComplete`**。
- **`+564.6s`（约 9 分 24 秒）**：服务端主动推送预警帧 `{"goAway":{"timeLeft":"30s"}}`。
- **`+594.4s`（约 9 分 54 秒）**：服务端强制关闭连接（`code=1000, reason="Session exceeded maximum allowed duration."`）。若客户端未做轮换，未转正草稿与后续音频**全部静默丢失**（630s 实测丢尾部 85 词）。

![图 1：gemini-3.5-transcribe-live 单会话 10 分钟限制与 VAD 机制](images/fig1-10min-lifecycle-and-vad.png)

*图 1：`gemini-3.5-transcribe-live` 单会话 630s 实测生命周期时间轴与 Configurable Endpointing（VAD）声学切分机制。*

---

## 2. 官方 GitHub Python 参考代码原理与局限性

Google 官方开源了两个客户端串行流重启参考实现：
1. **Sample 独立脚本**：[`GoogleCloudPlatform/generative-ai` — `gemini_3_5_transcribe_live_infinite.py`](https://github.com/GoogleCloudPlatform/generative-ai/blob/main/audio/speech/getting-started/gemini_3_5_transcribe_live_infinite.py)
2. **ADK 框架生产组件**：[`google/adk-python` — `src/google/adk/live/_gemini_stt.py` (`GeminiSTT`)](https://github.com/google/adk-python/blob/main/src/google/adk/live/_gemini_stt.py)

### 2.1 官方参考代码核心原理

两套官方代码均采用**单连接串行重启模型**（“先关闭旧连接 → 串行握手建立新连接 → 回放未定稿音频块 → 恢复实时推流”）：

- **`infinite.py`（固定周期 + 偏移量切片桥接）**：到达固定挂钟阈值后立即发 `audio_stream_end` 并断开旧流；根据最后一次 Final 的 `end_offset`（缺失时用本地挂钟估算）计算未定稿切片下标，重连后批量回放给新会话。
- **`_gemini_stt.py`（话轮感知 + 双水位队列，设计更完善）**：
  - **话轮感知轮换**：默认运行满 `480s`（8 分钟）后，等待用户停顿使 `not state.pending`（无未结草稿）才断开旧流，避免句中硬切；
  - **`_AudioStream` 双水位缓冲**：每次收到 Final 时保留末尾 `min(500ms, silence_duration_ms)` 静音帧作为下一会话的前导缓冲（防吞首音节）；未定稿期间最多缓存最近 **`10s`** 音频（`_DEFAULT_MAX_REPLAY_MS = 10_000`）；
  - **瞬态异常白名单**：仅对网络/服务端瞬态码（WS `1000/1001/1006/1011~1014`、HTTP `408/429/5xx`）自动重连。

![图 2：官方 Python 串行流重启原理与局限](images/fig2-official-serial-restart-flaws.png)

*图 2：官方 Python 串行流重启时序（先断旧流 → 503ms~1,070ms 握手盲区 → 突发回放）与核心局限剖析。*

### 2.2 官方串行方案的五大生产局限

1. **串行握手造成 `503ms ~ 1,070ms` 真实无连接盲区**：旧连接完全退出后才发起新连接 TCP/TLS/OAuth/`setupComplete` 握手；盲区积压帧与回放帧在重连瞬间突发涌入，易引起字幕卡顿跳变或触发单流令牌限流（`RESOURCE_EXHAUSTED`）。
2. **轮换切点两难（硬切碎句 vs `pending=True` 死锁）**：`infinite.py` 挂钟到点硬切会拦腰斩断句子；而 `_gemini_stt.py` 死等 `!pending` 且无会话内分段安全阀，遇到语速紧凑、换气 `<500ms` 的连续讲话时会被锁死（实测连续阻塞 **`42.0s`** 无法轮换，拖到 `GoAway` 仍被迫句中硬断）。
3. **`10s` 未定稿缓冲上限导致长句闪断必丢前文**：`_gemini_stt.py` 最多保留最近 `10s` 未定稿音频，超出直接从队首裁剪。实测在 `22s` 复合长句第 22 秒断网时，前 **`12.0s`（31 个词）永久丢失**。
4. **无跨会话重叠词消重与全局时间轴**：服务端偶发省略 `end_offset` 时估算切点易造成接缝重复出词；且每次重连后新会话时间戳归零。
5. **排空超时直接丢弃未定稿草稿**：`_flush()` 若在 `5s` 内未等到正式 Final，即使 `state.partial` 已有完整实时识别结果也会直接丢弃。

![图 3：音频重放缓冲对比](images/fig3-adk-dual-watermark-buffer.png)

*图 3：音频重放缓冲对比——官方 ADK `10s` 双水位队列 vs 增强方案 `60s` 环形缓冲与 `dropOverlap` 词级消重。*

---

## 3. 生产级双会话增强方案（Make-Before-Break 热备）

为同时兼顾 **服务端原生断句的高语义质量** 与 **长时极端工况下的零盲区、零丢词可靠性**，我们将官方 ADK 的协议精华与 **双会话先建后切（Make-Before-Break）** 架构相结合。

### 3.1 架构原理与三级切流流程（`P1 → P2 → P3`）

- **日常推流（`<24s` 零干预）**：100% 交由服务端 `Configurable Endpointing`（默认 `500ms`）自然断句；仅当连续紧凑说话满 **`24s`** 仍未落盘时，趁微停顿向**当前会话**发一次 `audioStreamEnd`（不断连，`~0.5s` 出 Final）破除 `pending` 死锁。
- **后台并行预热（`480s` 或收到 `GoAway`）**：旧会话 S1 **零中断继续收发音频**，后台异步拉起新会话 S2 完成握手进入 `READY` 待命态，将握手延迟 100% 掩盖在后台。
- **三级渐进切流**：
  1. **P1 首选（`0~12s` 窗口）**：优先等服务端自然输出句末确认（`!pending`），直接关闭 S1，向 S2 注入 `500ms` 句末静音前导后无缝接管（**零排空、零重叠、100% 原生整句**）；
  2. **P2 降级（`12~20s` 窗口）**：持续快语速无句末时，捕捉客户端自适应底噪微停顿气口切流，S1 后台优雅排空；
  3. **P3 兜底（`>20s` 或 `GoAway` 逼近）**：从 `60s` 环形缓冲回放最近 `2s` 重叠音频强制切流，输出层通过 `dropOverlap()` 自动剔除重复词。

![图 4：生产级双会话增强架构与三级切流决策树](images/fig4-method-c-mbb-architecture.png)

*图 4：生产级双会话增强架构（60s 环形缓冲 + 24s 安全阀 + 并行预热）与三级切流决策树（`P1 → P2 → P3`）。*

### 3.2 针对官方局限的五大核心改进对照

| 官方参考痛点 | 双会话增强方案改进 | 默认参数与工程实现 |
| :--- | :--- | :--- |
| 串行重连 `503ms~1,070ms` 盲区 | **Make-Before-Break 双会话热备** | `rotateAfterMs = 480s` 触发后台异步建连，S2 `setupComplete` 就绪后原子切流，**`0ms` 盲区**。 |
| 快语速 `pending=True` 死锁阻塞 | **服务端断句主导 + `24s` 软刷安全阀** | 常态零干预保留自然整句；连续未定稿满 `flushAfterMs = 24s` 趁微停顿发会话内 `audioStreamEnd` 破锁。 |
| 切流时机单一（硬切或死等） | **`P1 → P2 → P3` 三级渐进切流** | 前 `12s` 优先等服务端 `!pending` 无损切流，超时平滑降级至声学静音切流或 `2s` 重叠强切。 |
| `10s` 缓冲上限致长句断网丢词 | **`60s` 环形缓冲 + `dropOverlap` 消重** | 常驻保留 `maxReplayMs = 60s` PCM（仅占 `1.92 MB`），限速平滑回放防 `429`，支持英文连字符词级与中/日/泰无空格字符级消重。 |
| 排空超时丢草稿 & 时间戳归零 | **保序队列 + Interim 兜底 + 全局时间轴** | S1 排空完才发射 S2 结果；超时未转正草稿自动升格输出；全程换算单调递增 `[startMs, endMs]`。 |

### 3.3 Vertex AI 真实同源实测对比

在 Vertex AI `gemini-3.5-transcribe-live` 生产端点上，使用同一实时音频流（`JamesCameron.wav`）对**官方 Python 参考方案**与**双会话增强方案**实测对比如下：

| 实测场景 | 官方 Python 参考实现（`_gemini_stt.py`） | ✨ 双会话增强方案（Make-Before-Break） |
| :--- | :--- | :--- |
| **① 常规自然停顿轮换** (`0s~40s`) | `3` 段整句 · ⚠️ **`503ms` 串行握手盲区** | **`3` 段原生整句 · ✅ `0ms` 盲区无损切流（`P1 official_gap`）** |
| **② 42s 极快连读无停顿轮换** (`450s~492s`) | ❌ **`pending=True` 连续死锁阻塞 `42.0s` 无法轮换** | **✅ `0s` 阻塞 · `24s` 安全阀破锁输出 `2` 段完整长句 + `0ms` 切流** |
| **③ 22s 复合长句中途闪断** (`第 22s 断网`) | ❌ **重连盲区 `1,070ms` + 裁剪丢失前 `12.0s`（漏 31 词，找回 68.4%）** | **✅ `60s` 缓冲零裁剪，`100%` 完整找回全部 `98/98` 词** |
| **④ 16.4 分钟全长演讲** (`rotate=480s`) | 多次半秒级切流空窗与长独白阻塞风险 | **✅ 全程零丢词（WER `2.59%`）· `78.7%` 服务端原生自然整句 · 2 次 `0ms` 切流** |

![图 5：真实 Vertex AI 同源实测对比](images/fig5-vertex-ai-benchmark-comparison.png)

*图 5：真实 Vertex AI 同源实测对比——握手盲区延迟、42s 连读死锁、22s 长句断网找回率与跨 10 分钟长流完整性。*

---

### 3.4 远场长会议模式（`disabled=true` 与 10 分钟轮换的耦合设计及多语实测）

在多人会议室桌面全向麦、智能大屏等**远场高混响场景**中，服务端默认自动断句容易误判静音而吞掉每句话开头并频繁清空跨句上下文。但若要跑**超 10 分钟远场长流**，关闭自动断句与会话轮换存在强耦合：

- **官方 Python 参考代码在 `disabled=true` 下会失效丢词**：服务端不再自动产出句末 Final，`state.pending` 永恒为 `True`，导致第 8 分钟轮换被锁死拖至强断，且 `10s` 重放缓冲无法容纳数分钟未定稿音频而大段丢字。
- **双会话方案内置 `.farFieldMode()` 一键远场长流预设**（前端切勿叠加 RNNoise / DeepFilterNet 等传统重度降噪，直接送原始 PCM）：
  1. **关闭自动断句（`manualVad = true`）**：建连后立即发 `activityStart` 无门限收音，`interimInputTranscription` 全程实时流式刷新字幕；
  2. **`45s` 大窗口安全翻转**：每满 `45s` 趁微停顿发 `activityEnd` 刷出整段高精 Final，**严格等待服务端确认句末（`speech_active` 归零）后再发 `activityStart` 并从环形缓冲微回放暂存帧**（防止背靠背瞬发 `activityStart` 被服务端忽略导致静音，同时避开服务端 `220s` 单话轮硬上限并确保闪断不超 `60s` 重放窗口）；
  3. **8 分钟双连接无盲区接力 + 中/日/泰无空格字符级消重**：后台预热 S2 就绪后趁微停顿立即切换并开启新会话 `activityStart`，旧会话后台发 `activityEnd` 排空保序收尾，同时自动消除中/英/东南亚语种切流边界微回放重叠字词。

**多语种远场真实会议流式 + 跨连接轮换实测（`0616nonrealtime_test`，`90s` 实时流，`rotateAfter=45s`）**：

| 语种分组（样本数） | 官方 Python 参考实现（Auto-VAD + 串行重连） | ✨ 双会话增强 `.farFieldMode()`（关 Auto-VAD + 热备切流） | 提升幅度 |
| :--- | :---: | :---: | :---: |
| **中文普通话 `cmn-CN`** (`n=6`) | CER `48.45%` · 覆盖率 `68.9%` · 盲区 `644ms` | **CER `26.00%` · 覆盖率 `85.2%` · `0ms` 盲区** | **CER `-22.45 pt`（6/6 全胜）** |
| **英语 `en-US`** (`n=4`) | CER `39.94%` · 覆盖率 `60.2%` · 盲区 `657ms` | **CER `13.64%`（逐字 `1.35%`）· 覆盖率 `87.2%` · `0ms` 盲区** | **CER `-26.30 pt`（4/4 全胜）** |
| **印尼语 `id-ID`** (`n=4`) | CER `66.44%` · 覆盖率 `34.4%` · 盲区 `572ms` | **CER `21.46%`（逐字 `8.98%`）· 覆盖率 `81.1%` · `0ms` 盲区** | **CER `-44.98 pt`（4/4 全胜）** |
| **印地语 `hi-IN`** (`n=4`) | CER `46.02%` · 覆盖率 `55.5%` · 盲区 `630ms` | **CER `16.64%`（逐字 `7.06%`）· 覆盖率 `86.1%` · `0ms` 盲区** | **CER `-29.38 pt`（4/4 全胜）** |
| **马来语 `ms-MY`** (`n=4`) | CER `35.27%` · 覆盖率 `65.2%` · 盲区 `516ms` | **CER `7.89%`（逐字 `1.35%`）· 覆盖率 `92.6%` · `0ms` 盲区** | **CER `-27.38 pt`（4/4 全胜）** |
| **🏆 5 语种 22 条干净样本总均值** | CER `47.33%` · 覆盖率 `57.9%` · 盲区 `607ms` | **CER `17.93%`（逐字 `13.24%`）· 覆盖率 `86.3%` · `0ms` 盲区** | **错字率直降 `62%`（`22/22` 全胜）** |

*（注：另含 4 条参考文本混入中文污染的泰语 `th-TH` 样本，`.farFieldMode()` 同样将 CER 从 `71.95%` 降至 `53.90%`、恢复率 `25/26`，其中 `th-TH_3` 从官方方案 `0 字 / 100% 漏识` 恢复至 `368 字 / CER 39.8%`。）*

| 拾音场景 | 推荐配置模式 | 核心行为 |
| :--- | :--- | :--- |
| **近场 / 清晰人声**（耳机、手机、电话坐席） | **默认模式**（`builder().build()`） | 保留服务端原生自动断句 + `24s` 软刷安全阀 + `480s` 双会话热备 + `60s` 缓冲 |
| **多人远场拾音**（会议室全向麦、阵列硬件） | **远场预设**（`builder().farFieldMode().build()`） | `disabled=true` 防句首吞字 + `45s` 大窗 `activityEnd→activityStart` 安全翻转 + `480s` 双会话热备 |

---

### 3.5 参考代码包下载与快速使用指南

#### （1）Java 开箱即用代码包（推荐直接复用）

已将完整的双会话增强方案（含近场默认模式与 `.farFieldMode()` 远场长会议模式）整理为独立 Maven 工程压缩包并上传至 Google Drive，解压即可编译运行：

- **Google Drive 下载链接**：**[📦 `gemini-3.5-transcribe-live-mbb-java.zip`](https://drive.google.com/file/d/1QF-ns21fJ0dMmhy9oIIt5ga9s2mhmzHy/view?usp=drivesdk&resourcekey=0-i7y8ZS5Fe705OTUC7AdPLA)**
- **包含核心模块**：`RotatingLiveTranscriber`（双会话热备、60s 环形缓冲与远场大窗翻转引擎）、`LiveTranscriptionConfig`（参数配置与 `.farFieldMode()` 预设）、`LiveWsSession`（官方 SDK 协议序列化与瞬态错误白名单）、`RotatingLiveTranscriberDemo`（WAV 实时流、远场模式与断网注入 CLI）、`LiveSelfTest`（离线自检测试）。

**命令行快速验证**：
```bash
unzip gemini-3.5-transcribe-live-mbb-java.zip && cd gemini-3.5-transcribe-live-mbb-java
# 1. 运行离线自检（验证词级消重、Setup JSON、远场预设与瞬态错误分类）
mvn -q compile exec:java -Dexec.mainClass=com.example.speech.live.LiveSelfTest
# 2. 运行常规近场实时长音频流式转写 Demo
mvn compile exec:java -Dexec.mainClass=com.example.speech.live.RotatingLiveTranscriberDemo \
  -Dexec.args="--project=YOUR_GCP_PROJECT --audio=sample_16k.wav --name=live_demo"
# 3. 多人远场会议室长音频模式（追加 --farField）
mvn compile exec:java -Dexec.mainClass=com.example.speech.live.RotatingLiveTranscriberDemo \
  -Dexec.args="--project=YOUR_GCP_PROJECT --audio=sample_16k.wav --farField --name=farfield_demo"
```

**业务代码集成示例**：
```java
LiveTranscriptionConfig config = LiveTranscriptionConfig.builder()
    .project("your-gcp-project")
    .location("global")                  // 支持 global / us / eu
    .languageCodes(List.of("en-US"))     // 建议显式传入语种（如 en-US、cmn-Hans-CN）
    .silenceDurationMs(500)              // 近场 16kHz 推荐 500ms；8kHz 电话流推荐 700~800ms
    // 多人远场会议室拾音请启用下一行（disabled=true + 45s 大窗安全翻转；前端请直接喂原声勿叠传统降噪）：
    // .farFieldMode()
    .build();                            // 默认已开启：Make-Before-Break + 480s 轮换 + 60s 缓冲

try (RotatingLiveTranscriber transcriber = new RotatingLiveTranscriber(config, listener)) {
  transcriber.start();
  while (audioSource.hasNext()) {
    transcriber.feed(audioSource.nextPcm16kMonoChunk()); // 非阻塞入队：16kHz 16-bit 单声道 PCM
  }
  transcriber.finish();
}
```

#### （2）Python 技术栈改造要点（基于官方 `_gemini_stt.py` 升级）

若服务端使用 Python，建议以官方 `google/adk-python` 的 `_gemini_stt.py` 为基础做以下 5 点增量改造：
1. **扩大缓冲并限速回放**：将 `max_replay_ms` 从 `10_000` 调至 **`60_000`**，回放循环每发 10 帧休眠 `20~50ms` 防触发单流限流；
2. **改为后台预热建连**：满 `465s` 或收到 `go_away` 时用后台 `asyncio.Task` 提前完成新连接 `setupComplete`，在 `!state.pending`（或远场模式微停顿处）原子切换发送引用；
3. **加入会话内定期安全落盘**：近场自动 VAD 超 `24s` 未定稿时趁微停顿发 `audio_stream_end=True`；远场手动 VAD（`disabled=True`）每 `45s` 趁微停顿发 `activity_end` 等收到句末确认后再发 `activity_start` 并回放暂存帧；
4. **补齐草稿兜底与词级消重**：排空超时若 `state.partial` 非空则兜底输出；重叠切流时按最长公共前后缀剔除跨流重复词；
5. **累加全局偏移**：维护每个会话首帧对应的全局毫秒基准 `session_base_ms`，对外输出单调递增时间戳。

---

## 4. 权威参考链接速查

- **生产级双会话增强代码包（Google Drive）**：[`gemini-3.5-transcribe-live-mbb-java.zip`](https://drive.google.com/file/d/1QF-ns21fJ0dMmhy9oIIt5ga9s2mhmzHy/view?usp=drivesdk&resourcekey=0-i7y8ZS5Fe705OTUC7AdPLA)
- **Google Cloud 产品文档**：[Gemini 3.5 Transcribe 模型与限制说明](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini/3-5-transcribe)
- **Google AI Studio 指南**：[Live transcription with Gemini Live API](https://aistudio.google.com/docs/live-api/live-transcribe) · [Automatic VAD 配置指南](https://aistudio.google.com/docs/live-api/capabilities#automatic-vad-configuration)
- **Vertex AI RPC 参考**：[`RealtimeInputConfig.AutomaticActivityDetection`](https://docs.cloud.google.com/gemini-enterprise-agent-platform/reference/rpc/google.cloud.aiplatform.v1#google.cloud.aiplatform.v1.RealtimeInputConfig.AutomaticActivityDetection)
- **官方 GitHub Python 参考源码**：[`gemini_3_5_transcribe_live_infinite.py`](https://github.com/GoogleCloudPlatform/generative-ai/blob/main/audio/speech/getting-started/gemini_3_5_transcribe_live_infinite.py) · [`google/adk/live/_gemini_stt.py`](https://github.com/google/adk-python/blob/main/src/google/adk/live/_gemini_stt.py)
