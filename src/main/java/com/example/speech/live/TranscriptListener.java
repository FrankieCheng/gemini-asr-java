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

/**
 * 转写结果回调。回调在内部线程上执行，请勿在回调里长时间阻塞。
 */
public interface TranscriptListener {

  /** 实时中间结果（用于字幕预览）。 */
  default void onInterim(TranscriptEvent event) {}

  /** 落定结果：保证按时间顺序、跨会话去重，可直接追加到最终文本。 */
  void onFinal(TranscriptEvent event);

  /** 会话生命周期事件（建连、轮换、GoAway、断线恢复等），用于监控与日志。 */
  default void onSessionEvent(String message) {}

  /** 不可恢复的错误（例如重试多次仍无法建连）。 */
  default void onError(Throwable error) {}
}
