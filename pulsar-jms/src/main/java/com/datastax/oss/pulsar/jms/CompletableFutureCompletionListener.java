/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.jms;

import jakarta.jms.CompletionListener;
import jakarta.jms.Message;
import java.util.concurrent.CompletableFuture;

/** Utility class to convert a CompletionListener into a CompletableFuture. */
public class CompletableFutureCompletionListener extends CompletableFuture<Message>
    implements CompletionListener {

  @Override
  public void onCompletion(Message message) {
    complete(message);
  }

  @Override
  public void onException(Message message, Exception exception) {
    completeExceptionally(exception);
  }
}
