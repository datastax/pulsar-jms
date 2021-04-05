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

import javax.jms.Message;
import javax.jms.MessageListener;

public class MessageListenerWrapper {
  private final MessageListener listener;
  private final PulsarSession session;

  public MessageListenerWrapper(MessageListener listener, PulsarSession session) {
    this.listener = listener;
    this.session = session;
  }

  public void onMessage(Message message) {
    Utils.executeListener(
        session,
        () -> {
          listener.onMessage(message);
        });
  }

  MessageListener getListener() {
    return listener;
  }
}
