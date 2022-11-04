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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.jms.Message;
import javax.jms.MessageListener;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleMessageListener implements MessageListener {
  final List<Message> receivedMessages = new CopyOnWriteArrayList<>();

  @Override
  public void onMessage(Message message) {
    receivedMessages.add(message);
    // log.info("{} - received {}, total {}", this, message, receivedMessages.size());
  }
}
