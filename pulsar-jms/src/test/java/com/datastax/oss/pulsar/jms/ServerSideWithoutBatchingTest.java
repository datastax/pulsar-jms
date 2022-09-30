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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.messages.PulsarTextMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.jms.CompletionListener;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.TextMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.junit.jupiter.api.Test;

@Slf4j
public final class ServerSideWithoutBatchingTest extends SelectorsTestsBase {
  public ServerSideWithoutBatchingTest() {
    super(true, false);
  }

  @Test
  public void competingOnJMSPriority() throws Exception {
    Map<String, Object> properties = buildProperties();
    Map<String, Object> consumerConfig = (Map<String, Object>) properties.get("consumerConfig");
    consumerConfig.put("receiverQueueSize", 64);

    if (enableBatching) {
      // ensure that we create batches with more than 1 message
      Map<String, Object> producerConfig = (Map<String, Object>) properties.get("producerConfig");
      producerConfig.put("batchingMaxPublishDelayMicros", "1000000");
      // each batch will contain 5 messages
      producerConfig.put("batchingMaxMessages", "5");
    }

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumerSlowLowPriority =
                  (PulsarMessageConsumer) session.createConsumer(destination, "JMSPriority = 4");
              PulsarMessageConsumer consumerHighPriority =
                  (PulsarMessageConsumer)
                      session.createConsumer(destination, "JMSPriority > 4"); ) {
            assertEquals(
                SubscriptionType.Shared,
                ((PulsarMessageConsumer) consumerSlowLowPriority).getSubscriptionType());
            assertEquals("JMSPriority = 4", consumerSlowLowPriority.getMessageSelector());
            assertEquals(
                SubscriptionType.Shared,
                ((PulsarMessageConsumer) consumerSlowLowPriority).getSubscriptionType());
            assertEquals("JMSPriority > 4", consumerHighPriority.getMessageSelector());

            List<CompletableFuture<Message>> handles = new ArrayList<>();
            List<String> expected1 = new CopyOnWriteArrayList<>();
            List<String> expected2 = new CopyOnWriteArrayList<>();
            int numMessages = 40_000;
            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < numMessages; i++) {
                String text = "foo-" + i;
                TextMessage textMessage = session.createTextMessage(text);
                if (i < numMessages - 100) {
                  producer.setPriority(4);
                  expected1.add(text);
                } else {
                  producer.setPriority(8);
                  expected2.add(text);
                }
                CompletableFuture<Message> handle = new CompletableFuture<>();
                producer.send(
                    textMessage,
                    new CompletionListener() {
                      @Override
                      public void onCompletion(Message message) {
                        handle.complete(message);
                      }

                      @Override
                      public void onException(Message message, Exception e) {
                        handle.completeExceptionally(e);
                      }
                    });
                handles.add(handle);

                if (handles.size() % 1000 == 0) {
                  CompletableFuture.allOf(handles.toArray(new CompletableFuture[0])).get();
                  handles.clear();
                  log.info("sent {}...", i);
                }
              }
            }

            CompletableFuture.allOf(handles.toArray(new CompletableFuture[0])).get();

            CompletableFuture<String> thread1Result = new CompletableFuture();
            Thread thread1 =
                new Thread(
                    () -> {
                      try {
                        while (!expected1.isEmpty()) {
                          log.info("{} messages left for consumer1", expected1.size());
                          PulsarTextMessage textMessage =
                              (PulsarTextMessage) consumerSlowLowPriority.receive();
                          log.info(
                              "consumerSlowLowPriority received {} {}",
                              textMessage.getText(),
                              textMessage.getJMSPriority());
                          // ensure that we receive the message only ONCE
                          assertTrue(expected1.remove(textMessage.getText()));

                          // ensure that it is a batch message
                          assertEquals(
                              enableBatching,
                              textMessage.getReceivedPulsarMessage().getMessageId()
                                  instanceof BatchMessageIdImpl);

                          Thread.sleep(10000);
                        }
                        // no more messages (this also drains some remaining messages to be skipped)
                        assertNull(consumerSlowLowPriority.receive(1000));

                        thread1Result.complete("");
                      } catch (Throwable t) {
                        log.error("error thread1", t);
                        thread1Result.completeExceptionally(t);
                      }
                    });

            CompletableFuture<String> thread2Result = new CompletableFuture();
            Thread thread2 =
                new Thread(
                    () -> {
                      try {
                        while (!expected2.isEmpty()) {
                          log.info("{} messages left for consumerHighPriority", expected2.size());
                          PulsarTextMessage textMessage =
                              (PulsarTextMessage) consumerHighPriority.receive();
                          log.info(
                              "consumerHighPriority received {} {}",
                              textMessage.getText(),
                              textMessage.getJMSPriority());
                          // ensure that we receive the message only ONCE
                          assertTrue(expected2.remove(textMessage.getText()));

                          // ensure that it is a batch message
                          assertEquals(
                              enableBatching,
                              textMessage.getReceivedPulsarMessage().getMessageId()
                                  instanceof BatchMessageIdImpl);
                        }
                        // no more messages (this also drains some remaining messages to be skipped)
                        assertNull(consumerHighPriority.receive(1000));

                        thread2Result.complete("");
                      } catch (Throwable t) {
                        log.error("error thread2", t);
                        thread2Result.completeExceptionally(t);
                      }
                    });

            thread1.start();
            thread2.start();

            // fail if we don't consume all the high priority messages in a timely fashion
            // 10 seconds seems bad, but it is only a hard limit, the test should take less than 5
            // seconds
            // but CI machines may be slower.
            thread2Result.get(10, TimeUnit.SECONDS);
            log.info("COMPLETED HIGH PRIORITY!!!");
          }
        }
      }
    }
  }
}
