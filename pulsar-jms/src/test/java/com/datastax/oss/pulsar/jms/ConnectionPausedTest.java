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

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
public class ConnectionPausedTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

  @Test
  @Timeout(60)
  public void pausedConnectionTest() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        // DO NOT START THE CONNECTION
        try (Session session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          TextMessage textMsg = session.createTextMessage("foo");
          try (MessageProducer producer = session.createProducer(destination); ) {
            try (MessageConsumer consumer =
                session.createSharedDurableConsumer(destination, "sub1")) {

              // send many messages
              producer.send(textMsg);
              producer.send(textMsg);
              producer.send(textMsg);
              producer.send(textMsg);

              ScheduledExecutorService executeLater = Executors.newSingleThreadScheduledExecutor();
              try {

                executeLater.schedule(
                    () -> Utils.noException(() -> connection.start()), 5, TimeUnit.SECONDS);

                // block until the connection is started
                // the connection will be started in 5 seconds and the test won't be stuck
                assertEquals("foo", consumer.receive().getBody(String.class));

                connection.stop();

                // if the connection is stopped and there is a timeout we must return null
                assertNull(consumer.receive(2000));
                assertNull(consumer.receiveNoWait());

                connection.start();

                // now we are able to receive all of the remaining messages
                assertEquals("foo", consumer.receive(2000).getBody(String.class));
                assertEquals("foo", consumer.receiveNoWait().getBody(String.class));
                assertEquals("foo", consumer.receive().getBody(String.class));

              } finally {
                executeLater.shutdown();
              }
            }
          }
        }
      }
    }
  }

  @Test
  @Timeout(60)
  public void stopConnectionMustWaitForPendingReceive() throws Exception {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    CountDownLatch beforeReceive = new CountDownLatch(1);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          TextMessage textMsg = session.createTextMessage("foo");
          try (MessageProducer producer = session.createProducer(destination); ) {

            CompletableFuture<Message> consumerResult = new CompletableFuture<>();
            Thread consumerThread =
                new Thread(
                    () -> {
                      try (Session consumerSession = connection.createSession();
                          MessageConsumer consumer =
                              consumerSession.createSharedDurableConsumer(destination, "sub1")) {
                        // no message in the topic, so this consumer will hang
                        beforeReceive.countDown();
                        log.info("receiving...");
                        consumerResult.complete(consumer.receive());
                      } catch (Throwable err) {
                        consumerResult.completeExceptionally(err);
                      }
                    },
                    "consumer-test-thread");

            consumerThread.start();

            // wait for the consumer to block on "receive"
            beforeReceive.await();
            // wait to enter "receive" method and blocks

            Awaitility.await()
                .untilAsserted(
                    () -> {
                      log.info("Consumer thread status {}", consumerThread);
                      Stream.of(consumerThread.getStackTrace())
                          .forEach(t -> log.info(t.toString()));
                      assertEquals(Thread.State.TIMED_WAITING, consumerThread.getState());
                    });

            ScheduledExecutorService executeLater = Executors.newSingleThreadScheduledExecutor();
            try {

              executeLater.schedule(
                  () -> Utils.noException(() -> producer.send(textMsg)), 5, TimeUnit.SECONDS);

              // as we have one consumer that is blocked Connection#stop must block
              connection.stop();

              // ensure that the consumer received the message
              assertEquals("foo", consumerResult.get().getBody(String.class));

            } finally {
              executeLater.shutdown();
            }
          }
        }
      }
    }
  }
}
