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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.TextMessage;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Slf4j
public class TimeToLiveTest {
  @RegisterExtension
  static PulsarContainerExtension pulsarContainer =
      new PulsarContainerExtension()
          .withEnv("PULSAR_PREFIX_jmsProcessingMode", "full")
          .withEnv("PULSAR_PREFIX_jmsProcessJMSExpiration", "true");

  private static Stream<Arguments> combinations() {
    return Stream.of(
        Arguments.of(true, true),
        Arguments.of(false, true),
        Arguments.of(true, false),
        Arguments.of(false, false));
  }

  @ParameterizedTest(name = "{index} useServerSideFiltering {0} enableBatching {1}")
  @MethodSource("combinations")
  public void sendMessageReceiveFromQueueWithTimeToLive(
      boolean useServerSideFiltering, boolean enableBatching) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.enableClientSideEmulation", !useServerSideFiltering);
    properties.put("jms.useServerSideFiltering", useServerSideFiltering);
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batchingEnabled", enableBatching);
    properties.put("producerConfig", producerConfig);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection(); ) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageProducer producer = session.createProducer(destination); ) {
            for (int i = 0; i < 10; i++) {
              TextMessage textMessage = session.createTextMessage("foo-" + i);
              // 1 second timeToLive to some messages
              long timeToLive;
              if (i % 2 == 0) {
                timeToLive = 1000;
              } else {
                timeToLive = 0;
              }
              producer.send(
                  textMessage, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, timeToLive);
              if (timeToLive > 0) {
                assertTrue(textMessage.getJMSExpiration() > 0);
              }
            }
          }

          // wait for messages to expire
          Thread.sleep(2000);

          // please note that the server-side filter is applied on the broker when it pushed the
          // messages
          // to the consumer, and not when the consumer receives the messages
          // so with server-side filtering it is still possible to see a message dispatched to a
          // consumer
          // if the consumer is connected when the producer sends the message

          // here we are creating the Consumer after Thread.sleep, so when the broker
          // dispatches the messages they are already expired
          try (PulsarMessageConsumer consumer1 = session.createConsumer(destination); ) {
            assertEquals(SubscriptionType.Shared, consumer1.getSubscriptionType());

            // only foo-1, foo-3, foo-5... can be received
            for (int i = 0; i < 5; i++) {
              TextMessage textMessage = (TextMessage) consumer1.receive(10000);
              log.info("received {}", textMessage);
              assertNotNull(textMessage, "only " + i + " messages have been received");
              assertEquals("foo-" + (i * 2 + 1), textMessage.getText());
            }

            if (useServerSideFiltering) {
              assertEquals(5, consumer1.getReceivedMessages());
              assertEquals(0, consumer1.getSkippedMessages());
            } else {
              assertEquals(10, consumer1.getReceivedMessages());
              assertEquals(5, consumer1.getSkippedMessages());
            }
          }
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} useServerSideFiltering {0} enableBatching {1}")
  @MethodSource("combinations")
  public void sendMessageReceiveFromTopicWithTimeToLive(
      boolean useServerSideFiltering, boolean enableBatching) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.enableClientSideEmulation", !useServerSideFiltering);
    properties.put("jms.useServerSideFiltering", useServerSideFiltering);
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batchingEnabled", enableBatching);
    properties.put("producerConfig", producerConfig);

    // we don't want the consumer to pre-fetch all the messages
    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("receiverQueueSize", 1);
    properties.put("consumerConfig", consumerConfig);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection(); ) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          // please note that the server-side filter is applied on the broker when it pushed the
          // messages
          // to the consumer, and not when the consumer receives the messages
          // so with server-side filtering it is still possible to see a message dispatched to a
          // consumer
          // if the consumer is connected when the producer sends the message
          // here we are creating the Consumer before sending the messages
          // so one message (receiverQueueSize=1) will be processed on the broker as soon as it has
          // been
          // produced
          try (PulsarMessageConsumer consumer1 = session.createConsumer(destination); ) {
            assertEquals(SubscriptionType.Exclusive, consumer1.getSubscriptionType());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                // 1 second timeToLive to some of the messages
                long timeToLive;
                if (i % 2 == 0) {
                  timeToLive = 1000;
                } else {
                  timeToLive = 0;
                }
                producer.send(
                    textMessage, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, timeToLive);
                if (timeToLive > 0) {
                  assertTrue(textMessage.getJMSExpiration() > 0);
                }
              }
            }

            // wait for messages to expire
            Thread.sleep(2000);

            // only foo-1, foo-3, foo-5... can be received
            for (int i = 0; i < 5; i++) {
              TextMessage textMessage = (TextMessage) consumer1.receive();
              assertEquals("foo-" + (i * 2 + 1), textMessage.getText());
            }

            if (useServerSideFiltering) {
              // the consumer pre-fetches one message, so this is dispatched before Thread.sleep()
              assertEquals(1, consumer1.getSkippedMessages());
              assertEquals(6, consumer1.getReceivedMessages());
            } else {
              assertEquals(5, consumer1.getSkippedMessages());
              assertEquals(10, consumer1.getReceivedMessages());
            }
          }
        }
      }
    }
  }
}
