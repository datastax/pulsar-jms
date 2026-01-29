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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import jakarta.jms.CompletionListener;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
public class DeadLetterQueueTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer =
      new PulsarContainerExtension()
          .withEnv("PULSAR_PREFIX_transactionCoordinatorEnabled", "false");

  @Test
  public void deadLetterTestForQueue() throws Exception {

    // basic test
    String topic = "persistent://public/default/test-" + UUID.randomUUID();
    // this name is automatic, but you can configure it
    // https://pulsar.apache.org/docs/concepts-messaging/#dead-letter-topic
    String queueSubscriptionName = "thesub";
    String deadLetterTopic = topic + "-" + queueSubscriptionName + "-DLQ";

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.queueSubscriptionName", queueSubscriptionName);

    Map<String, Object> consumerConfig = new HashMap<>();
    properties.put("consumerConfig", consumerConfig);

    consumerConfig.put("ackTimeoutMillis", 1000);

    Map<String, Object> deadLetterPolicy = new HashMap<>();
    consumerConfig.put("deadLetterPolicy", deadLetterPolicy);
    deadLetterPolicy.put("maxRedeliverCount", 1);
    // this is the name of a subscription that is created
    // while creating the producer to the DQL topic
    deadLetterPolicy.put("initialSubscriptionName", "dqlsub");

    performTest(topic, deadLetterTopic, properties, false);
  }

  @Test
  public void deadLetterTestForTopic() throws Exception {

    // basic test
    String topic = "persistent://public/default/test-" + UUID.randomUUID();
    // this name is automatic, but you can configure it
    // https://pulsar.apache.org/docs/concepts-messaging/#dead-letter-topic
    String topicSubscriptionName = "thesub";
    String deadLetterTopic = topic + "-" + topicSubscriptionName + "-DLQ";

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();

    Map<String, Object> consumerConfig = new HashMap<>();
    properties.put("consumerConfig", consumerConfig);

    consumerConfig.put("ackTimeoutMillis", 1000);

    Map<String, Object> deadLetterPolicy = new HashMap<>();
    consumerConfig.put("deadLetterPolicy", deadLetterPolicy);
    deadLetterPolicy.put("maxRedeliverCount", 1);
    // this is the name of a subscription that is created
    // while creating the producer to the DQL topic
    deadLetterPolicy.put("initialSubscriptionName", "dqlsub");

    performTest(topic, deadLetterTopic, properties, true, topicSubscriptionName);
  }

  @Test
  public void deadLetterConfigTest() throws Exception {

    // in this test we try to set al possible configuration value

    String topic = "persistent://public/default/test-" + UUID.randomUUID();
    String deadLetterTopic = "persistent://public/default/test-dlq-" + UUID.randomUUID();

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();

    Map<String, Object> consumerConfig = new HashMap<>();
    properties.put("consumerConfig", consumerConfig);

    consumerConfig.put("ackTimeoutMillis", 1000);

    Map<String, Object> deadLetterPolicy = new HashMap<>();
    consumerConfig.put("deadLetterPolicy", deadLetterPolicy);
    deadLetterPolicy.put("maxRedeliverCount", 1);
    deadLetterPolicy.put("deadLetterTopic", deadLetterTopic);
    deadLetterPolicy.put("initialSubscriptionName", "dqlsub");

    Map<String, Object> negativeAckRedeliveryBackoff = new HashMap<>();
    consumerConfig.put("negativeAckRedeliveryBackoff", negativeAckRedeliveryBackoff);
    negativeAckRedeliveryBackoff.put("minDelayMs", 10);
    negativeAckRedeliveryBackoff.put("maxDelayMs", 100);
    negativeAckRedeliveryBackoff.put("multiplier", 2.0);

    Map<String, Object> ackTimeoutRedeliveryBackoff = new HashMap<>();
    consumerConfig.put("ackTimeoutRedeliveryBackoff", ackTimeoutRedeliveryBackoff);
    ackTimeoutRedeliveryBackoff.put("minDelayMs", 10);
    ackTimeoutRedeliveryBackoff.put("maxDelayMs", 100);
    ackTimeoutRedeliveryBackoff.put("multiplier", 2.0);

    performTest(topic, deadLetterTopic, properties, false);
  }

  private void performTest(
      String topic, String deadLetterTopic, Map<String, Object> properties, boolean useTopic)
      throws Exception {
    performTest(topic, deadLetterTopic, properties, useTopic, null);
  }

  private void performTest(
      String topic,
      String deadLetterTopic,
      Map<String, Object> properties,
      boolean useTopic,
      String topicSubscriptionName)
      throws Exception {
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
            Session session2 = connection.createSession(Session.AUTO_ACKNOWLEDGE); ) {
          Destination destination =
              useTopic ? session.createTopic(topic) : session.createQueue(topic);

          try (MessageConsumer consumer1 =
              useTopic
                  ? session.createSharedConsumer((Topic) destination, topicSubscriptionName)
                  : session.createConsumer(destination)) {

            Topic destinationDeadletter = session.createTopic(deadLetterTopic);

            try (MessageConsumer consumerDeadLetter =
                session2.createSharedConsumer(destinationDeadletter, "dqlsub"); ) {

              try (MessageProducer producer = session2.createProducer(destination); ) {
                producer.send(session.createTextMessage("foo"));
              }

              Message message = consumer1.receive();
              assertEquals("foo", message.getBody(String.class));
              assertEquals(1, message.getIntProperty("JMSXDeliveryCount"));
              assertFalse(message.getJMSRedelivered());

              // message is re-delivered again after ackTimeoutMillis
              message = consumer1.receive();
              assertEquals("foo", message.getBody(String.class));
              assertEquals(2, message.getIntProperty("JMSXDeliveryCount"));
              assertTrue(message.getJMSRedelivered());

              message = consumerDeadLetter.receive();
              assertEquals("foo", message.getBody(String.class));
              log.debug("DLQ MESSAGE {}", message);
              // this is another topic, and the JMSXDeliveryCount is only handled on the client side
              // so the count is back to 1
              assertEquals(1, message.getIntProperty("JMSXDeliveryCount"));
              assertFalse(message.getJMSRedelivered());
            }

            // verify that the messages are not leaking
            Field unackedMessagesField = session.getClass().getDeclaredField("unackedMessages");
            unackedMessagesField.setAccessible(true);
            List<PulsarMessage> unackedMessages =
                (List<PulsarMessage>) unackedMessagesField.get(session);
            Awaitility.await()
                .untilAsserted(
                    () -> {
                      log.debug("unackedMessages {}", unackedMessages);
                      assertTrue(unackedMessages.isEmpty());
                    });
          }
        }
      }
    }
  }

  @ParameterizedTest(name = "numPartitions {0}")
  @ValueSource(ints = {0, 1, 4})
  public void batchingTest(int numPartitions) throws Exception {

    // basic test
    String topic = "persistent://public/default/test-" + UUID.randomUUID();

    // this name is automatic, but you can configure it
    // https://pulsar.apache.org/docs/concepts-messaging/#dead-letter-topic
    String queueSubscriptionName = "thesub";
    String deadLetterTopic;

    if (numPartitions > 0) {
      pulsarContainer.getAdmin().topics().createPartitionedTopic(topic, numPartitions);

      // multi:topic1,topic2....
      deadLetterTopic =
          IntStream.range(0, numPartitions)
              .mapToObj(
                  partition -> {
                    return topic + "-partition-" + partition + "-" + queueSubscriptionName + "-DLQ";
                  })
              .collect(Collectors.joining(",", "multi:", ""));

    } else {
      deadLetterTopic = topic + "-" + queueSubscriptionName + "-DLQ";
    }
    log.debug("deadLetterTopic {}", deadLetterTopic);

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.queueSubscriptionName", queueSubscriptionName);

    Map<String, Object> producerConfig = new HashMap<>();
    properties.put("producerConfig", producerConfig);

    producerConfig.put("batchingEnabled", "true");
    producerConfig.put("batchingMaxPublishDelayMicros", "1000000");
    producerConfig.put("batchingMaxMessages", "1000000");

    Map<String, Object> consumerConfig = new HashMap<>();
    properties.put("consumerConfig", consumerConfig);

    consumerConfig.put("batchIndexAckEnabled", "true");

    consumerConfig.put("ackTimeoutMillis", 1000);

    Map<String, Object> deadLetterPolicy = new HashMap<>();
    consumerConfig.put("deadLetterPolicy", deadLetterPolicy);
    deadLetterPolicy.put("maxRedeliverCount", 1);
    // this is the name of a subscription that is created
    // while creating the producer to the DQL topic
    deadLetterPolicy.put("initialSubscriptionName", "dqlsub");

    performTestWithBatching(topic, deadLetterTopic, properties, numPartitions);
  }

  private void performTestWithBatching(
      String topic, String deadLetterTopic, Map<String, Object> properties, int numPartitions)
      throws Exception {
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
            PulsarSession session2 = connection.createSession(Session.AUTO_ACKNOWLEDGE); ) {
          Destination destination = session.createQueue(topic);

          try (MessageConsumer consumer1 = session.createConsumer(destination)) {

            Topic destinationDeadletter = session.createTopic(deadLetterTopic);

            try (MessageConsumer consumerDeadLetter =
                session2.createSharedConsumer(destinationDeadletter, "dqlsub"); ) {

              int numMessages = 10;
              try (MessageProducer producer = session2.createProducer(destination); ) {
                CountDownLatch count = new CountDownLatch(numMessages);
                List<Message> sentMessages = new CopyOnWriteArrayList<>();
                for (int i = 0; i < numMessages; i++) {
                  producer.send(
                      session.createTextMessage("foo"),
                      new CompletionListener() {
                        @Override
                        public void onCompletion(Message message) {
                          sentMessages.add(message);
                          count.countDown();
                        }

                        @Override
                        public void onException(Message message, Exception e) {}
                      });
                }
                assertTrue(count.await(10, TimeUnit.SECONDS));
              }

              Map<String, Integer> counterByMessageId = new HashMap<>();
              // receive all the messages, without acknowledging them.
              // they will be redelivered multiple-times
              while (true) {
                PulsarMessage message = (PulsarMessage) consumer1.receive();
                assertEquals("foo", message.getBody(String.class));
                int _JMSXDeliveryCount = message.getIntProperty("JMSXDeliveryCount");
                counterByMessageId.put(message.getJMSMessageID(), _JMSXDeliveryCount);
                assertEquals(_JMSXDeliveryCount > 1, message.getJMSRedelivered());

                if (numPartitions > 0) {
                  assertTrue(
                      message.getReceivedPulsarMessage().getMessageId()
                          instanceof TopicMessageIdImpl,
                      "bad message type "
                          + message.getReceivedPulsarMessage().getMessageId().getClass());
                } else {
                  assertTrue(
                      message.getReceivedPulsarMessage().getMessageId()
                          instanceof BatchMessageIdImpl,
                      "bad message type "
                          + message.getReceivedPulsarMessage().getMessageId().getClass());
                }

                log.debug("counterByMessageId {}", counterByMessageId);
                // wait that all messages have been re-delivered at least 2 times, in order to
                // trigger the DLQ
                if ((counterByMessageId.size() == numMessages)
                    && counterByMessageId.values().stream().allMatch(i -> i.intValue() >= 2)) {
                  break;
                }
              }

              List<Message> fromDeadLetter = new ArrayList<>();
              while (true) {
                PulsarMessage message = (PulsarMessage) consumerDeadLetter.receive(10000);
                if (message == null) {
                  break;
                }
                assertEquals("foo", message.getBody(String.class));
                log.debug("DLQ MESSAGE {}", message);
                // this is another topic, and the JMSXDeliveryCount is only handled on the client
                // side
                // so the count is back to 1
                assertEquals(1, message.getIntProperty("JMSXDeliveryCount"));
                assertFalse(message.getJMSRedelivered());
                fromDeadLetter.add(message);
              }

              if (numPartitions > 0) {
                assertTrue(fromDeadLetter.size() >= numMessages);
              } else {
                // no duplicates on the DLQ
                assertEquals(numMessages, fromDeadLetter.size());
                assertEquals(
                    numMessages,
                    fromDeadLetter
                        .stream()
                        .map(m -> Utils.runtimeException(() -> m.getJMSMessageID()))
                        .distinct()
                        .count());
              }
            }

            // verify that the messages are not leaking
            Field unackedMessagesField = session.getClass().getDeclaredField("unackedMessages");
            unackedMessagesField.setAccessible(true);
            List<PulsarMessage> unackedMessages =
                (List<PulsarMessage>) unackedMessagesField.get(session);
            Awaitility.await()
                .untilAsserted(
                    () -> {
                      log.debug("unackedMessages {}", unackedMessages);
                      assertTrue(unackedMessages.isEmpty());
                    });
          }
        }
      }
    }
  }
}
