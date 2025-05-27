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
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class JMSPublishFiltersBase {

  abstract PulsarContainerExtension getPulsarContainer();

  private Map<String, Object> buildProperties() {
    Map<String, Object> properties = getPulsarContainer().buildJMSConnectionProperties();
    properties.put("jms.useServerSideFiltering", true);
    properties.put("jms.enableClientSideEmulation", false);

    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batchingEnabled", false);
    properties.put("producerConfig", producerConfig);
    return properties;
  }

  @Test
  public void sendMessageReceiveFromQueue() throws Exception {
    sendMessageReceiveFromQueue(false);
  }

  @Test
  public void sendMessageReceiveFromQueueInTransaction() throws Exception {
    sendMessageReceiveFromQueue(true);
  }

  private void sendMessageReceiveFromQueue(boolean transacted) throws Exception {
    Map<String, Object> properties = buildProperties();
    properties.put("enableTransaction", true);
    String topicName = "persistent://public/default/test-" + UUID.randomUUID();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session =
            connection.createSession(
                transacted ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE); ) {
          Queue destination = session.createQueue(topicName);

          try (PulsarMessageConsumer consumer1 = session.createConsumer(destination); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());

            String newSelector = "lastMessage=TRUE";
            Map<String, String> subscriptionProperties = new HashMap<>();
            subscriptionProperties.put("jms.selector", newSelector);
            subscriptionProperties.put("jms.filtering", "true");

            getPulsarContainer()
                .getAdmin()
                .topics()
                .updateSubscriptionProperties(topicName, "jms-queue", subscriptionProperties);

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                if (i == 9) {
                  textMessage.setBooleanProperty("lastMessage", true);
                }
                producer.send(textMessage);
              }
              if (transacted) {
                session.commit();
              }
            }

            // wait for the filters to be processed in background
            Thread.sleep(5000);

            TextMessage textMessage = (TextMessage) consumer1.receive();
            assertEquals("foo-9", textMessage.getText());

            assertEquals(1, consumer1.getReceivedMessages());
            assertEquals(0, consumer1.getSkippedMessages());

            // no more messages
            assertNull(consumer1.receiveNoWait());

            Awaitility.await()
                .untilAsserted(
                    () -> {
                      // ensure that the filter didn't reject any message while dispatching to the
                      // consumer
                      // because the filter has been already applied on the write path
                      TopicStats stats =
                          getPulsarContainer().getAdmin().topics().getStats(topicName);
                      SubscriptionStats subscriptionStats =
                          stats.getSubscriptions().get("jms-queue");
                      if (transacted) {
                        // when we enable transactions the stats are not updated correctly
                        // it seems that the transaction marker is counted as "processed by filters"
                        // but actually it is not processed by the JMSFilter at all
                        assertEquals(subscriptionStats.getFilterProcessedMsgCount(), 2);
                        assertEquals(subscriptionStats.getFilterRejectedMsgCount(), 0);
                        assertEquals(subscriptionStats.getFilterAcceptedMsgCount(), 1);
                        session.commit();
                      } else {
                        assertEquals(subscriptionStats.getFilterProcessedMsgCount(), 1);
                        assertEquals(subscriptionStats.getFilterRejectedMsgCount(), 0);
                        assertEquals(subscriptionStats.getFilterAcceptedMsgCount(), 1);
                      }
                    });
          }

          // create a message that doesn't match the filter
          // verify that the back log is accurate (0)

          try (MessageProducer producer = session.createProducer(destination); ) {
            TextMessage textMessage = session.createTextMessage("backlog");
            producer.send(textMessage);

            Awaitility.await()
                .untilAsserted(
                    () -> {
                      TopicStats stats =
                          getPulsarContainer().getAdmin().topics().getStats(topicName);
                      SubscriptionStats subscriptionStats =
                          stats.getSubscriptions().get("jms-queue");
                      assertEquals(0, subscriptionStats.getMsgBacklog());
                    });

            if (transacted) {
              session.commit();
            }
          }
        }
      }
    }
  }

  @Test
  void testManyMessagesWithPartitions() throws Exception {
    Map<String, Object> properties = buildProperties();
    String topicName = "persistent://public/default/test-" + UUID.randomUUID();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(Session.AUTO_ACKNOWLEDGE); ) {
          factory.getPulsarAdmin().createPartitionedTopic(topicName, 20);

          Queue destination = session.createQueue(topicName);

          try (PulsarMessageConsumer consumer1 = session.createConsumer(destination); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());

            String newSelector = "foo='bar'";
            Map<String, String> subscriptionProperties = new HashMap<>();
            subscriptionProperties.put("jms.selector", newSelector);
            subscriptionProperties.put("jms.filtering", "true");

            getPulsarContainer()
                .getAdmin()
                .topics()
                .updateSubscriptionProperties(topicName, "jms-queue", subscriptionProperties);

            int numMessages = 10000;
            try (MessageProducer producer = session.createProducer(destination); ) {
              List<CompletableFutureCompletionListener> futures = new ArrayList<>();
              for (int i = 0; i < numMessages; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                for (int j = 0; j < 10; j++) {
                  textMessage.setIntProperty("some" + j, j);
                }
                // half of the messages pass the filter
                if (i % 2 == 0) {
                  textMessage.setStringProperty("foo", "bar");
                }
                CompletableFutureCompletionListener listener =
                    new CompletableFutureCompletionListener();
                futures.add(listener);
                producer.send(textMessage, listener);
                if (futures.size() == 1000) {
                  for (CompletableFutureCompletionListener future : futures) {
                    future.get();
                  }
                  futures.clear();
                }
              }
              for (CompletableFutureCompletionListener future : futures) {
                future.get();
              }
            }

            // wait for the filters to be processed in background
            Awaitility.await()
                .untilAsserted(
                    () -> {
                      PartitionedTopicStats partitionedInternalStats =
                          factory.getPulsarAdmin().getPartitionedTopicStats(topicName, true);
                      AtomicLong sum = new AtomicLong();
                      partitionedInternalStats
                          .getPartitions()
                          .forEach(
                              (partition, stats) -> {
                                SubscriptionStats subscriptionStats =
                                    stats.getSubscriptions().get("jms-queue");
                                log.info(
                                    "backlog for partition {}: {}",
                                    partition,
                                    subscriptionStats.getMsgBacklog());
                                sum.addAndGet(subscriptionStats.getMsgBacklog());
                              });
                      log.info("total backlog: {}", sum.get());
                      assertEquals(numMessages / 2, sum.get());
                    });
          }
        }
      }
    }
  }
}
