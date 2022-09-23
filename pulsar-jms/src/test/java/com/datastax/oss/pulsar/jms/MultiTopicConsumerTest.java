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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.jms.Connection;
import javax.jms.InvalidDestinationException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class MultiTopicConsumerTest {

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  @BeforeAll
  public static void before() throws Exception {
    cluster =
        new PulsarCluster(
            tempDir,
            serviceConfiguration -> {
              serviceConfiguration.setTransactionCoordinatorEnabled(false);
              serviceConfiguration.setAllowAutoTopicCreation(false);
            });
    cluster.start();
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  public void testMultiTopicNonPartitioned() throws Exception {
    testMultiTopic(0);
  }

  @Test
  public void testMultiTopicPartitioned() throws Exception {
    testMultiTopic(4);
  }

  private void testMultiTopic(int numPartitions) throws Exception {
    int numMessagesPerDestination = 10;
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.usePulsarAdmin", "true");

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(null)) {
          String prefix = UUID.randomUUID().toString();

          List<Topic> destinationsToWrite = new ArrayList<>();
          for (int i = 0; i < 4; i++) {
            String topicName = "test-" + prefix + "-" + i;
            if (numPartitions > 0) {
              factory.getPulsarAdmin().topics().createPartitionedTopic(topicName, numPartitions);
            } else {
              factory.getPulsarAdmin().topics().createNonPartitionedTopic(topicName);
            }
            destinationsToWrite.add(session.createTopic(topicName));
          }

          Set<String> payloads = new HashSet<>();
          int count = 0;
          for (int i = 0; i < numMessagesPerDestination; i++) {
            for (Topic destination : destinationsToWrite) {
              String payload = "foo - " + count * i;
              log.info("write {} to {}", payload, destination);
              producer.send(destination, session.createTextMessage(payload));
              count++;
            }
          }

          Queue destination =
              session.createQueue("regex:persistent://public/default/test-" + prefix + "-.*");
          PulsarDestination asPulsarDestination = (PulsarDestination) destination;
          assertTrue(asPulsarDestination.isRegExp());

          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            for (int i = 0; i < count; i++) {
              String payload = consumer.receive().getBody(String.class);
              log.info("Received {}, remaining {}", payload, payloads.size());
              assertFalse(payloads.remove(payload));
            }
          }
          assertTrue(payloads.isEmpty());
        }
      }
    }
  }

  @Test
  public void sendUsingExistingPulsarSubscriptionWithServerSideFilterForNonPartitionedQueue()
      throws Exception {
    sendUsingExistingPulsarSubscriptionWithServerSideFilterForQueue(0, false);
  }

  @Test
  public void sendUsingExistingPulsarSubscriptionWithServerSideFilterForPartitionedQueue()
      throws Exception {
    sendUsingExistingPulsarSubscriptionWithServerSideFilterForQueue(4, false);
  }

  @Test
  public void
      sendUsingExistingPulsarSubscriptionWithServerSideFilterForNonPartitionedQueueWithCustomSubscription()
          throws Exception {
    sendUsingExistingPulsarSubscriptionWithServerSideFilterForQueue(0, true);
  }

  @Test
  public void
      sendUsingExistingPulsarSubscriptionWithServerSideFilterForPartitionedQueueWithCustomSubscription()
          throws Exception {
    sendUsingExistingPulsarSubscriptionWithServerSideFilterForQueue(4, true);
  }

  private void sendUsingExistingPulsarSubscriptionWithServerSideFilterForQueue(
      int numPartitions, boolean embedSubscriptionName) throws Exception {

    String prefix = "test-" + UUID.randomUUID();
    String subscriptionName = "the-sub";
    String selector = "keepme = TRUE";

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.usePulsarAdmin", "true");
    properties.put("jms.useServerSideFiltering", "true");

    properties.put("jms.prependTopicNameToCustomQueueSubscriptionName", "false");
    if (!embedSubscriptionName) {
      properties.put("jms.queueSubscriptionName", subscriptionName);
    }

    Map<String, String> subscriptionProperties = new HashMap<>();
    subscriptionProperties.put("jms.selector", selector);
    subscriptionProperties.put("jms.filtering", "true");

    List<Queue> destinationsToWrite = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      String topicName = prefix + "-" + i;
      if (numPartitions > 0) {
        cluster
            .getService()
            .getAdminClient()
            .topics()
            .createPartitionedTopic(topicName, numPartitions);
      } else {
        cluster.getService().getAdminClient().topics().createNonPartitionedTopic(topicName);
      }

      // create a Subscription with a selector
      cluster
          .getService()
          .getAdminClient()
          .topics()
          .createSubscription(
              topicName, subscriptionName, MessageId.earliest, false, subscriptionProperties);
      destinationsToWrite.add(new PulsarQueue(topicName));
    }

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {

          String pattern = "regex:persistent://public/default/" + prefix + ".*";
          if (embedSubscriptionName) {
            pattern = pattern + ":" + subscriptionName;
          }
          Queue wildcardDestination = session.createQueue(pattern);

          // do not set the selector, it will be loaded from the Subscription Properties
          try (PulsarMessageConsumer consumer1 = session.createConsumer(wildcardDestination); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());

            // this is downloaded from the server
            // getMessageSelector returns any of the selectors actually
            assertEquals(selector, consumer1.getMessageSelector());

            try (MessageProducer producer = session.createProducer(null); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                if (i % 2 == 0) {
                  textMessage.setBooleanProperty("keepme", true);
                }
                for (Queue queue : destinationsToWrite) {
                  producer.send(queue, textMessage);
                  log.info("Sent {} to {}", textMessage.getText(), queue);
                }
              }
            }

            assertThrows(
                InvalidDestinationException.class,
                () -> session.createBrowser(wildcardDestination));

            // with a partitioned/multi topic we don't have control over ordering
            List<String> received = new ArrayList<>();
            for (int i = 0; i < 10 * destinationsToWrite.size(); i++) {
              if (i % 2 == 0) {
                TextMessage textMessage = (TextMessage) consumer1.receive();
                log.info(
                    "Received {} from {}", textMessage.getText(), textMessage.getJMSDestination());
                received.add(textMessage.getText());
              }
            }
            for (int i = 0; i < 10; i++) {
              for (int j = 0; j < destinationsToWrite.size(); j++) {
                if (i % 2 == 0) {
                  String expected = "foo-" + i;
                  log.info("Removing {} from {}", expected, received);
                  assertTrue(received.remove("foo-" + i));
                }
              }
            }
            assertTrue(received.isEmpty());

            assertEquals(5 * destinationsToWrite.size(), consumer1.getReceivedMessages());
            assertEquals(0, consumer1.getSkippedMessages());

            // no more messages
            assertNull(consumer1.receiveNoWait());
          }
        }
      }
    }
  }

  @Test
  public void sendUsingExistingPulsarSubscriptionWithClientSideFilterForNonPartitionedQueue()
      throws Exception {
    sendUsingExistingPulsarSubscriptionWithClientSideFilterForPartitionedQueue(0);
  }

  @Test
  public void sendUsingExistingPulsarSubscriptionWithClientSideFilterForPartitionedQueue()
      throws Exception {
    sendUsingExistingPulsarSubscriptionWithClientSideFilterForPartitionedQueue(4);
  }

  private void sendUsingExistingPulsarSubscriptionWithClientSideFilterForPartitionedQueue(
      int numPartitions) throws Exception {

    String prefix = "test-" + UUID.randomUUID();
    String subscriptionName = "the-sub";
    String selector = "keepme = TRUE";

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.usePulsarAdmin", "true");
    properties.put("jms.useServerSideFiltering", "false");
    properties.put("jms.enableClientSideEmulation", "true");
    properties.put("jms.acknowledgeRejectedMessages", "true");
    properties.put("jms.queueSubscriptionName", subscriptionName);

    List<Queue> destinationsToWrite = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      String topicName = prefix + "-" + i;
      if (numPartitions > 0) {
        cluster
            .getService()
            .getAdminClient()
            .topics()
            .createPartitionedTopic(topicName, numPartitions);
      } else {
        cluster.getService().getAdminClient().topics().createNonPartitionedTopic(topicName);
      }

      // create a Subscription with a selector
      cluster
          .getService()
          .getAdminClient()
          .topics()
          .createSubscription(topicName, subscriptionName, MessageId.earliest, false);
      destinationsToWrite.add(new PulsarQueue(topicName));
    }

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {

          Queue wildcardDestination =
              session.createQueue("regex:persistent://public/default/" + prefix + ".*");

          // do not set the selector, it will be loaded from the Subscription Properties
          try (PulsarMessageConsumer consumer1 =
              session.createConsumer(wildcardDestination, selector); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());

            assertEquals(selector, consumer1.getMessageSelector());

            int totalSent = 0;
            try (MessageProducer producer = session.createProducer(null); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                if (i % 2 == 0) {
                  textMessage.setBooleanProperty("keepme", true);
                }
                for (Queue queue : destinationsToWrite) {
                  producer.send(queue, textMessage);
                  totalSent++;
                }
              }
            }

            int totalReceived = 0;
            // with a partitioned topic we don't have control over ordering
            List<String> received = new ArrayList<>();
            for (int i = 0; i < 10 * destinationsToWrite.size(); i++) {
              if (i % 2 == 0) {
                TextMessage textMessage = (TextMessage) consumer1.receive();
                received.add(textMessage.getText());
                totalReceived++;
              }
            }
            for (int i = 0; i < 10; i++) {
              for (int j = 0; j < destinationsToWrite.size(); j++) {
                if (i % 2 == 0) {
                  String expected = "foo-" + i;
                  assertTrue(received.remove("foo-" + i));
                }
              }
            }
            assertTrue(received.isEmpty());

            // drain the last messages, the will be skipped on the client side
            assertNull(consumer1.receive(1000));
            assertNull(consumer1.receive(1000));
            assertNull(consumer1.receive(1000));
            assertNull(consumer1.receive(1000));

            assertEquals(totalSent, consumer1.getReceivedMessages());
            assertEquals(totalSent - totalReceived, consumer1.getSkippedMessages());

            // no more messages
            assertNull(consumer1.receiveNoWait());
          }
        }
      }
    }
  }
}
