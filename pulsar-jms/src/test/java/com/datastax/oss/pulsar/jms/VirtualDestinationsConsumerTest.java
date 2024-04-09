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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.selectors.SelectorSupport;
import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Slf4j
public class VirtualDestinationsConsumerTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer =
      new PulsarContainerExtension()
          .withEnv("PULSAR_PREFIX_transactionCoordinatorEnabled", "false")
          .withEnv("PULSAR_PREFIX_allowAutoTopicCreation", "false");

  private static Stream<Arguments> combinationsTestMultiTopic() {
    return Stream.of(
        Arguments.of(0, true),
        Arguments.of(4, true),
        Arguments.of(0, false),
        Arguments.of(4, false));
  }

  @ParameterizedTest(name = "{index} numPartitions {0} useRegExp {1}")
  @MethodSource("combinationsTestMultiTopic")
  public void testMultiTopic(int numPartitions, boolean useRegExp) throws Exception {
    int numMessagesPerDestination = 10;
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
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

          Queue destination;
          if (useRegExp) {
            destination =
                session.createQueue("regex:persistent://public/default/test-" + prefix + "-.*");
          } else {
            String url =
                destinationsToWrite
                    .stream()
                    .map(d -> Utils.runtimeException(() -> d.getTopicName()))
                    .collect(Collectors.joining(",", "multi:", ""));
            destination = session.createQueue(url);
          }
          PulsarDestination asPulsarDestination = (PulsarDestination) destination;
          assertTrue(asPulsarDestination.isVirtualDestination());
          assertEquals(!useRegExp, asPulsarDestination.isMultiTopic());
          assertEquals(useRegExp, asPulsarDestination.isRegExp());

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

  private static Stream<Arguments>
      combinationsSendUsingExistingPulsarSubscriptionWithServerSideFilterForQueue() {
    return Stream.of(
        Arguments.of(0, true, true),
        Arguments.of(4, true, true),
        Arguments.of(0, false, true),
        Arguments.of(4, false, true),
        Arguments.of(0, true, false),
        Arguments.of(4, true, false),
        Arguments.of(0, false, false),
        Arguments.of(4, false, false));
  }

  @ParameterizedTest(name = "{index} numPartitions {0} embedSubscriptionName {1} useRegExp {2}")
  @MethodSource("combinationsSendUsingExistingPulsarSubscriptionWithServerSideFilterForQueue")
  public void sendUsingExistingPulsarSubscriptionWithServerSideFilterForQueue(
      int numPartitions, boolean embedSubscriptionName, boolean useRegExp) throws Exception {

    String prefix = "test-" + UUID.randomUUID();
    String subscriptionName = "the-sub";
    String selector = "keepme = TRUE";

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.usePulsarAdmin", "true");
    properties.put("jms.useServerSideFiltering", "true");

    if (!embedSubscriptionName) {
      properties.put("jms.queueSubscriptionName", subscriptionName);
    }

    Map<String, String> subscriptionProperties = new HashMap<>();
    subscriptionProperties.put("jms.selector", selector);
    subscriptionProperties.put("jms.filtering", "true");

    List<PulsarQueue> destinationsToWrite = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      String topicName = prefix + "-" + i;
      if (numPartitions > 0) {
        pulsarContainer.getAdmin().topics().createPartitionedTopic(topicName, numPartitions);
      } else {
        pulsarContainer.getAdmin().topics().createNonPartitionedTopic(topicName);
      }

      // create a Subscription with a selector
      pulsarContainer
          .getAdmin()
          .topics()
          .createSubscription(
              topicName, subscriptionName, MessageId.earliest, false, subscriptionProperties);
      destinationsToWrite.add(new PulsarQueue(topicName));
    }

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {

          Queue wildcardDestination;
          if (useRegExp) {
            String pattern = "regex:persistent://public/default/" + prefix + ".*";
            if (embedSubscriptionName) {
              pattern = pattern + ":" + subscriptionName;
            }
            wildcardDestination = session.createQueue(pattern);
          } else {
            String url =
                destinationsToWrite
                    .stream()
                    .map(d -> Utils.runtimeException(() -> d.getQueueName()))
                    .collect(Collectors.joining(",", "multi:", ""));
            if (embedSubscriptionName) {
              url = url + ":" + subscriptionName;
            }
            wildcardDestination = session.createQueue(url);
          }
          PulsarDestination asPulsarDestination = (PulsarDestination) wildcardDestination;
          assertTrue(asPulsarDestination.isVirtualDestination());
          assertEquals(!useRegExp, asPulsarDestination.isMultiTopic());
          assertEquals(useRegExp, asPulsarDestination.isRegExp());

          // do not set the selector, it will be loaded from the Subscription Properties
          try (PulsarMessageConsumer consumer1 = session.createConsumer(wildcardDestination); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());

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

            try (QueueBrowser browser = session.createBrowser(wildcardDestination)) {
              int count = 0;
              Enumeration enumeration = browser.getEnumeration();
              while (enumeration.hasMoreElements()) {
                enumeration.nextElement();
                count++;
              }
              assertEquals(5 * destinationsToWrite.size(), count);
            }

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

            // this is downloaded from the server
            // getMessageSelector returns any of the selectors actually
            assertEquals(selector, consumer1.getMessageSelector());

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

  private static Stream<Arguments>
      combinationsSendUsingExistingPulsarSubscriptionWithClientSideFilterForPartitionedQueue() {
    return Stream.of(
        Arguments.of(0, true),
        Arguments.of(4, true),
        Arguments.of(0, false),
        Arguments.of(4, false));
  }

  @ParameterizedTest(name = "{index} numPartitions {0} useRegExp {1}")
  @MethodSource(
      "combinationsSendUsingExistingPulsarSubscriptionWithClientSideFilterForPartitionedQueue")
  public void sendUsingExistingPulsarSubscriptionWithClientSideFilterForPartitionedQueue(
      int numPartitions, boolean useRegExp) throws Exception {

    String prefix = "test-" + UUID.randomUUID();
    String subscriptionName = "the-sub";
    String selector = "keepme = TRUE";

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.usePulsarAdmin", "true");
    properties.put("jms.useServerSideFiltering", "false");
    properties.put("jms.enableClientSideEmulation", "true");
    properties.put("jms.acknowledgeRejectedMessages", "true");
    properties.put("jms.queueSubscriptionName", subscriptionName);

    List<Queue> destinationsToWrite = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      String topicName = prefix + "-" + i;
      if (numPartitions > 0) {
        pulsarContainer.getAdmin().topics().createPartitionedTopic(topicName, numPartitions);
      } else {
        pulsarContainer.getAdmin().topics().createNonPartitionedTopic(topicName);
      }

      // create a Subscription with a selector
      pulsarContainer
          .getAdmin()
          .topics()
          .createSubscription(topicName, subscriptionName, MessageId.earliest, false);
      destinationsToWrite.add(new PulsarQueue(topicName));
    }

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {

          Queue wildcardDestination;
          if (useRegExp) {
            String pattern = "regex:persistent://public/default/" + prefix + ".*";
            wildcardDestination = session.createQueue(pattern);
          } else {
            String url =
                destinationsToWrite
                    .stream()
                    .map(d -> Utils.runtimeException(() -> d.getQueueName()))
                    .collect(Collectors.joining(",", "multi:", ""));
            wildcardDestination = session.createQueue(url);
          }
          PulsarDestination asPulsarDestination = (PulsarDestination) wildcardDestination;
          assertTrue(asPulsarDestination.isVirtualDestination());
          assertEquals(!useRegExp, asPulsarDestination.isMultiTopic());
          assertEquals(useRegExp, asPulsarDestination.isRegExp());

          // do not set the selector, it will be loaded from the Subscription Properties
          try (PulsarMessageConsumer consumer1 =
              session.createConsumer(wildcardDestination, selector); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());

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

            assertEquals(selector, consumer1.getMessageSelector());

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

  @Test
  public void testPatternConsumerAddingTopicWithServerSideFilters() throws Exception {
    int numMessagesPerDestination = 10;
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.usePulsarAdmin", "true");
    properties.put("jms.useServerSideFiltering", true);
    // discover new topics every 5 seconds
    properties.put("consumerConfig", ImmutableMap.of("patternAutoDiscoveryPeriod", "5"));

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(null)) {
          String prefix = "aaa";

          int nextDestinationId = 0;
          List<Topic> destinationsToWrite = new ArrayList<>();
          for (int i = 0; i < 4; i++) {
            String topicName = "test-" + prefix + "-" + nextDestinationId;
            factory.getPulsarAdmin().topics().createNonPartitionedTopic(topicName);
            destinationsToWrite.add(session.createTopic(topicName));
            nextDestinationId = i + 1;
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
          assertTrue(asPulsarDestination.isVirtualDestination());
          assertTrue(asPulsarDestination.isRegExp());

          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            for (int i = 0; i < count; i++) {
              String payload = consumer.receive().getBody(String.class);
              log.info("Received {}, remaining {}", payload, payloads.size());
              assertFalse(payloads.remove(payload));
            }

            assertTrue(payloads.isEmpty());

            // add a new topic matching the pattern
            // the new topic has server side filters on the jms-queue subscription
            String topicName = "test-" + prefix + "-" + nextDestinationId;
            factory.getPulsarAdmin().topics().createNonPartitionedTopic(topicName);
            // await that the consumer session creates the subscription, then we update it
            Awaitility.await()
                    .untilAsserted(() -> {
                      List<String> subs = pulsarContainer
                              .getAdmin()
                              .topics().getSubscriptions(topicName);
                      assertEquals(subs.size(), 1);
                      assertTrue(subs.contains("jms-queue"));
                    });
            Map<String, String> subscriptionProperties = new HashMap<>();
            subscriptionProperties.put("jms.selector", "keepme=TRUE");
            subscriptionProperties.put("jms.filtering", "true");
            pulsarContainer
                    .getAdmin()
                    .topics()
                    .updateSubscriptionProperties(topicName, "jms-queue", subscriptionProperties);

            Queue newDestination = session.createQueue(topicName);
            TextMessage nextMessage = session.createTextMessage("new");
            nextMessage.setBooleanProperty("keepme", true);
            producer.send(newDestination, nextMessage);
            log.info("id: {}", nextMessage.getJMSMessageID());

            TextMessage received = (TextMessage) consumer.receive();
            assertEquals("new", received.getText());

            Field selectorSupportOnSubscriptions =
                consumer.getClass().getDeclaredField("selectorSupportOnSubscriptions");
            selectorSupportOnSubscriptions.setAccessible(true);
            Map<String, SelectorSupport> downloaded =
                (Map<String, SelectorSupport>) selectorSupportOnSubscriptions.get(consumer);
            assertEquals(5, downloaded.size());
            assertEquals(
                "keepme=TRUE",
                downloaded.get(factory.getPulsarTopicName(newDestination)).getSelector());
          }
        }
      }
    }
  }
}
