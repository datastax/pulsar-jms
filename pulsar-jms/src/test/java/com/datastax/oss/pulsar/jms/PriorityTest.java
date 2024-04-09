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
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.jms.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
public class PriorityTest {

  private static final String SYSTEM_NAMESPACE_OVERRIDDEN = "foo/ns";

  static int LOW_PRIORITY = 4;
  static int HIGH_PRIORITY = 9;

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer =
      new PulsarContainerExtension()
          .withEnv("PULSAR_PREFIX_allowAutoTopicCreation", "true")
          .withEnv("PULSAR_PREFIX_allowAutoTopicCreationType", "partitioned")
          .withEnv("PULSAR_PREFIX_defaultNumPartitions", "10")
          .withEnv("PULSAR_PREFIX_transactionCoordinatorEnabled", "false")
          .withOnContainerReady(
              new Consumer<PulsarContainerExtension>() {
                @Override
                @SneakyThrows
                public void accept(PulsarContainerExtension pulsarContainerExtension) {
                  Set<String> clusters =
                      new HashSet<>(pulsarContainerExtension.getAdmin().clusters().getClusters());
                  pulsarContainerExtension
                      .getAdmin()
                      .tenants()
                      .createTenant("foo", TenantInfo.builder().allowedClusters(clusters).build());
                  pulsarContainerExtension
                      .getAdmin()
                      .namespaces()
                      .createNamespace(SYSTEM_NAMESPACE_OVERRIDDEN);
                }
              });

  private static Stream<Arguments> combinations() {
    return Stream.of(
        Arguments.of(4, "linear"),
        Arguments.of(4, "non-linear"),
        Arguments.of(10, "linear"),
        Arguments.of(10, "non-linear"));
  }

  @ParameterizedTest(name = "numPartitions {0} mapping {1}")
  @MethodSource("combinations")
  public void basicTest(int numPartitions, String mapping) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.enableJMSPriority", true);
    properties.put("jms.priorityMapping", mapping);
    properties.put("jms.systemNamespace", SYSTEM_NAMESPACE_OVERRIDDEN);
    properties.put(
        "producerConfig", ImmutableMap.of("blockIfQueueFull", true, "batchingEnabled", false));
    properties.put("consumerConfig", ImmutableMap.of("receiverQueueSize", 10));

    String topicName = "test-" + UUID.randomUUID();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {

        assertTrue(factory.isEnableJMSPriority());
        assertEquals(mapping.equals("linear"), factory.isPriorityUseLinearMapping());

        connection.start();
        try (Session session = connection.createSession(); ) {
          Queue destination = session.createQueue(topicName);

          pulsarContainer
              .getAdmin()
              .topics()
              .createPartitionedTopic(factory.getPulsarTopicName(destination), numPartitions);

          int numMessages = 100;
          try (MessageProducer producer = session.createProducer(destination); ) {
            for (int i = 0; i < numMessages; i++) {
              TextMessage textMessage = session.createTextMessage("foo-" + i);
              if (i < numMessages / 2) {
                // the first messages are lower priority
                producer.setPriority(LOW_PRIORITY);
              } else {
                producer.setPriority(HIGH_PRIORITY);
              }
              log.info("send {} prio {}", textMessage.getText(), producer.getPriority());
              producer.send(textMessage);
            }
          }

          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {

            List<TextMessage> received = new ArrayList<>();
            for (int i = 0; i < numMessages; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();
              log.info(
                  "got msg {} prio {} from {} actually {}",
                  msg.getText(),
                  msg.getJMSPriority(),
                  msg.getJMSDestination(),
                  ((PulsarMessage) msg).getReceivedPulsarMessage().getTopicName());
              received.add(msg);
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());

            verifyOrder(received);
          }

          Queue fromTheBeginning = session.createQueue(topicName + ":newSubscription");
          try (QueueBrowser browserAll = session.createBrowser(fromTheBeginning); ) {
            int count = 0;
            Enumeration enumeration = browserAll.getEnumeration();
            while (enumeration.hasMoreElements()) {
              enumeration.nextElement();
              count++;
            }
            assertEquals(numMessages, count);
          }

          try (QueueBrowser browserAll =
              session.createBrowser(fromTheBeginning, "JMSPriority = " + LOW_PRIORITY); ) {
            int count = 0;
            Enumeration enumeration = browserAll.getEnumeration();
            while (enumeration.hasMoreElements()) {
              enumeration.nextElement();
              count++;
            }
            assertEquals(numMessages / 2, count);
          }
        }
      }
    }
  }

  /**
   * Build a huge backlog (around 1.000.000 messages) of low priority messages.
   *
   * @throws Exception
   */
  @ParameterizedTest(name = "mapping {0}")
  @ValueSource(strings = {"linear", "non-linear"})
  public void basicPriorityBigBacklogTest(String mapping) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.enableJMSPriority", true);
    properties.put("jms.priorityMapping", mapping);
    properties.put(
        "producerConfig", ImmutableMap.of("blockIfQueueFull", true, "batchingEnabled", false));
    properties.put("consumerConfig", ImmutableMap.of("receiverQueueSize", 10));
    log.info("running basicPriorityBigBacklogTest with {}", properties);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        assertTrue(factory.isEnableJMSPriority());
        assertEquals(mapping.equals("linear"), factory.isPriorityUseLinearMapping());
        connection.start();
        try (Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE); ) {
          Queue destination = session.createQueue("test-" + UUID.randomUUID());

          pulsarContainer
              .getAdmin()
              .topics()
              .createPartitionedTopic(factory.getPulsarTopicName(destination), 10);

          int numHighPriority = 100;
          int numMessages = 1_000_000;
          try (MessageProducer producer = session.createProducer(destination); ) {
            List<CompletableFuture<?>> handles = new ArrayList<>();
            for (int i = 0; i < numMessages; i++) {
              TextMessage textMessage = session.createTextMessage("foo-" + i);
              if (i < numMessages - numHighPriority) {
                // the first messages are lower priority
                producer.setPriority(LOW_PRIORITY);
              } else {
                producer.setPriority(HIGH_PRIORITY);
              }
              CompletableFuture<?> handle = new CompletableFuture<>();
              producer.send(
                  textMessage,
                  new CompletionListener() {
                    @Override
                    public void onCompletion(Message message) {
                      handle.complete(null);
                    }

                    @Override
                    public void onException(Message message, Exception e) {
                      handle.completeExceptionally(e);
                    }
                  });
              handles.add(handle);
              if (handles.size() == 2000) {
                FutureUtil.waitForAll(handles).get();
                handles.clear();
              }
            }
            FutureUtil.waitForAll(handles).get();
          }

          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {
            int countLowPriority = 0;
            int countHighPriority = 0;
            Set<String> receivedTexts = new HashSet<>();
            List<Integer> received = new ArrayList<>();
            for (int i = 0; i < numMessages; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();

              received.add(msg.getJMSPriority());
              if (msg.getJMSPriority() == LOW_PRIORITY) {
                countLowPriority++;
              }
              if (msg.getJMSPriority() == HIGH_PRIORITY) {
                countHighPriority++;
              }
              if (!receivedTexts.add(msg.getText())) {
                String topicName = factory.getPulsarTopicName(destination);
                PartitionedTopicStats partitionedStats =
                    pulsarContainer.getAdmin().topics().getPartitionedStats(topicName, true);
                log.info("topicName {}", topicName);
                log.info(
                    "stats {}",
                    ObjectMapperFactory.getThreadLocal().writeValueAsString(partitionedStats));
                for (int j = 0; j < 10; j++) {
                  String partition = topicName + "-partition-" + j;
                  PersistentTopicInternalStats internalStats =
                      pulsarContainer.getAdmin().topics().getInternalStats(partition);

                  log.info("partition {}", partition);
                  log.info(
                      "stats {}",
                      ObjectMapperFactory.getThreadLocal().writeValueAsString(internalStats));
                }
                fail(
                    "received message "
                        + msg.getText()
                        + " twice,"
                        + " priority is "
                        + msg.getJMSPriority()
                        + " countLowPriority="
                        + countLowPriority
                        + " countHighPriority="
                        + countHighPriority);
              }
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());

            assertEquals(numMessages - numHighPriority, countLowPriority);
            assertEquals(numHighPriority, countHighPriority);
            verifyPriorities(received);
          }
        }
      }
    }
  }

  private static void verifyOrder(List<TextMessage> received) throws JMSException {
    verifyPriorities(
        received
            .stream()
            .map(m -> Utils.noException(() -> m.getJMSPriority()))
            .collect(Collectors.toList()));
  }

  private static void verifyPriorities(List<Integer> received) throws JMSException {
    // verify that some higher priority messages arrived before the low priority messages
    // please remember that we sent all the low priority messages and then the high priority ones
    // so if we find some low priority message before the high priority messages
    // it means that the priority has been takes into account
    // we cannot make a stricter check because it is possible that the broker
    // was able to dispatch some low priority messages before the high priority
    // this happens because the topics are independent from each other
    boolean foundHighPriority = false;
    boolean foundLowPriorityAfterHighPriority = false;
    int count = 0;
    for (int priority : received) {

      if (priority == LOW_PRIORITY && foundHighPriority) {
        log.info("received priority {} after {}", priority, count);
        foundLowPriorityAfterHighPriority = true;
        break;
      }
      if (priority == HIGH_PRIORITY) {
        log.info("received priority {} after {}", priority, count);
        foundHighPriority = true;
      }
      count++;
    }
    assertTrue(foundLowPriorityAfterHighPriority);
  }

  @Test
  public void basicPriorityMultiTopicTest() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.enableJMSPriority", true);
    properties.put("jms.systemNamespace", SYSTEM_NAMESPACE_OVERRIDDEN);
    properties.put("consumerConfig", ImmutableMap.of("receiverQueueSize", 10));
    properties.put(
        "producerConfig", ImmutableMap.of("blockIfQueueFull", true, "batchingEnabled", false));
    String nameTopic1 = "test-topic1-" + UUID.randomUUID();
    String nameTopic2 = "test-topic2-" + UUID.randomUUID();

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Queue destination1 =
              session.createQueue("persistent://" + SYSTEM_NAMESPACE_OVERRIDDEN + "/" + nameTopic1);
          Queue destination2 =
              session.createQueue("persistent://" + SYSTEM_NAMESPACE_OVERRIDDEN + "/" + nameTopic2);

          int numMessages = 100;
          try (MessageProducer producer1 = session.createProducer(destination1);
              MessageProducer producer2 = session.createProducer(destination2); ) {
            for (int i = 0; i < numMessages; i++) {
              TextMessage textMessage = session.createTextMessage("foo-" + i);
              if (i < numMessages / 2) {
                // the first messages are lower priority
                producer1.setPriority(LOW_PRIORITY);
                producer2.setPriority(LOW_PRIORITY);
              } else {
                producer1.setPriority(HIGH_PRIORITY);
                producer2.setPriority(HIGH_PRIORITY);
              }
              if (i % 2 == 0) {
                log.info("send1 {} prio {}", textMessage.getText(), producer1.getPriority());
                producer1.send(textMessage);
              } else {
                log.info("send2 {} prio {}", textMessage.getText(), producer2.getPriority());
                producer2.send(textMessage);
              }
            }
          }

          Queue destinationFullyQualified =
              session.createQueue(
                  "multi:"
                      + "persistent://"
                      + SYSTEM_NAMESPACE_OVERRIDDEN
                      + "/"
                      + nameTopic1
                      + ","
                      + "persistent://"
                      + SYSTEM_NAMESPACE_OVERRIDDEN
                      + "/"
                      + nameTopic2);
          testMultiTopicConsumer(session, numMessages, destinationFullyQualified);

          Queue destinationOnlyNamesInSystemNamespaceAndCustomSubscription =
              session.createQueue("multi:" + nameTopic1 + "," + nameTopic2 + ":customsubscription");
          testMultiTopicConsumer(
              session, numMessages, destinationOnlyNamesInSystemNamespaceAndCustomSubscription);
        }
      }
    }
  }

  private static void testMultiTopicConsumer(Session session, int numMessages, Queue destination)
      throws JMSException, InterruptedException {
    try (MessageConsumer consumer1 = session.createConsumer(destination); ) {

      // wait for the broker to push the messages to the client
      // the client reorders in memory the messages
      Thread.sleep(2000);

      List<TextMessage> received = new ArrayList<>();
      for (int i = 0; i < numMessages; i++) {
        TextMessage msg = (TextMessage) consumer1.receive();
        log.info(
            "got msg {} prio {} from {} actually {}",
            msg.getText(),
            msg.getJMSPriority(),
            msg.getJMSDestination(),
            ((PulsarMessage) msg).getReceivedPulsarMessage().getTopicName());
        received.add(msg);
      }

      // no more messages
      assertNull(consumer1.receiveNoWait());
      verifyOrder(received);
    }
  }

  @Test
  public void basicPriorityJMSContextTest() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.enableJMSPriority", true);
    properties.put("consumerConfig", ImmutableMap.of("receiverQueueSize", 10));
    properties.put(
        "producerConfig", ImmutableMap.of("blockIfQueueFull", true, "batchingEnabled", false));

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext()) {

        Queue destination =
            context.createQueue("persistent://public/default/test-" + UUID.randomUUID());

        int numMessages = 100;

        for (int i = 0; i < numMessages; i++) {
          JMSProducer producer = context.createProducer();
          if (i < numMessages / 2) {
            // the first messages are lower priority
            producer.setPriority(LOW_PRIORITY);
          } else {
            producer.setPriority(HIGH_PRIORITY);
          }
          String text = "foo-" + i;
          log.info("send {} prio {}", text, producer.getPriority());
          producer.send(destination, text);
        }

        try (JMSConsumer consumer1 = context.createConsumer(destination); ) {

          // wait for the broker to push the messages to the client
          // the client reorders in memory the messages
          Thread.sleep(2000);

          List<TextMessage> received = new ArrayList<>();
          for (int i = 0; i < numMessages; i++) {
            TextMessage msg = (TextMessage) consumer1.receive();
            log.info("got msg {} prio {}", msg.getText(), msg.getJMSPriority());
            received.add(msg);
          }

          // no more messages
          assertNull(consumer1.receiveNoWait());
          verifyOrder(received);
        }
      }
    }
  }
}
