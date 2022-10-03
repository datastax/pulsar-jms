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

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.jms.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Slf4j
public class PriorityTest {

  private static final String SYSTEM_NAMESPACE_OVERRIDDEN = "foo/ns";

  static int LOW_PRIORITY = 4;
  static int HIGH_PRIORITY = 9;

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  @BeforeAll
  public static void before() throws Exception {
    cluster = new PulsarCluster(tempDir);
    cluster.start();

    cluster
        .getService()
        .getAdminClient()
        .tenants()
        .createTenant(
            "foo",
            TenantInfo.builder()
                .allowedClusters(
                    ImmutableSet.of(cluster.getService().getConfiguration().getClusterName()))
                .build());
    cluster.getService().getAdminClient().namespaces().createNamespace(SYSTEM_NAMESPACE_OVERRIDDEN);
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  private static Stream<Arguments> basicCombinations() {
    return Stream.of(Arguments.of(0, true), Arguments.of(4, false), Arguments.of(4, true));
  }

  @ParameterizedTest(name = "numPartitions {0} sideTopicPartitioned {1}")
  @MethodSource("basicCombinations")
  public void basicTest(int numPartitions, boolean sideTopicPartitioned) throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.emulateJMSPriority", true);
    properties.put("jms.systemNamespace", SYSTEM_NAMESPACE_OVERRIDDEN);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Queue destination = session.createQueue("test-" + UUID.randomUUID());

          if (numPartitions > 0) {
            // the main topic is partitioned
            cluster
                .getService()
                .getAdminClient()
                .topics()
                .createPartitionedTopic(factory.getPulsarTopicName(destination), 4);

            if (sideTopicPartitioned) {
              cluster
                  .getService()
                  .getAdminClient()
                  .topics()
                  .createPartitionedTopic(
                      factory.getPulsarTopicName(destination, HIGH_PRIORITY), 4);
            } else {
              cluster
                  .getService()
                  .getAdminClient()
                  .topics()
                  .createNonPartitionedTopic(
                      factory.getPulsarTopicName(destination, HIGH_PRIORITY));
            }
          } else {
            // auto-creation in case of non-partitioned topic
          }

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
        }
      }
    }
  }

  /**
   * Build a huge backlog (around 1.000.000 messages) of low priority messages.
   *
   * @throws Exception
   */
  @Test
  public void basicPriorityBigBacklogTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.emulateJMSPriority", true);
    properties.put("producerConfig", ImmutableMap.of("blockIfQueueFull", true));

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Queue destination = session.createQueue("test-" + UUID.randomUUID());

          int numHighPriority = 100;
          int numMessages = 1_000_000;
          CountDownLatch counter = new CountDownLatch(numMessages);
          try (MessageProducer producer = session.createProducer(destination); ) {
            for (int i = 0; i < numMessages; i++) {
              TextMessage textMessage = session.createTextMessage("foo-" + i);
              if (i < numMessages - numHighPriority) {
                // the first messages are lower priority
                producer.setPriority(LOW_PRIORITY);
              } else {
                producer.setPriority(HIGH_PRIORITY);
              }

              producer.send(
                  textMessage,
                  new CompletionListener() {
                    @Override
                    public void onCompletion(Message message) {
                      counter.countDown();
                    }

                    @Override
                    public void onException(Message message, Exception e) {}
                  });
            }
            assertTrue(counter.await(10, TimeUnit.SECONDS));
          }

          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {
            int countLowPriority = 0;
            int countHighPriority = 0;
            List<TextMessage> received = new ArrayList<>();
            for (int i = 0; i < numMessages; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();

              received.add(msg);
              if (msg.getJMSPriority() == LOW_PRIORITY) {
                countLowPriority++;
              }
              if (msg.getJMSPriority() == HIGH_PRIORITY) {
                countHighPriority++;
              }
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());

            assertEquals(numMessages - numHighPriority, countLowPriority);
            assertEquals(numHighPriority, countHighPriority);
            verifyOrder(received);
          }
        }
      }
    }
  }

  private static void verifyOrder(List<TextMessage> received) throws JMSException {
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
    for (TextMessage msg : received) {
      int priority = msg.getJMSPriority();

      if (priority == LOW_PRIORITY && foundHighPriority) {
        log.info("received {} priority {} after {}", msg.getText(), priority, count);
        foundLowPriorityAfterHighPriority = true;
        break;
      }
      if (priority == HIGH_PRIORITY) {
        log.info("received {} priority {} after {}", msg.getText(), priority, count);
        foundHighPriority = true;
      }
      count++;
    }
    assertTrue(foundLowPriorityAfterHighPriority);
  }

  @Test
  public void basicPriorityMultiTopicTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.emulateJMSPriority", true);
    properties.put("jms.systemNamespace", SYSTEM_NAMESPACE_OVERRIDDEN);
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

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.emulateJMSPriority", true);

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
