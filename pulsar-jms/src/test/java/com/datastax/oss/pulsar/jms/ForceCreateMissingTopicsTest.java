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

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
public class ForceCreateMissingTopicsTest {

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  @BeforeAll
  public static void before() throws Exception {
    cluster =
        new PulsarCluster(
            tempDir,
            config -> {
              config.setTransactionCoordinatorEnabled(false);
              config.setAllowAutoTopicCreation(false);
            });
    cluster.start();
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 4})
  public void forceCreateMissingTopicsForProducerJmsPrioritySideTopics(int numPartitions)
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.forceCreateMissingTopics", true);
    properties.put("jms.forceCreateMissingTopicsPartitions", numPartitions);
    properties.put("jms.emulateJMSPriority", true);

    String topic = "persistent://public/default/test-" + UUID.randomUUID();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(Session.CLIENT_ACKNOWLEDGE); ) {
          PulsarDestination destination = session.createQueue(topic);

          try (MessageProducer producer = session.createProducer(destination); ) {
            producer.setPriority(9);
            try {
              producer.send(session.createTextMessage("foo"));
            } catch (InvalidDestinationException expected) {
              assertEquals("Reference topic does not exist " + topic, expected.getMessage());
            }
            cluster.getService().getAdminClient().topics().createNonPartitionedTopic(topic);
            producer.send(session.createTextMessage("foo"));
          }
          try (MessageConsumer consumer1 = session.createConsumer(destination)) {

            Message message = consumer1.receive();
            assertEquals("foo", message.getBody(String.class));
          }
          String pulsarTopicName = factory.getPulsarTopicName(destination, 9);
          PartitionedTopicMetadata partitionedTopicMetadata =
              cluster
                  .getService()
                  .getAdminClient()
                  .topics()
                  .getPartitionedTopicMetadata(pulsarTopicName);
          assertEquals(numPartitions, partitionedTopicMetadata.partitions);
        }
      }
    }
  }

  private static Stream<Arguments> forceCreateMissingTopicsForDLQSideTopicsCombinations() {
    return Stream.of(
        Arguments.of(0, 0, false),
        Arguments.of(4, 0, false),
        Arguments.of(4, 4, false),
        Arguments.of(0, 0, true),
        Arguments.of(4, 0, true),
        Arguments.of(4, 4, true));
  }

  @ParameterizedTest(
    name = "numPartitionsMainTopic {0} numPartitionsDLQTopic {1} forceDeadLetterTopicName {2}"
  )
  @MethodSource("forceCreateMissingTopicsForDLQSideTopicsCombinations")
  public void forceCreateMissingTopicsForDLQSideTopics(
      int numPartitionsMainTopic, int numPartitionsDLQTopic, boolean forceDeadLetterTopicName)
      throws Exception {

    // in case of forceDeadLetterTopicName the JMS Client will create a topic with
    // a number of partitions equals to numPartitionsDLQTopic
    // (jms.forceCreateMissingTopicsPartitions)
    // in case of automatic deadlettertopic name the JMS client always created non-partitioned
    // topics
    // in case of partitioned topics the name is computed per each partition (it is not a
    // partitioned topic)

    // basic test
    String topic = "persistent://public/default/test-" + UUID.randomUUID();
    if (numPartitionsMainTopic == 0) {
      cluster.getService().getAdminClient().topics().createNonPartitionedTopic(topic);
    } else {
      cluster
          .getService()
          .getAdminClient()
          .topics()
          .createPartitionedTopic(topic, numPartitionsMainTopic);
    }
    // this name is automatic, but you can configure it
    // https://pulsar.apache.org/docs/concepts-messaging/#dead-letter-topic
    String queueSubscriptionName = "thesub";

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.queueSubscriptionName", queueSubscriptionName);

    Map<String, Object> consumerConfig = new HashMap<>();
    properties.put("consumerConfig", consumerConfig);

    consumerConfig.put("ackTimeoutMillis", 1000);

    Map<String, Object> deadLetterPolicy = new HashMap<>();
    consumerConfig.put("deadLetterPolicy", deadLetterPolicy);
    String forcedDeadLetterTopic = null;
    if (forceDeadLetterTopicName) {
      forcedDeadLetterTopic = "persistent://public/default/test-dlq-" + UUID.randomUUID();
      deadLetterPolicy.put("deadLetterTopic", forcedDeadLetterTopic);
    }
    deadLetterPolicy.put("maxRedeliverCount", 1);
    // this is the name of a subscription that is created
    // while creating the producer to the DQL topic
    deadLetterPolicy.put("initialSubscriptionName", "dqlsub");

    properties.put("jms.forceCreateMissingTopics", true);
    properties.put("jms.forceCreateMissingTopicsPartitions", numPartitionsDLQTopic);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
            Session session2 = connection.createSession(Session.AUTO_ACKNOWLEDGE); ) {
          Destination destination = session.createQueue(topic);

          try (MessageConsumer consumer1 = session.createConsumer(destination)) {

            try (MessageProducer producer = session2.createProducer(destination); ) {
              producer.send(session.createTextMessage("foo"));
            }

            // calling consumer.receive() creates the internal Pulsar Consumer
            // and after creating the Consumer we ensure that the DLQ topic exists
            Message message = consumer1.receive();
            assertEquals("foo", message.getBody(String.class));
            assertEquals(1, message.getIntProperty("JMSXDeliveryCount"));
            assertFalse(message.getJMSRedelivered());

            final Topic destinationDeadletter;
            if (forcedDeadLetterTopic != null) {
              destinationDeadletter = session.createTopic(forcedDeadLetterTopic);
            } else if (numPartitionsMainTopic > 0) {
              String deadLetterTopic =
                  "regex:" + topic + "-partition-.*-" + queueSubscriptionName + "-DLQ";
              destinationDeadletter = session.createTopic(deadLetterTopic);
            } else {
              String deadLetterTopic = topic + "-" + queueSubscriptionName + "-DLQ";
              destinationDeadletter = session.createTopic(deadLetterTopic);
            }

            try (MessageConsumer consumerDeadLetter =
                session2.createSharedConsumer(destinationDeadletter, "dqlsub"); ) {
              // message is re-delivered again after ackTimeoutMillis
              message = consumer1.receive();
              assertEquals("foo", message.getBody(String.class));
              assertEquals(2, message.getIntProperty("JMSXDeliveryCount"));
              assertTrue(message.getJMSRedelivered());

              message = consumerDeadLetter.receive();
              assertEquals("foo", message.getBody(String.class));
              log.info("DLQ MESSAGE {}", message);
              // this is another topic, and the JMSXDeliveryCount is only handled on the client side
              // so the count is back to 1
              assertEquals(1, message.getIntProperty("JMSXDeliveryCount"));
              assertFalse(message.getJMSRedelivered());
            }

            if (forcedDeadLetterTopic != null) {
              // if the name is fixed the topic is created with the configured number of partitions
              PartitionedTopicMetadata partitionedTopicMetadata =
                  cluster
                      .getService()
                      .getAdminClient()
                      .topics()
                      .getPartitionedTopicMetadata(forcedDeadLetterTopic);
              assertEquals(numPartitionsDLQTopic, partitionedTopicMetadata.partitions);
            }
          }
        }
      }
    }
  }
}
