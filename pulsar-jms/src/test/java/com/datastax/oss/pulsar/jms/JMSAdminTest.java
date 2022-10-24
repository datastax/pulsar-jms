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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.api.JMSAdmin;
import com.datastax.oss.pulsar.jms.api.JMSDestinationMetadata;
import com.datastax.oss.pulsar.jms.messages.PulsarTextMessage;
import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.jms.InvalidDestinationException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class JMSAdminTest {

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  @BeforeAll
  public static void before() throws Exception {
    cluster =
        new PulsarCluster(
            tempDir,
            c -> {
              c.setAllowAutoTopicCreation(false);
            });
    cluster.start();
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @ParameterizedTest(name = "numPartitions {0}")
  @ValueSource(ints = {0, 4})
  public void adminApiForQueues(int numPartitions) throws Exception {
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("producerName", "the-name");
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("producerConfig", producerConfig);
    String topic = "test-" + UUID.randomUUID();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
        PulsarConnection connection = factory.createConnection(); ) {
      JMSAdmin admin = factory.getAdmin();

      final Queue destination = admin.getQueue(topic);
      JMSDestinationMetadata.QueueMetadata description =
          admin.describe(destination).unwrap(JMSDestinationMetadata.QueueMetadata.class);
      assertEquals(topic, description.getDestination());
      assertEquals("jms-queue", description.getQueueSubscription());
      assertEquals("persistent://public/default/" + topic, description.getPulsarTopic());
      assertFalse(description.isExists());
      assertTrue(description.isQueue());
      assertFalse(description.isTopic());
      assertFalse(description.isVirtualDestination());
      assertFalse(description.isQueueSubscriptionExists());
      assertFalse(description.isPartitioned());
      assertEquals(0, description.getPartitions());
      assertTrue(description.getProducers().isEmpty());
      assertNull(description.getSubscriptionMetadata());

      admin.createQueue(destination, numPartitions, false, null);

      description = admin.describe(destination).unwrap(JMSDestinationMetadata.QueueMetadata.class);
      assertEquals(topic, description.getDestination());
      assertEquals("persistent://public/default/" + topic, description.getPulsarTopic());
      assertEquals("jms-queue", description.getQueueSubscription());
      assertTrue(description.isExists());
      assertTrue(description.isQueue());
      assertFalse(description.isTopic());
      assertFalse(description.isVirtualDestination());
      assertTrue(description.isQueueSubscriptionExists());
      assertEquals(numPartitions != 0, description.isPartitioned());
      assertEquals(numPartitions, description.getPartitions());
      assertTrue(description.getProducers().isEmpty());
      assertNotNull(description.getSubscriptionMetadata());

      // try to create a topic of different type...fail
      assertThrows(
          InvalidDestinationException.class,
          () -> {
            int otherNumPartitions = numPartitions > 0 ? 0 : 5;
            admin.createQueue(destination, otherNumPartitions, false, null);
          });

      // try to create another subscription on the same topic
      Queue sameTopicNewSubscription = admin.getQueue(topic + ":mysub");
      admin.createQueue(sameTopicNewSubscription, numPartitions, true, "foo='bar'");

      JMSDestinationMetadata.QueueMetadata descriptionMySub =
          admin
              .describe(sameTopicNewSubscription)
              .unwrap(JMSDestinationMetadata.QueueMetadata.class);
      assertEquals(topic + ":mysub", descriptionMySub.getDestination());
      assertEquals("persistent://public/default/" + topic, description.getPulsarTopic());
      assertEquals("mysub", descriptionMySub.getQueueSubscription());
      assertTrue(descriptionMySub.isExists());
      assertTrue(descriptionMySub.isQueue());
      assertFalse(descriptionMySub.isTopic());
      assertFalse(descriptionMySub.isVirtualDestination());
      assertTrue(descriptionMySub.isQueueSubscriptionExists());
      assertEquals(numPartitions != 0, description.isPartitioned());
      assertEquals(numPartitions, description.getPartitions());
      assertTrue(descriptionMySub.getProducers().isEmpty());
      assertNotNull(descriptionMySub.getSubscriptionMetadata());

      // try to create a new subscription but with topic of other kind...fail

      assertThrows(
          InvalidDestinationException.class,
          () -> {
            Queue sameTopicNewSubscription2 = admin.getQueue(topic + ":mysub2");
            int otherNumPartitions = numPartitions > 0 ? 0 : 5;
            admin.createQueue(sameTopicNewSubscription2, otherNumPartitions, false, null);
          });

      // use virtual multi-topic destination

      Queue destinationAsMultiTopic = admin.getQueue("multi:" + topic);
      // this is a single destination, so multitopic will be 'simplified' to a single non virtual
      // destination
      JMSDestinationMetadata.QueueMetadata queueDescription =
          admin
              .describe(destinationAsMultiTopic)
              .unwrap(JMSDestinationMetadata.QueueMetadata.class);
      assertEquals(topic, queueDescription.getDestination());
      assertEquals("jms-queue", queueDescription.getQueueSubscription());
      assertEquals("persistent://public/default/" + topic, queueDescription.getPulsarTopic());
      assertTrue(queueDescription.isExists());
      assertTrue(queueDescription.isQueue());
      assertFalse(queueDescription.isTopic());
      assertFalse(queueDescription.isVirtualDestination());
      assertTrue(queueDescription.isQueueSubscriptionExists());
      assertEquals(numPartitions > 0, queueDescription.isPartitioned());
      assertEquals(numPartitions, queueDescription.getPartitions());
      assertTrue(queueDescription.getProducers().isEmpty());
      assertNotNull(queueDescription.getSubscriptionMetadata());

      destinationAsMultiTopic = admin.getQueue("multi:" + topic + ",topic");

      JMSDestinationMetadata.VirtualDestinationMetadata descriptionVD =
          admin
              .describe(destinationAsMultiTopic)
              .unwrap(JMSDestinationMetadata.VirtualDestinationMetadata.class);
      assertEquals(2, descriptionVD.getDestinations().size());
      assertEquals("multi:" + topic + ",topic", descriptionVD.getDestination());
      assertTrue(descriptionVD.isQueue());
      assertTrue(descriptionVD.isVirtualDestination());
      assertTrue(descriptionVD.isMultiTopic());
      assertFalse(descriptionVD.isTopic());

      queueDescription =
          descriptionVD.getDestinations().get(0).unwrap(JMSDestinationMetadata.QueueMetadata.class);
      assertEquals(topic, queueDescription.getDestination());
      assertEquals("jms-queue", queueDescription.getQueueSubscription());
      assertEquals("persistent://public/default/" + topic, queueDescription.getPulsarTopic());
      assertTrue(queueDescription.isExists());
      assertTrue(queueDescription.isQueue());
      assertFalse(queueDescription.isTopic());
      assertFalse(queueDescription.isVirtualDestination());
      assertTrue(queueDescription.isQueueSubscriptionExists());
      assertEquals(numPartitions > 0, queueDescription.isPartitioned());
      assertEquals(numPartitions, queueDescription.getPartitions());
      assertTrue(queueDescription.getProducers().isEmpty());
      assertNotNull(queueDescription.getSubscriptionMetadata());

      JMSDestinationMetadata.QueueMetadata queueDescription2 =
          descriptionVD.getDestinations().get(1).unwrap(JMSDestinationMetadata.QueueMetadata.class);
      assertEquals("topic", queueDescription2.getDestination());
      assertEquals("jms-queue", queueDescription2.getQueueSubscription());
      assertEquals("persistent://public/default/topic", queueDescription2.getPulsarTopic());
      assertFalse(queueDescription2.isExists());
      assertTrue(queueDescription2.isQueue());
      assertFalse(queueDescription2.isTopic());
      assertFalse(queueDescription2.isVirtualDestination());
      assertFalse(queueDescription2.isQueueSubscriptionExists());
      assertFalse(queueDescription2.isPartitioned());
      assertEquals(0, queueDescription2.getPartitions());
      assertTrue(queueDescription2.getProducers().isEmpty());
      assertNull(queueDescription2.getSubscriptionMetadata());
    }
  }

  @ParameterizedTest(name = "numPartitions {0}")
  @ValueSource(ints = {0, 4})
  public void adminApiForTopic(int numPartitions) throws Exception {
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("producerName", "the-name");
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("producerConfig", producerConfig);
    String topic = "test-" + UUID.randomUUID();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
        PulsarConnection connection = factory.createConnection(); ) {
      JMSAdmin admin = factory.getAdmin();

      final Topic destination = admin.getTopic(topic);
      JMSDestinationMetadata.TopicMetadata description =
          admin.describe(destination).unwrap(JMSDestinationMetadata.TopicMetadata.class);
      assertEquals(topic, description.getDestination());
      assertEquals("persistent://public/default/" + topic, description.getPulsarTopic());
      assertFalse(description.isExists());
      assertFalse(description.isQueue());
      assertTrue(description.isTopic());
      assertFalse(description.isVirtualDestination());
      assertFalse(description.isPartitioned());
      assertEquals(0, description.getPartitions());
      assertTrue(description.getProducers().isEmpty());
      assertEquals(0, description.getSubscriptions().size());

      admin.createTopic(destination, numPartitions);

      description = admin.describe(destination).unwrap(JMSDestinationMetadata.TopicMetadata.class);
      assertEquals(topic, description.getDestination());
      assertEquals("persistent://public/default/" + topic, description.getPulsarTopic());
      assertTrue(description.isExists());
      assertFalse(description.isQueue());
      assertTrue(description.isTopic());
      assertFalse(description.isVirtualDestination());
      assertEquals(numPartitions != 0, description.isPartitioned());
      assertEquals(numPartitions, description.getPartitions());
      assertTrue(description.getProducers().isEmpty());
      assertEquals(0, description.getSubscriptions().size());

      // try to create a topic of different type...fail
      assertThrows(
          InvalidDestinationException.class,
          () -> {
            int otherNumPartitions = numPartitions > 0 ? 0 : 5;
            admin.createTopic(destination, otherNumPartitions);
          });

      admin.createSubscription(destination, "sub1", true, "foo is null", false);
      description = admin.describe(destination).unwrap(JMSDestinationMetadata.TopicMetadata.class);
      assertEquals(topic, description.getDestination());
      assertEquals("persistent://public/default/" + topic, description.getPulsarTopic());
      assertTrue(description.isExists());
      assertFalse(description.isQueue());
      assertTrue(description.isTopic());
      assertFalse(description.isVirtualDestination());
      assertEquals(numPartitions != 0, description.isPartitioned());
      assertEquals(numPartitions, description.getPartitions());
      assertTrue(description.getProducers().isEmpty());
      assertEquals(1, description.getSubscriptions().size());
      JMSDestinationMetadata.SubscriptionMetadata sub1Metadata =
          description.getSubscriptions().get(0);
      assertEquals("sub1", sub1Metadata.getSubscriptionName());
      assertEquals("true", sub1Metadata.getSubscriptionProperties().get("jms.filtering"));
      assertEquals("foo is null", sub1Metadata.getSubscriptionProperties().get("jms.selector"));
      assertTrue(sub1Metadata.isEnableFilters());
      assertEquals("foo is null", sub1Metadata.getSelector());
      assertTrue(sub1Metadata.getConsumers().isEmpty());

      // try to create another subscription on the same topic
      admin.createSubscription(destination, "sub2", false, "foo='IGNORED'", true);
      description = admin.describe(destination).unwrap(JMSDestinationMetadata.TopicMetadata.class);

      assertEquals(2, description.getSubscriptions().size());
      sub1Metadata =
          description
              .getSubscriptions()
              .stream()
              .filter(s -> s.getSubscriptionName().equals("sub1"))
              .findFirst()
              .get();
      assertEquals("sub1", sub1Metadata.getSubscriptionName());
      assertEquals("true", sub1Metadata.getSubscriptionProperties().get("jms.filtering"));
      assertEquals("foo is null", sub1Metadata.getSubscriptionProperties().get("jms.selector"));
      assertTrue(sub1Metadata.isEnableFilters());
      assertEquals("foo is null", sub1Metadata.getSelector());
      assertTrue(sub1Metadata.getConsumers().isEmpty());

      JMSDestinationMetadata.SubscriptionMetadata sub2Metadata =
          description
              .getSubscriptions()
              .stream()
              .filter(s -> s.getSubscriptionName().equals("sub2"))
              .findFirst()
              .get();
      assertEquals("sub2", sub2Metadata.getSubscriptionName());
      assertTrue(sub2Metadata.getSubscriptionProperties().isEmpty());
      assertFalse(sub2Metadata.isEnableFilters());
      assertNull(sub2Metadata.getSelector());
      assertTrue(sub2Metadata.getConsumers().isEmpty());

      // use virtual multi-topic destination

      Topic destinationAsMultiTopic = admin.getTopic("multi:" + topic);
      // this is a single destination, so multitopic will be 'simplified' to a single non virtual
      // destination
      JMSDestinationMetadata.TopicMetadata topicDescription =
          admin
              .describe(destinationAsMultiTopic)
              .unwrap(JMSDestinationMetadata.TopicMetadata.class);
      assertEquals(topic, topicDescription.getDestination());
      assertEquals("persistent://public/default/" + topic, topicDescription.getPulsarTopic());
      assertTrue(topicDescription.isExists());
      assertFalse(topicDescription.isQueue());
      assertTrue(topicDescription.getProducers().isEmpty());
      assertFalse(topicDescription.getSubscriptions().isEmpty());

      destinationAsMultiTopic = admin.getTopic("multi:" + topic + ",topic");

      JMSDestinationMetadata.VirtualDestinationMetadata descriptionVD =
          admin
              .describe(destinationAsMultiTopic)
              .unwrap(JMSDestinationMetadata.VirtualDestinationMetadata.class);
      assertEquals(2, descriptionVD.getDestinations().size());
      assertEquals("multi:" + topic + ",topic", descriptionVD.getDestination());
      assertFalse(descriptionVD.isQueue());
      assertTrue(descriptionVD.isVirtualDestination());
      assertTrue(descriptionVD.isMultiTopic());
      assertTrue(descriptionVD.isTopic());

      topicDescription =
          descriptionVD.getDestinations().get(0).unwrap(JMSDestinationMetadata.TopicMetadata.class);
      assertEquals(topic, topicDescription.getDestination());
      assertEquals("persistent://public/default/" + topic, topicDescription.getPulsarTopic());
      assertTrue(topicDescription.isExists());
      assertFalse(topicDescription.isQueue());
      assertTrue(topicDescription.isTopic());
      assertFalse(topicDescription.isVirtualDestination());
      assertEquals(numPartitions > 0, topicDescription.isPartitioned());
      assertEquals(numPartitions, topicDescription.getPartitions());
      assertTrue(topicDescription.getProducers().isEmpty());

      JMSDestinationMetadata.TopicMetadata topicDescription2 =
          descriptionVD.getDestinations().get(1).unwrap(JMSDestinationMetadata.TopicMetadata.class);
      assertEquals("topic", topicDescription2.getDestination());
      assertEquals("persistent://public/default/topic", topicDescription2.getPulsarTopic());
      assertFalse(topicDescription2.isExists());
      assertFalse(topicDescription2.isQueue());
      assertTrue(topicDescription2.isTopic());
      assertFalse(topicDescription2.isVirtualDestination());
      assertFalse(topicDescription2.isPartitioned());
      assertEquals(0, topicDescription2.getPartitions());
      assertTrue(topicDescription2.getProducers().isEmpty());
    }
  }

  @ParameterizedTest(name = "numPartitions {0}")
  @ValueSource(ints = {0, 4})
  public void describeProducers(int numPartitions) throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    String topic = "test-" + UUID.randomUUID();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      JMSAdmin admin = factory.getAdmin();

      final Queue destination = admin.getQueue(topic);

      admin.createQueue(destination, numPartitions, false, null);

      try (PulsarConnectionFactory factoryNoPrio =
              new PulsarConnectionFactory(buildProducerProperties(false, null));
          PulsarSession sessionNoPrio = factoryNoPrio.createConnection().createSession();
          MessageProducer producerNoPrio = sessionNoPrio.createProducer(destination);
          PulsarSession sessionTransacted =
              factoryNoPrio.createConnection().createSession(Session.SESSION_TRANSACTED);
          MessageProducer producerTransacted = sessionTransacted.createProducer(destination);
          PulsarConnectionFactory factoryPrio =
              new PulsarConnectionFactory(buildProducerProperties(true, "linear"));
          PulsarSession sessionPrio = factoryPrio.createConnection().createSession();
          MessageProducer producerPrio = sessionPrio.createProducer(destination);
          PulsarConnectionFactory factoryPrioNonLinear =
              new PulsarConnectionFactory(buildProducerProperties(true, "non-linear"));
          PulsarSession sessionPrioNonLinear =
              factoryPrioNonLinear.createConnection().createSession();
          MessageProducer producerPrioNonLinear =
              sessionPrioNonLinear.createProducer(destination)) {

        producerNoPrio.send(new PulsarTextMessage("foo"));
        producerPrio.send(new PulsarTextMessage("foo"));
        producerPrioNonLinear.send(new PulsarTextMessage("foo"));
        producerTransacted.send(new PulsarTextMessage("foo"));

        JMSDestinationMetadata.QueueMetadata description =
            admin.describe(destination).unwrap(JMSDestinationMetadata.QueueMetadata.class);
        assertTrue(description.isExists());
        assertTrue(description.isQueue());
        assertFalse(description.isTopic());
        assertFalse(description.isVirtualDestination());
        assertTrue(description.isQueueSubscriptionExists());
        assertEquals(numPartitions != 0, description.isPartitioned());
        assertEquals(numPartitions, description.getPartitions());
        assertFalse(description.getProducers().isEmpty());
        assertNotNull(description.getSubscriptionMetadata());
        verifyProducersMetadata(numPartitions, description);

        Queue destinationRegExp = admin.getQueue("regex:" + topic);
        JMSDestinationMetadata.QueueMetadata descriptionFromRegExp =
            admin
                .describe(destinationRegExp)
                .unwrap(JMSDestinationMetadata.VirtualDestinationMetadata.class)
                .getDestinations()
                .get(0)
                .unwrap(JMSDestinationMetadata.QueueMetadata.class);
        verifyProducersMetadata(numPartitions, descriptionFromRegExp);
      }
    }
  }

  private static void verifyProducersMetadata(
      int numPartitions, JMSDestinationMetadata.QueueMetadata description) {

    assertTrue(description.getProducers().stream().anyMatch(s -> s.isTransacted()));

    assertTrue(
        description
            .getProducers()
            .stream()
            .anyMatch(s -> !s.isEnablePriority() && "".equals(s.getPriorityMapping())));

    assertTrue(
        description
            .getProducers()
            .stream()
            .anyMatch(s -> s.isEnablePriority() && "linear".equals(s.getPriorityMapping())));

    assertTrue(
        description
            .getProducers()
            .stream()
            .anyMatch(s -> s.isEnablePriority() && "non-linear".equals(s.getPriorityMapping())));

    description.getProducers().stream().allMatch(s -> s.getProducerName() != null);

    description
        .getProducers()
        .stream()
        .allMatch(s -> s.getClientVersion() != null && s.getAddress() != null);
  }

  @ParameterizedTest(name = "numPartitions {0}")
  @ValueSource(ints = {0, 4})
  public void describeConsumers(int numPartitions) throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    String topic = "test-" + UUID.randomUUID();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      JMSAdmin admin = factory.getAdmin();

      final Queue destination = admin.getQueue(topic);

      admin.createQueue(destination, numPartitions, false, null);

      try (PulsarConnectionFactory factoryNoPrio =
              new PulsarConnectionFactory(buildConsumerProperties("no-prio", false));
          PulsarSession sessionNoPrio = factoryNoPrio.createConnection().createSession();
          MessageConsumer consumerNoPrio = sessionNoPrio.createConsumer(destination);
          PulsarConnectionFactory factoryPrio =
              new PulsarConnectionFactory(buildConsumerProperties("prio-linear", true));
          PulsarSession sessionPrio = factoryPrio.createConnection().createSession();
          MessageConsumer consumerPrio = sessionPrio.createConsumer(destination); ) {

        sessionNoPrio.getConnection().start();
        sessionPrio.getConnection().start();
        consumerNoPrio.receiveNoWait();
        consumerPrio.receiveNoWait();

        JMSDestinationMetadata.QueueMetadata description =
            admin.describe(destination).unwrap(JMSDestinationMetadata.QueueMetadata.class);
        assertTrue(description.isExists());
        assertTrue(description.isQueue());
        assertFalse(description.isTopic());
        assertFalse(description.isVirtualDestination());
        assertTrue(description.isQueueSubscriptionExists());
        assertEquals(numPartitions != 0, description.isPartitioned());
        assertEquals(numPartitions, description.getPartitions());
        assertNotNull(description.getSubscriptionMetadata());
        verifyConsumersMetadata(numPartitions, description);

        Queue destinationRegExp = admin.getQueue("regex:" + topic);
        JMSDestinationMetadata.QueueMetadata descriptionFromRegExp =
            admin
                .describe(destinationRegExp)
                .unwrap(JMSDestinationMetadata.VirtualDestinationMetadata.class)
                .getDestinations()
                .get(0)
                .unwrap(JMSDestinationMetadata.QueueMetadata.class);
        verifyConsumersMetadata(numPartitions, descriptionFromRegExp);
      }
    }
  }

  private static void verifyConsumersMetadata(
      int numPartitions, JMSDestinationMetadata.QueueMetadata description) {

    assertFalse(description.getSubscriptionMetadata().getConsumers().isEmpty());

    assertTrue(
        description
            .getSubscriptionMetadata()
            .getConsumers()
            .stream()
            .anyMatch(s -> !s.isEnablePriority()));

    assertTrue(
        description
            .getSubscriptionMetadata()
            .getConsumers()
            .stream()
            .anyMatch(s -> s.isEnablePriority()));

    assertTrue(
        description
            .getSubscriptionMetadata()
            .getConsumers()
            .stream()
            .allMatch(s -> s.getConsumerName() != null));

    assertTrue(
        description
            .getSubscriptionMetadata()
            .getConsumers()
            .stream()
            .allMatch(
                s ->
                    s.getClientVersion() != null
                        && s.getAddress() != null
                        && s.getAcknowledgeMode() != null
                        && !s.getAcknowledgeMode().equals("?")));
  }

  private static Map<String, Object> buildProducerProperties(
      boolean priority, String priorityMapping) {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableJMSPriority", priority);
    properties.put("enableTransaction", true);
    properties.put("jms.priorityMapping", priorityMapping);
    return properties;
  }

  private static Map<String, Object> buildConsumerProperties(String name, boolean priority) {
    Map<String, Object> consumerConfig = new HashMap<>();
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("consumerConfig", consumerConfig);
    properties.put("jms.enableJMSPriority", priority);
    return properties;
  }
}
