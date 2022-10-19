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
import static org.mockito.Mockito.*;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.internal.util.reflection.Whitebox;

@Slf4j
public class BasicServerSideFilterTest {

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  public BasicServerSideFilterTest() {}

  @BeforeAll
  public static void before() throws Exception {
    cluster =
        new PulsarCluster(
            tempDir,
            (config) -> {
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

  static int refreshServerSideFiltersPeriod = 10;

  private Map<String, Object> buildProperties() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());

    properties.put("jms.useServerSideFiltering", "true");
    properties.put("jms.refreshServerSideFiltersPeriod", refreshServerSideFiltersPeriod);
    properties.put("jms.enableClientSideEmulation", "false");

    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batchingEnabled", false);
    properties.put("producerConfig", producerConfig);

    Map<String, Object> consumerConfig = new HashMap<>();
    properties.put("consumerConfig", consumerConfig);
    // batchIndexAckEnabled is required in order for the client to be able to
    // negatively/positively acknowledge single messages inside a batch
    consumerConfig.put("batchIndexAckEnabled", true);
    return properties;
  }

  @ParameterizedTest(name = "numPartitions {0}")
  @ValueSource(ints = {0, 4})
  public void downloadSubscriptionProperties(int numPartitions) throws Exception {

    Map<String, Object> properties = buildProperties();

    String topicName = "topic-with-sub-" + UUID.randomUUID();
    String topicName2 = "topic-with-sub-" + UUID.randomUUID();
    if (numPartitions > 0) {
      cluster
          .getService()
          .getAdminClient()
          .topics()
          .createPartitionedTopic(topicName, numPartitions);

      cluster
          .getService()
          .getAdminClient()
          .namespaces()
          .setAutoTopicCreation(
              "public/default",
              AutoTopicCreationOverride.builder()
                  .defaultNumPartitions(4)
                  .topicType("partitioned")
                  .allowAutoTopicCreation(true)
                  .build());
    } else {
      cluster.getService().getAdminClient().topics().createNonPartitionedTopic(topicName);
      cluster
          .getService()
          .getAdminClient()
          .namespaces()
          .setAutoTopicCreation(
              "public/default",
              AutoTopicCreationOverride.builder()
                  .topicType("non-partitioned")
                  .allowAutoTopicCreation(true)
                  .build());
    }

    String subscriptionName = "the-sub";
    String selector = "keepme = TRUE";

    Map<String, String> subscriptionProperties = new HashMap<>();
    subscriptionProperties.put("jms.selector", selector);
    subscriptionProperties.put("jms.filtering", "true");

    // create a Subscription with a selector
    cluster
        .getService()
        .getAdminClient()
        .topics()
        .createSubscription(
            topicName, subscriptionName, MessageId.earliest, false, subscriptionProperties);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Topic destination = session.createTopic(topicName);
          Topic destination2 = session.createTopic(topicName2);

          // do not set the selector, it will be loaded from the Subscription Properties
          try (PulsarMessageConsumer consumer1 =
              session.createSharedDurableConsumer(destination, subscriptionName, null); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());

            produce(session, destination);
            consume(consumer1);

            // this is downloaded from the server
            assertEquals(selector, consumer1.getMessageSelector());
          }

          // unload the topic
          cluster.getService().getAdminClient().topics().unload(topicName);

          try (PulsarMessageConsumer consumer1 =
              session.createSharedDurableConsumer(destination, subscriptionName, null); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());

            produce(session, destination);
            consume(consumer1);

            // this is downloaded from the server
            assertEquals(selector, consumer1.getMessageSelector());
          }

          // non-existing topic, auto-created
          try (PulsarMessageConsumer consumer1 =
              session.createSharedDurableConsumer(destination2, subscriptionName, null); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            produce(session, destination2);
            consume(consumer1);
            // this is downloaded from the server
            assertEquals(null, consumer1.getMessageSelector());
          }

          // non-existing subscription
          try (PulsarMessageConsumer consumer1 =
              session.createSharedDurableConsumer(
                  destination2, subscriptionName + "non-existing", null); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            produce(session, destination2);
            consume(consumer1);
            // this is downloaded from the server
            assertEquals(null, consumer1.getMessageSelector());
          }

          // PreconditionFailedException

          PulsarAdmin original = factory.getPulsarAdmin();
          PulsarAdmin mockPulsarAdmin = mock(PulsarAdmin.class);
          Topics topics = mock(Topics.class);
          AtomicBoolean done = new AtomicBoolean();
          when(topics.getSubscriptionProperties(anyString(), anyString()))
              .thenAnswer(
                  i -> {
                    done.set(true);
                    // restore the original PulsarAdmin
                    Whitebox.setInternalState(factory, "pulsarAdmin", original);
                    // throw an error
                    throw new PulsarAdminException.PreconditionFailedException(
                        new Exception(), "", 404);
                  });
          when(mockPulsarAdmin.topics()).thenReturn(topics);
          Whitebox.setInternalState(factory, "pulsarAdmin", mockPulsarAdmin);

          try (PulsarMessageConsumer consumer1 =
              session.createSharedDurableConsumer(destination, subscriptionName, null); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            produce(session, destination);
            consume(consumer1);
            // this is downloaded from the server
            assertEquals(selector, consumer1.getMessageSelector());
            assertTrue(done.get());
          }

          try (PulsarMessageConsumer consumer1 =
              session.createSharedDurableConsumer(destination, subscriptionName, null); ) {
            produce(session, destination);
            consume(consumer1);
            // this is downloaded from the server
            assertEquals(selector, consumer1.getMessageSelector());

            // update the properties on the server
            String newSelector = "keepme = TRUE or 1=1";
            subscriptionProperties = new HashMap<>();
            subscriptionProperties.put("jms.selector", newSelector);
            subscriptionProperties.put("jms.filtering", "true");

            cluster
                .getService()
                .getAdminClient()
                .topics()
                .updateSubscriptionProperties(topicName, subscriptionName, subscriptionProperties);

            Awaitility.await()
                .atMost(refreshServerSideFiltersPeriod * 2, TimeUnit.SECONDS)
                .untilAsserted(
                    () -> {
                      if (numPartitions > 0) {
                        consumer1.getSelectorSupportOnSubscription(topicName + "-partition-0");
                      } else {
                        consumer1.getSelectorSupportOnSubscription(topicName);
                      }
                      assertEquals(newSelector, consumer1.getMessageSelector());
                    });

            // disable server side selector
            subscriptionProperties = new HashMap<>();
            subscriptionProperties.put("jms.selector", newSelector);
            subscriptionProperties.put("jms.filtering", "false");

            cluster
                .getService()
                .getAdminClient()
                .topics()
                .updateSubscriptionProperties(topicName, subscriptionName, subscriptionProperties);

            Awaitility.await()
                .atMost(refreshServerSideFiltersPeriod * 2, TimeUnit.SECONDS)
                .untilAsserted(
                    () -> {
                      if (numPartitions > 0) {
                        consumer1.getSelectorSupportOnSubscription(topicName + "-partition-0");
                      } else {
                        consumer1.getSelectorSupportOnSubscription(topicName);
                      }
                      assertEquals(null, consumer1.getMessageSelector());
                    });
          }
        }
      }
    }
  }

  private void produce(PulsarSession session, Destination destination) throws JMSException {
    TextMessage text = session.createTextMessage("foo");
    text.setBooleanProperty("keepme", true);
    session.createProducer(null).send(destination, text);
  }

  private void consume(MessageConsumer consumer) throws JMSException {
    assertNotNull(consumer.receive());
  }
}
