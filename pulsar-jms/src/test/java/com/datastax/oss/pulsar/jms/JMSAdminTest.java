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
import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.jms.InvalidDestinationException;
import javax.jms.Queue;
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
              c.setTransactionCoordinatorEnabled(false);
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

      Queue destination = admin.getQueue(topic);
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
    }
  }
}
