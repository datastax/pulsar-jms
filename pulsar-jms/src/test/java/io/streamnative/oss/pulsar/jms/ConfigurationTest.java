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
package io.streamnative.oss.pulsar.jms;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.streamnative.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.jms.Queue;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ConfigurationTest {

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  @BeforeAll
  public static void before() throws Exception {
    cluster = new PulsarCluster(tempDir);
    cluster.start();
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  public void customizeProducerTest() throws Exception {
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("producerName", "the-name");
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("producerConfig", producerConfig);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
        PulsarConnection connection = factory.createConnection(); ) {
      PulsarDestination destination = new PulsarQueue("test-" + UUID.randomUUID());
      Producer<byte[]> producer = factory.getProducerForDestination(destination, false);
      assertEquals("the-name", producer.getProducerName());
    }
  }

  @Test
  public void customizeConsumerTest() throws Exception {
    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("consumerName", "the-consumer-name");
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("consumerConfig", consumerConfig);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
        PulsarConnection connection = factory.createConnection();
        PulsarSession session = connection.createSession()) {
      Queue queue = session.createQueue("test" + UUID.randomUUID());
      try (PulsarMessageConsumer consumer = session.createConsumer(queue); ) {
        Consumer<?> pulsarConsumer = consumer.getConsumer();
        assertEquals("the-consumer-name", pulsarConsumer.getConsumerName());
      }
    }
  }
}
