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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.MessageImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ConfigurationTest {
  @RegisterExtension
  static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

  @Test
  public void customizeProducerTest() throws Exception {
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("producerName", "the-name");
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
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
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
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

  @ParameterizedTest(name = "compressionTest {0}")
  @ValueSource(strings = {"", "NONE", "LZ4", "ZSTD", "ZLIB", "ZSTD", "SNAPPY", "Unknown"})
  public void compressionTest(String compressionType) throws Exception {
    Map<String, Object> producerConfig = new HashMap<>();
    if (!"".equals(compressionType)) {
      producerConfig.put("compressionType", compressionType);
    } else {
      // check if the compression is disabled by default
      compressionType = "NONE";
    }

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("producerConfig", producerConfig);
    char[] data = new char[1024];
    Arrays.fill(data, 'a');
    String message = new String(data);
    try {
      try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
          PulsarConnection connection = factory.createConnection(); ) {
        connection.start();
        var admin = factory.getPulsarAdmin();
        try (PulsarSession session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());
          try (PulsarMessageConsumer consumer = session.createConsumer(destination); ) {
            try (MessageProducer producer = session.createProducer(destination); ) {
              producer.send(session.createTextMessage(message));
            }

            // check msg compression type by pulsar consumer (without ack)
            @SuppressWarnings("unchecked")
            var msg = (MessageImpl<byte[]>) consumer.getConsumer().receive(5, TimeUnit.SECONDS);
            assertEquals(compressionType, msg.getMessageBuilder().getCompression().name());

            // check topic stats byte in/out count as per compression type
            var stat = admin.topics().getStats(destination.getQueueName());
            if ("NONE".equals(compressionType)) {
              assertTrue(stat.getBytesInCounter() > data.length);
              assertTrue(stat.getBytesOutCounter() > data.length);
            } else {
              assertTrue(stat.getBytesInCounter() < data.length / 3);
              assertTrue(stat.getBytesOutCounter() < data.length / 3);
            }

            // check auto-decompressed msg content by pulsar-consumer
            assertEquals(message, new String(msg.getValue()));
          }

          // double-check auto-decompressed msg content by jms-consumer
          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            var msg = consumer.receive(5_000);
            assertEquals(message, msg.getBody(String.class));
          }
        }
      }

    } catch (Exception e) {
      if (compressionType.equals("Unknown")) {
        // Cannot deserialize value of type `org.apache.pulsar.client.api.CompressionType` from
        // String "Unknown"
        assertTrue(e.getMessage().contains("Failed to load config"));
      } else {
        throw e;
      }
    }
  }
}
