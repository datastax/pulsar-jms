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

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class PulsarInteropTest {

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
  public void sendFromJMSReceiveFromPulsarClientTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        try (Session session = connection.createSession(); ) {
          String topic = "persistent://public/default/test-" + UUID.randomUUID();
          Destination destination = session.createTopic(topic);

          PulsarClient client =
              cluster
                  .getService()
                  .getClient(); // do not close this client, it is internal to the broker

          try (Consumer<String> consumer =
              client.newConsumer(Schema.STRING).subscriptionName("test").topic(topic).subscribe()) {

            try (MessageProducer producer = session.createProducer(destination); ) {
              TextMessage textMsg = session.createTextMessage("foo");
              textMsg.setStringProperty("JMSXGroupID", "bar");
              producer.send(textMsg);

              Message<String> receivedMessage = consumer.receive();
              assertEquals("foo", receivedMessage.getValue());
              assertEquals("bar", receivedMessage.getKey());
            }
          }
        }
      }
    }
  }

  @Test
  public void sendFromPulsarClientReceiveWithJMS() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext()) {

        String topic = "persistent://public/default/test-" + UUID.randomUUID();
        Destination destination = context.createTopic(topic);

        PulsarClient client =
            cluster
                .getService()
                .getClient(); // do not close this client, it is internal to the broker
        try (JMSConsumer consumer = context.createConsumer(destination)) {

          try (Producer<String> producer =
              client.newProducer(Schema.STRING).topic(topic).create(); ) {
            producer.newMessage().value("foo").key("bar").send();

            // the JMS client reads raw messages always as BytesMessage
            BytesMessage message = (BytesMessage) consumer.receive();
            assertArrayEquals(
                "foo".getBytes(StandardCharsets.UTF_8), message.getBody(byte[].class));
            assertEquals("bar", message.getStringProperty("JMSXGroupID"));
          }
        }
      }
    }
  }
}
