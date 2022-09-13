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
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class DeadLetterQueueTest {

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  @BeforeAll
  public static void before() throws Exception {
    cluster = new PulsarCluster(tempDir, config -> config.setTransactionCoordinatorEnabled(false));
    cluster.start();
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  public void deadLetterTestForQueue() throws Exception {

    // basic test
    String topic = "persistent://public/default/test-" + UUID.randomUUID();
    // this name is automatic, but you can configure it
    // https://pulsar.apache.org/docs/concepts-messaging/#dead-letter-topic
    String queueSubscriptionName = "thesub";
    String deadLetterTopic = topic + "-" + queueSubscriptionName + "-DLQ";

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.queueSubscriptionName", queueSubscriptionName);

    Map<String, Object> consumerConfig = new HashMap<>();
    properties.put("consumerConfig", consumerConfig);

    consumerConfig.put("ackTimeoutMillis", 1000);

    Map<String, Object> deadLetterPolicy = new HashMap<>();
    consumerConfig.put("deadLetterPolicy", deadLetterPolicy);
    deadLetterPolicy.put("maxRedeliverCount", 1);
    // this is the name of a subscription that is created
    // while creating the producer to the DQL topic
    deadLetterPolicy.put("initialSubscriptionName", "dqlsub");

    performTest(topic, deadLetterTopic, properties, false);
  }

  @Test
  public void deadLetterTestForTopic() throws Exception {

    // basic test
    String topic = "persistent://public/default/test-" + UUID.randomUUID();
    // this name is automatic, but you can configure it
    // https://pulsar.apache.org/docs/concepts-messaging/#dead-letter-topic
    String topicSubscriptionName = "thesub";
    String deadLetterTopic = topic + "-" + topicSubscriptionName + "-DLQ";

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());

    Map<String, Object> consumerConfig = new HashMap<>();
    properties.put("consumerConfig", consumerConfig);

    consumerConfig.put("ackTimeoutMillis", 1000);

    Map<String, Object> deadLetterPolicy = new HashMap<>();
    consumerConfig.put("deadLetterPolicy", deadLetterPolicy);
    deadLetterPolicy.put("maxRedeliverCount", 1);
    // this is the name of a subscription that is created
    // while creating the producer to the DQL topic
    deadLetterPolicy.put("initialSubscriptionName", "dqlsub");

    performTest(topic, deadLetterTopic, properties, true, topicSubscriptionName);
  }

  @Test
  public void deadLetterConfigTest() throws Exception {

    // in this test we try to set al possible configuration value

    String topic = "persistent://public/default/test-" + UUID.randomUUID();
    String deadLetterTopic = "persistent://public/default/test-dlq-" + UUID.randomUUID();

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());

    Map<String, Object> consumerConfig = new HashMap<>();
    properties.put("consumerConfig", consumerConfig);

    consumerConfig.put("ackTimeoutMillis", 1000);

    Map<String, Object> deadLetterPolicy = new HashMap<>();
    consumerConfig.put("deadLetterPolicy", deadLetterPolicy);
    deadLetterPolicy.put("maxRedeliverCount", 1);
    deadLetterPolicy.put("deadLetterTopic", deadLetterTopic);
    deadLetterPolicy.put("initialSubscriptionName", "dqlsub");

    Map<String, Object> negativeAckRedeliveryBackoff = new HashMap<>();
    consumerConfig.put("negativeAckRedeliveryBackoff", negativeAckRedeliveryBackoff);
    negativeAckRedeliveryBackoff.put("minDelayMs", 10);
    negativeAckRedeliveryBackoff.put("maxDelayMs", 100);
    negativeAckRedeliveryBackoff.put("multiplier", 2.0);

    Map<String, Object> ackTimeoutRedeliveryBackoff = new HashMap<>();
    consumerConfig.put("ackTimeoutRedeliveryBackoff", ackTimeoutRedeliveryBackoff);
    ackTimeoutRedeliveryBackoff.put("minDelayMs", 10);
    ackTimeoutRedeliveryBackoff.put("maxDelayMs", 100);
    ackTimeoutRedeliveryBackoff.put("multiplier", 2.0);

    performTest(topic, deadLetterTopic, properties, false);
  }

  private void performTest(
      String topic, String deadLetterTopic, Map<String, Object> properties, boolean useTopic)
      throws JMSException {
    performTest(topic, deadLetterTopic, properties, useTopic, null);
  }

  private void performTest(
      String topic,
      String deadLetterTopic,
      Map<String, Object> properties,
      boolean useTopic,
      String topicSubscriptionName)
      throws JMSException {
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE); ) {
          Destination destination =
              useTopic ? session.createTopic(topic) : session.createQueue(topic);

          try (MessageConsumer consumer1 =
              useTopic
                  ? session.createSharedConsumer((Topic) destination, topicSubscriptionName)
                  : session.createConsumer(destination)) {

            Topic destinationDeadletter = session.createTopic(deadLetterTopic);

            try (MessageConsumer consumerDeadLetter =
                session.createSharedConsumer(destinationDeadletter, "dqlsub"); ) {

              try (MessageProducer producer = session.createProducer(destination); ) {
                producer.send(session.createTextMessage("foo"));
              }

              Message message = consumer1.receive();
              assertEquals("foo", message.getBody(String.class));
              assertEquals(1, message.getIntProperty("JMSXDeliveryCount"));
              assertFalse(message.getJMSRedelivered());

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
          }
        }
      }
    }
  }
}
