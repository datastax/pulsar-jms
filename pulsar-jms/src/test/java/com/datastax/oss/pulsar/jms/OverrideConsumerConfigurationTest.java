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

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
public class OverrideConsumerConfigurationTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer =
      new PulsarContainerExtension()
          .withEnv("PULSAR_PREFIX_transactionCoordinatorEnabled", "false");

  @Test
  public void overrideDQLConfigurationWithJMSContext() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
        PulsarJMSContext primaryContext =
            (PulsarJMSContext) factory.createContext(JMSContext.CLIENT_ACKNOWLEDGE)) {
      Queue destination =
          primaryContext.createQueue("persistent://public/default/test-" + UUID.randomUUID());

      Topic destinationDeadletter =
          primaryContext.createTopic("persistent://public/default/test-dlq-" + UUID.randomUUID());

      Map<String, Object> consumerConfig =
          createConsumerConfigurationWithDQL(destinationDeadletter);

      try (JMSContext overrideConsumerConfiguration =
              primaryContext.createContext(
                  primaryContext.getSessionMode(),
                  ImmutableMap.of("consumerConfig", consumerConfig));
          JMSConsumer consumerWithDLQConfiguration =
              overrideConsumerConfiguration.createConsumer(destination); ) {

        primaryContext.createProducer().send(destination, "foo");

        Message message = consumerWithDLQConfiguration.receive();
        assertEquals("foo", message.getBody(String.class));
        assertEquals(1, message.getIntProperty("JMSXDeliveryCount"));
        assertFalse(message.getJMSRedelivered());

        // message is re-delivered again after ackTimeoutMillis
        message = consumerWithDLQConfiguration.receive();
        assertEquals("foo", message.getBody(String.class));
        assertEquals(2, message.getIntProperty("JMSXDeliveryCount"));
        assertTrue(message.getJMSRedelivered());

        try (JMSConsumer consumerDeadLetter =
            primaryContext.createSharedConsumer(destinationDeadletter, "dqlsub"); ) {

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

  @Test
  public void overrideDQLConfigurationWithSession() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
        PulsarConnection connection = factory.createConnection();
        PulsarSession primarySession = connection.createSession(Session.CLIENT_ACKNOWLEDGE)) {
      connection.start();
      Queue destination =
          primarySession.createQueue("persistent://public/default/test-" + UUID.randomUUID());

      Topic destinationDeadletter =
          primarySession.createTopic("persistent://public/default/test-dlq-" + UUID.randomUUID());

      Map<String, Object> consumerConfig =
          createConsumerConfigurationWithDQL(destinationDeadletter);

      try (PulsarSession overrideConsumerConfiguration =
              primarySession.createSession(
                  primarySession.getAcknowledgeMode(),
                  ImmutableMap.of("consumerConfig", consumerConfig));
          MessageConsumer consumerWithDLQConfiguration =
              overrideConsumerConfiguration.createConsumer(destination); ) {

        primarySession.createProducer(destination).send(primarySession.createTextMessage("foo"));

        Message message = consumerWithDLQConfiguration.receive();
        assertEquals("foo", message.getBody(String.class));
        assertEquals(1, message.getIntProperty("JMSXDeliveryCount"));
        assertFalse(message.getJMSRedelivered());

        // message is re-delivered again after ackTimeoutMillis
        message = consumerWithDLQConfiguration.receive();
        assertEquals("foo", message.getBody(String.class));
        assertEquals(2, message.getIntProperty("JMSXDeliveryCount"));
        assertTrue(message.getJMSRedelivered());

        try (MessageConsumer consumerDeadLetter =
            primarySession.createSharedConsumer(destinationDeadletter, "dqlsub"); ) {

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

  private Map<String, Object> createConsumerConfigurationWithDQL(Topic destinationDeadletter)
      throws JMSException {
    Map<String, Object> consumerConfig = new HashMap<>();

    consumerConfig.put("ackTimeoutMillis", 1000);
    Map<String, Object> deadLetterPolicy = new HashMap<>();
    consumerConfig.put("deadLetterPolicy", deadLetterPolicy);
    deadLetterPolicy.put("maxRedeliverCount", 1);
    deadLetterPolicy.put("deadLetterTopic", destinationDeadletter.getTopicName());
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
    return consumerConfig;
  }
}
