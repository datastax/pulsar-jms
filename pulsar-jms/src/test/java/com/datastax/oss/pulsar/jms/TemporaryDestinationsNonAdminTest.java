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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
public class TemporaryDestinationsNonAdminTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer =
      new PulsarContainerExtension()
          .withEnv("PULSAR_PREFIX_allowAutoTopicCreation", "true")
          .withEnv("PULSAR_PREFIX_allowAutoTopicCreationType", "non-partitioned")
          .withEnv("PULSAR_PREFIX_transactionCoordinatorEnabled", "false");

  @Test
  public void allowTemporaryTopicWithoutAdminTest() throws Exception {
    Map<String, Object> properties = getJmsProperties();
    properties.put("jms.allowTemporaryTopicWithoutAdmin", "true");
    useTemporaryDestinationNonAdminTest(properties, false);
  }

  @Test
  public void forbidTemporaryTopicWithoutAdminTest() throws Exception {
    Map<String, Object> properties = getJmsProperties();
    useTemporaryDestinationNonAdminTest(properties, true);
  }

  @NotNull
  private static Map<String, Object> getJmsProperties() {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.forceDeleteTemporaryDestinations", "true");
    properties.put("jms.usePulsarAdmin", "false");
    return properties;
  }

  private void useTemporaryDestinationNonAdminTest(
      Map<String, Object> properties, boolean expectAdminErrors) throws Exception {

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties)) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession()) {
          if (expectAdminErrors) {
            assertThrows(JMSException.class, session::createTemporaryTopic);
            return;
          }
          Destination clientAddress = session.createTemporaryTopic();
          testProducerAndConsumer(session, clientAddress);
        }
      }
    }
  }

  private static void testProducerAndConsumer(Session session, Destination clientAddress)
      throws JMSException {
    try (MessageProducer producerClient = session.createProducer(clientAddress)) {
      // subscribe on the temporary queue
      try (MessageConsumer consumerClient = session.createConsumer(clientAddress)) {

        String testMessage = "message";
        // produce a message
        producerClient.send(session.createTextMessage(testMessage));

        // on the consumer receive the message
        Message theResponse = consumerClient.receive();
        assertEquals(testMessage, theResponse.getBody(String.class));
      }
    }
  }
}
