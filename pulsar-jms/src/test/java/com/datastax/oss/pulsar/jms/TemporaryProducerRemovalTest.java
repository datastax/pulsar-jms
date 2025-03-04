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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
public class TemporaryProducerRemovalTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

  @Test
  public void testTemporaryProducerRemovalOnClose() throws JMSException {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    // Enable temporary producers.
    properties.put("jms.useTemporaryProducers", "true");

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties)) {

      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession()) {

          // Create a temporary queue and a producer for it.
          TemporaryQueue tempQueue = session.createTemporaryQueue();
          MessageProducer producer = session.createProducer(tempQueue);

          // A temporary producer should be registered in the factory and the broker stats when send
          // is
          // called.
          producer.send(session.createTextMessage("foo-1"));

          // Assert that there is one producer on the topic from broker stats and in hashmap of
          // factory
          assertEquals(1, factory.getProducers().size());
          assertEquals(1, fetchProducerCount(tempQueue));

          // Close the temporary producer. This should trigger its removal from the map and from the
          // broker.
          producer.close();
          // Assert double close doesn't throw error
          assertDoesNotThrow(producer::close);

          // Assert that there is zero producers on the topic from broker stats
          assertEquals(0, factory.getProducers().size());
          assertEquals(0, fetchProducerCount(tempQueue));
        } catch (PulsarAdminException e) {
          throw Utils.handleException(e);
        }
      }
    }
  }

  @Test
  public void testProducerDefaultFunctionalityOnClose() throws JMSException {
    // By default, temporary producers are disabled. So close() will not remove them from broker
    // stats
    // or concurrent hashmap
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties)) {

      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession()) {

          // Create a temporary queue and a producer for it.
          TemporaryQueue tempQueue = session.createTemporaryQueue();
          MessageProducer producer = session.createProducer(tempQueue);

          // A producer should be registered in the factory and the broker stats when send is
          // called.
          producer.send(session.createTextMessage("foo-1"));

          // Assert that there is one producer on the topic from broker stats and in factory
          assertEquals(1, factory.getProducers().size());
          assertEquals(1, fetchProducerCount(tempQueue));

          // Close the producer. This should not trigger its removal from the map and from the
          // broker.
          producer.close();

          // Assert that there is still one producer on the topic from broker stats and in factory
          assertEquals(1, factory.getProducers().size());
          assertEquals(1, fetchProducerCount(tempQueue));
        } catch (PulsarAdminException e) {
          throw Utils.handleException(e);
        }
      }
    }
  }

  private static int fetchProducerCount(TemporaryQueue tempQueue)
      throws PulsarAdminException, JMSException {
    return pulsarContainer
        .getAdmin()
        .topics()
        .getStats(tempQueue.getQueueName())
        .getPublishers()
        .size();
  }
}
