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

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
public class ProducerCacheTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

  /**
   * Test for jms.maxNumOfProducers.
   *
   * <p>Sets the maximum number of producers to 1. When two producers are created (on different
   * topics), the cache should evict the oldest entry so that only 1 producer remains in the cache.
   * Broker stats should reflect the same.
   */
  @Test
  public void testMaxNumOfProducers() throws JMSException {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    // Configure maximum number of producers to 1.
    properties.put("jms.maxNumOfProducers", "1");

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties)) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession()) {
          // Create two temporary queues and corresponding producers.
          TemporaryQueue queue1 = session.createTemporaryQueue();
          MessageProducer producer1 = session.createProducer(queue1);
          producer1.send(session.createTextMessage("foo-1"));

          TemporaryQueue queue2 = session.createTemporaryQueue();
          MessageProducer producer2 = session.createProducer(queue2);
          producer2.send(session.createTextMessage("foo-2"));

          // Assert that at most 1 producer is retained in the cache.
          assertEquals(1, factory.getProducers().size(), "Cache should contain at most 1 producer");
          // Assert with broker stats that the evicted producer is closed.
          int totalBrokerProducers = fetchProducerCount(queue1) + fetchProducerCount(queue2);
          assertEquals(1, totalBrokerProducers, "Total broker producers count should be 1");
        } catch (PulsarAdminException e) {
          throw Utils.handleException(e);
        }
      }
    }
  }

  /**
   * Test for jms.producerAutoCloseTimeoutSec.
   *
   * <p>Sets the auto-close timeout to 1 second. After creating a producer and sending a message,
   * waiting longer than the timeout should result in the producer being evicted from the cache, and
   * the underlying producer closed (as verified via broker stats).
   */
  @Test
  public void testProducerAutoCloseTimeout() throws JMSException, InterruptedException {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    // Set the auto-close timeout to 1 second.
    properties.put("jms.producerAutoCloseTimeoutSec", "1");

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties)) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession()) {
          TemporaryQueue tempQueue = session.createTemporaryQueue();
          MessageProducer producer = session.createProducer(tempQueue);
          producer.send(session.createTextMessage("foo-1"));

          // Assert that the producer is registered.
          assertEquals(1, factory.getProducers().size(), "Cache should contain 1 producer");
          assertEquals(1, fetchProducerCount(tempQueue), "Broker should have 1 producer initially");

          // Wait longer than the auto-close timeout to allow expiration.
          Thread.sleep(2000);
          // Trigger cache cleanup to remove the expired entry.
          factory.getProducers().cleanUp();

          // Now, the cache should be empty.
          assertEquals(
              0, factory.getProducers().size(), "Cache should be empty after auto-close timeout");
          // Verify with broker stats that no producers exist.
          assertEquals(
              0,
              fetchProducerCount(tempQueue),
              "Broker should have 0 producers after auto-close timeout");
        } catch (PulsarAdminException e) {
          throw Utils.handleException(e);
        }
      }
    }
  }

  /**
   * Helper method to fetch the number of producers for a given temporary queue from broker stats.
   */
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
