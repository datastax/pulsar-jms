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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import java.util.Map;
import java.util.UUID;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
public class NoAutoCreateSubscriptionTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer =
      new PulsarContainerExtension()
          .withEnv("PULSAR_PREFIX_allowAutoTopicCreation", "false")
          .withEnv("PULSAR_PREFIX_allowAutoSubscriptionCreation", "false");

  @Test
  public void doNotPrecreateQueueSubscriptionTest() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.queueSubscriptionName", "default-sub-name");
    properties.put("jms.precreateQueueSubscription", "false");
    properties.put("operationTimeoutMs", "5000");

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          String shortTopicName = "test-" + UUID.randomUUID();

          pulsarContainer.getAdmin().topics().createNonPartitionedTopic(shortTopicName);

          Queue destinationWithSubscription = session.createQueue(shortTopicName + ":sub1");

          try (MessageProducer producer = session.createProducer(destinationWithSubscription); ) {
            for (int i = 0; i < 10; i++) {
              producer.send(session.createTextMessage("foo-" + i));
            }
          }

          // fail
          try (MessageConsumer consumer1 = session.createConsumer(destinationWithSubscription)) {
          } catch (JMSException err) {
            assertTrue((err + "").contains("Subscription does not exist"));
          }

          // manually create the subscription topic:sub1
          pulsarContainer
              .getAdmin()
              .topics()
              .createSubscription(shortTopicName, "sub1", MessageId.earliest);

          try (MessageConsumer consumer1 = session.createConsumer(destinationWithSubscription)) {
            for (int i = 0; i < 10; i++) {
              assertNotNull(consumer1.receive());
            }

            // verify that we have 1 subscription
            TopicStats stats = pulsarContainer.getAdmin().topics().getStats(shortTopicName);
            log.info("Subscriptions {}", stats.getSubscriptions().keySet());
            assertNotNull(stats.getSubscriptions().get("sub1"));
          }
        }
      }
    }
  }
}
