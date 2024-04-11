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
import static org.junit.jupiter.api.Assertions.assertNull;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.TextMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JMSPublishFiltersTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer =
      new PulsarContainerExtension()
          .withEnv("PULSAR_PREFIX_transactionCoordinatorEnabled", "false")
          .withEnv("PULSAR_PREFIX_brokerInterceptorsDirectory", "/pulsar/interceptors")
          .withEnv("PULSAR_PREFIX_brokerInterceptors", "jms-publish-filters")
          .withEnv("PULSAR_LOG_LEVEL", "info");

  private Map<String, Object> buildProperties() {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.useServerSideFiltering", true);
    properties.put("jms.enableClientSideEmulation", false);

    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batchingEnabled", false);
    properties.put("producerConfig", producerConfig);
    return properties;
  }

  @Test
  public void sendMessageReceiveFromQueue() throws Exception {
    Map<String, Object> properties = buildProperties();

    String topicName = "persistent://public/default/test-" + UUID.randomUUID();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Queue destination = session.createQueue(topicName);

          try (PulsarMessageConsumer consumer1 = session.createConsumer(destination); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());

            String newSelector = "lastMessage=TRUE";
            Map<String, String> subscriptionProperties = new HashMap<>();
            subscriptionProperties.put("jms.selector", newSelector);
            subscriptionProperties.put("jms.filtering", "true");

            pulsarContainer
                .getAdmin()
                .topics()
                .updateSubscriptionProperties(topicName, "jms-queue", subscriptionProperties);

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                if (i == 9) {
                  textMessage.setBooleanProperty("lastMessage", true);
                }
                producer.send(textMessage);
              }
            }

            TextMessage textMessage = (TextMessage) consumer1.receive();
            assertEquals("foo-9", textMessage.getText());

            assertEquals(1, consumer1.getReceivedMessages());
            assertEquals(0, consumer1.getSkippedMessages());

            // no more messages
            assertNull(consumer1.receiveNoWait());

            // ensure that the filter didn't reject any message while dispatching to the consumer
            // because the filter has been already applied on the write path
            TopicStats stats = pulsarContainer.getAdmin().topics().getStats(topicName);
            SubscriptionStats subscriptionStats = stats.getSubscriptions().get("jms-queue");
            assertEquals(subscriptionStats.getFilterProcessedMsgCount(), 1);
            assertEquals(subscriptionStats.getFilterRejectedMsgCount(), 0);
            assertEquals(subscriptionStats.getFilterAcceptedMsgCount(), 1);
          }

          // create a message that doesn't match the filter
          // verify that the back log is accurate (0)

          try (MessageProducer producer = session.createProducer(destination); ) {
            TextMessage textMessage = session.createTextMessage("backlog");
            producer.send(textMessage);

            TopicStats stats = pulsarContainer.getAdmin().topics().getStats(topicName);
            SubscriptionStats subscriptionStats = stats.getSubscriptions().get("jms-queue");
            assertEquals(0, subscriptionStats.getMsgBacklog());
          }
        }
      }
    }
  }
}
