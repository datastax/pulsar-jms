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
import com.google.common.collect.ImmutableMap;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.internal.util.reflection.Whitebox;

@Slf4j
public class QueryStringTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer =
      new PulsarContainerExtension()
          .withEnv("PULSAR_PREFIX_transactionCoordinatorEnabled", "false")
          .withEnv("PULSAR_PREFIX_entryFilterNames", "");

  @Test
  public void testOverrideReceiverQueueSize() throws Exception {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("consumerConfig", ImmutableMap.of("receiverQueueSize", 18));
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection(); ) {
        connection.start();
        try (Session session = connection.createSession();
            Session sessionOverrideConsumerConfig =
                connection.createSession(
                    false,
                    Session.AUTO_ACKNOWLEDGE,
                    ConsumerConfiguration.buildConsumerConfiguration(
                        ImmutableMap.of("receiverQueueSize", 19)))) {
          String topicName = "persistent://public/default/test-" + UUID.randomUUID();
          Queue destinationWithCustomReceiverQueueSize =
              session.createQueue(topicName + "?consumerConfig.receiverQueueSize=10");

          Queue destinationWithDefaultReceiverQueueSize = session.createQueue(topicName);

          try (MessageConsumer consumer1 =
              session.createConsumer(destinationWithCustomReceiverQueueSize, null, true); ) {
            int maxReceiverQueueSize =
                (int)
                    Whitebox.getInternalState(
                        ((PulsarMessageConsumer) consumer1).getConsumer(), "maxReceiverQueueSize");
            assertEquals(10, maxReceiverQueueSize);
          }

          try (MessageConsumer consumer2 =
              session.createConsumer(destinationWithDefaultReceiverQueueSize, null, true); ) {
            int maxReceiverQueueSize =
                (int)
                    Whitebox.getInternalState(
                        ((PulsarMessageConsumer) consumer2).getConsumer(), "maxReceiverQueueSize");
            assertEquals(18, maxReceiverQueueSize);
          }

          try (MessageConsumer consumer3 =
              sessionOverrideConsumerConfig.createConsumer(
                  destinationWithCustomReceiverQueueSize, null, true); ) {
            int maxReceiverQueueSize =
                (int)
                    Whitebox.getInternalState(
                        ((PulsarMessageConsumer) consumer3).getConsumer(), "maxReceiverQueueSize");
            assertEquals(10, maxReceiverQueueSize);
          }

          try (MessageConsumer consumer4 =
              sessionOverrideConsumerConfig.createConsumer(
                  destinationWithDefaultReceiverQueueSize, null, true); ) {
            int maxReceiverQueueSize =
                (int)
                    Whitebox.getInternalState(
                        ((PulsarMessageConsumer) consumer4).getConsumer(), "maxReceiverQueueSize");
            assertEquals(19, maxReceiverQueueSize);
          }

          Queue multiDestinationWithCustomReceiverQueueSize =
              session.createQueue("multi:" + topicName + "?consumerConfig.receiverQueueSize=10");
          try (MessageConsumer consumer1 =
              session.createConsumer(multiDestinationWithCustomReceiverQueueSize, null, true); ) {
            int maxReceiverQueueSize =
                (int)
                    Whitebox.getInternalState(
                        ((PulsarMessageConsumer) consumer1).getConsumer(), "maxReceiverQueueSize");
            assertEquals(10, maxReceiverQueueSize);
          }

          Queue multiDoubleDestinationWithCustomReceiverQueueSize =
              session.createQueue(
                  "multi:" + topicName + ",topic2?consumerConfig.receiverQueueSize=10");
          try (MessageConsumer consumer1 =
              session.createConsumer(
                  multiDoubleDestinationWithCustomReceiverQueueSize, null, true); ) {
            int maxReceiverQueueSize =
                (int)
                    Whitebox.getInternalState(
                        ((PulsarMessageConsumer) consumer1).getConsumer(), "maxReceiverQueueSize");
            assertEquals(10, maxReceiverQueueSize);
          }

          Queue regExDestinationWithCustomReceiverQueueSize =
              session.createQueue("regex:" + topicName + "?consumerConfig.receiverQueueSize=10");
          try (MessageConsumer consumer1 =
              session.createConsumer(regExDestinationWithCustomReceiverQueueSize, null, true); ) {
            int maxReceiverQueueSize =
                (int)
                    Whitebox.getInternalState(
                        ((PulsarMessageConsumer) consumer1).getConsumer(), "maxReceiverQueueSize");
            assertEquals(10, maxReceiverQueueSize);
          }
        }
      }
    }
  }
}
