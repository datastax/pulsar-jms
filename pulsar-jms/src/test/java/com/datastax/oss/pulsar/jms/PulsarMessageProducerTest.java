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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import jakarta.jms.*;
import java.util.Map;
import java.util.UUID;
import org.junit.ClassRule;
import org.junit.Test;

public class PulsarMessageProducerTest {

  @ClassRule
  public static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

  @Test
  public void producerErrorShouldContainTopic() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
        PulsarConnection connection = factory.createConnection()) {

      PulsarSession session = connection.createSession();

      PulsarDestination destination = new PulsarQueue("invalid@@@" + UUID.randomUUID());

      MessageProducer producer = session.createProducer(destination);

      try {
        producer.send(session.createTextMessage("test"));
        fail("Expected exception");

      } catch (JMSException e) {
        System.out.println("EXCEPTION = " + e.getMessage());

        assertTrue(e.getMessage().contains("topic="));
      }
    }
  }
}
