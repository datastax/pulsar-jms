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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.TextMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

@Timeout(value = 1, unit = TimeUnit.MINUTES)
public class JMSMessageHeaderTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

  @Test
  public void sendMessageWithHeaderReceiveJMSContext() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext()) {
        Destination destination =
            context.createQueue("persistent://public/default/test-" + UUID.randomUUID());
        try (JMSConsumer consumer = context.createConsumer(destination)) {
          JMSProducer producer = context.createProducer();
          String message = "Hey JMS!";
          TextMessage expTextMessage = context.createTextMessage(message);
          expTextMessage.setJMSReplyTo(destination);
          expTextMessage.setJMSType("mytype");
          expTextMessage.setJMSCorrelationIDAsBytes(new byte[] {1, 2, 3});
          producer.send(destination, expTextMessage);
          TextMessage actTextMessage = (TextMessage) consumer.receive();

          assertNotNull(actTextMessage);
          assertEquals(expTextMessage.getText(), actTextMessage.getText());
          assertEquals(expTextMessage.getJMSReplyTo(), actTextMessage.getJMSReplyTo());
          assertEquals(expTextMessage.getJMSType(), actTextMessage.getJMSType());
          assertArrayEquals(new byte[] {1, 2, 3}, actTextMessage.getJMSCorrelationIDAsBytes());
        }
      }
    }
  }
}
