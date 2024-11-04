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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import jakarta.jms.CompletionListener;
import jakarta.jms.Connection;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

@Timeout(30)
@Slf4j
public class AcknowledgementModeTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

  @Test
  public void testAUTO_ACKNOWLEDGE() throws Exception {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());
          try (MessageProducer producer = session.createProducer(destination); ) {
            TextMessage textMsg = session.createTextMessage("foo");
            textMsg.setStringProperty("test", "foo");
            producer.send(textMsg);
          }

          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            assertEquals("foo", consumer.receive().getStringProperty("test"));
            // message is automatically acknowledged on receive,
            // but, as we are not setting ackReceiptEnabled, the acknowledgement does not wait
            // for the server to return success or failure
          }

          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            assertNull(consumer.receive(100));
          }
        }
      }
    }
  }

  @Test
  public void testAUTO_ACKNOWLEDGE_ackReceipt() throws Exception {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("ackReceiptEnabled", true);
    properties.put("consumerConfig", consumerConfig);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());
          try (MessageProducer producer = session.createProducer(destination); ) {
            for (int i = 0; i < 1000; i++) {
              TextMessage textMsg = session.createTextMessage("foo");
              textMsg.setStringProperty("test", "foo");
              producer.send(
                  textMsg,
                  new CompletionListener() {
                    @Override
                    public void onCompletion(Message message) {}

                    @Override
                    public void onException(Message message, Exception e) {}
                  });
            }
          }

          // the consumer waits for the confirmation of the ack
          // with you set ackReceiptEnabled=false you will see
          // that this test is notably faster
          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            for (int i = 0; i < 1000; i++) {
              assertEquals("foo", consumer.receive().getStringProperty("test"));
              log.info("ack {}", i);
              // message is automatically acknowledged on receive
            }
          }

          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            assertNull(consumer.receive(100));
          }
        }
      }
    }
  }

  @Test
  public void testADUPS_OK_ACKNOWLEDGE() throws Exception {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(Session.DUPS_OK_ACKNOWLEDGE); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());
          try (MessageProducer producer = session.createProducer(destination); ) {
            TextMessage textMsg = session.createTextMessage("foo");
            textMsg.setStringProperty("test", "foo");
            producer.send(textMsg);
          }

          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            assertEquals("foo", consumer.receive().getStringProperty("test"));
            // message is automatically acknowledged on receive, but best effort and async
          }
          // give time for the async ack
          Thread.sleep(1000);

          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            assertNull(consumer.receive(100));
          }
        }
      }
    }
  }

  @Test()
  public void testACLIENT_ACKNOWLEDGE() throws Exception {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageProducer producer = session.createProducer(destination); ) {
            TextMessage textMsg = session.createTextMessage("foo");
            textMsg.setStringProperty("test", "foo");
            producer.send(textMsg);

            TextMessage textMsg2 = session.createTextMessage("foo");
            textMsg2.setStringProperty("test", "foo2");
            producer.send(textMsg2);
          }

          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            assertEquals("foo", consumer.receive().getStringProperty("test"));
            // message is not automatically acknowledged on receive

            // closing the consumer
          }

          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            // receive and ack
            Message receive = consumer.receive();
            assertEquals("foo", receive.getStringProperty("test"));

            Message receive2 = consumer.receive();
            assertEquals("foo2", receive2.getStringProperty("test"));

            // ack only message1, this automatically acks all the other messages
            receive.acknowledge();
          }

          // no more messages
          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            assertNull(consumer.receive(100));
          }
        }
      }
    }
  }

  @Test
  public void testINDIVIDUAL_ACKNOWLEDGE() throws Exception {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session =
            connection.createSession(PulsarJMSConstants.INDIVIDUAL_ACKNOWLEDGE); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageProducer producer = session.createProducer(destination); ) {
            TextMessage textMsg = session.createTextMessage("foo");
            textMsg.setStringProperty("test", "foo");
            producer.send(textMsg);

            TextMessage textMsg2 = session.createTextMessage("foo");
            textMsg2.setStringProperty("test", "foo2");
            producer.send(textMsg2);
          }

          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            assertEquals("foo", consumer.receive().getStringProperty("test"));
            // message is not automatically acknowledged on receive

            // closing the consumer
          }

          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            // receive and ack
            Message receive = consumer.receive();
            assertEquals("foo", receive.getStringProperty("test"));

            Message receive2 = consumer.receive();
            assertEquals("foo2", receive2.getStringProperty("test"));

            // ack only message1,
            receive.acknowledge();
          }

          try (MessageConsumer consumer = session.createConsumer(destination); ) {

            // message2 is still there
            Message receive2 = consumer.receive();
            assertEquals("foo2", receive2.getStringProperty("test"));

            assertNull(consumer.receive(100));
          }
        }
      }
    }
  }

  @Test
  public void testINDIVIDUAL_ACKNOWLEDGEWithBatchingandBatchIndexAckEnabled() throws Exception {
    // see https://github.com/apache/pulsar/wiki/PIP-54:-Support-acknowledgment-at-batch-index-level
    testINDIVIDUAL_ACKNOWLEDGEWithBatching(true);
  }

  @Test
  public void testINDIVIDUAL_ACKNOWLEDGEWithBatchingWithoutBatchIndexAckEnabled() throws Exception {
    testINDIVIDUAL_ACKNOWLEDGEWithBatching(false);
  }

  private void testINDIVIDUAL_ACKNOWLEDGEWithBatching(boolean batchIndexAckEnabled)
      throws Exception {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batchingEnabled", "true");
    producerConfig.put("batchingMaxPublishDelayMicros", "1000000");
    producerConfig.put("batchingMaxMessages", "1000000");
    properties.put("producerConfig", producerConfig);

    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("ackReceiptEnabled", true);
    consumerConfig.put("batchIndexAckEnabled", batchIndexAckEnabled);

    properties.put("consumerConfig", consumerConfig);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session =
            connection.createSession(PulsarJMSConstants.INDIVIDUAL_ACKNOWLEDGE); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageProducer producer = session.createProducer(destination); ) {
            TextMessage textMsg = session.createTextMessage("foo");
            textMsg.setStringProperty("test", "foo");
            producer.send(
                textMsg,
                new CompletionListener() {
                  @Override
                  public void onCompletion(Message message) {}

                  @Override
                  public void onException(Message message, Exception e) {}
                });

            TextMessage textMsg2 = session.createTextMessage("foo");
            textMsg2.setStringProperty("test", "foo2");
            producer.send(
                textMsg2,
                new CompletionListener() {
                  @Override
                  public void onCompletion(Message message) {}

                  @Override
                  public void onException(Message message, Exception e) {}
                });
          }

          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            assertEquals("foo", consumer.receive().getStringProperty("test"));
            // message is not automatically acknowledged on receive

            // closing the consumer
          }

          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            // receive and ack
            PulsarMessage receive = (PulsarMessage) consumer.receive();
            assertEquals("foo", receive.getStringProperty("test"));
            assertTrue(
                receive.getReceivedPulsarMessage().getMessageId() instanceof BatchMessageIdImpl);

            PulsarMessage receive2 = (PulsarMessage) consumer.receive();
            assertEquals("foo2", receive2.getStringProperty("test"));
            assertTrue(
                receive2.getReceivedPulsarMessage().getMessageId() instanceof BatchMessageIdImpl);

            // ack only message1,
            receive.acknowledge();
          }

          try (MessageConsumer consumer = session.createConsumer(destination); ) {

            if (batchIndexAckEnabled) {
              // message1 is hidden to the client (even if it has been sent on the wire)
              // see PIP-54

              // message2 is still there
              Message receive2 = consumer.receive();
              assertEquals("foo2", receive2.getStringProperty("test"));

              assertNull(consumer.receive(100));

              // ack message2
              receive2.acknowledge();
            } else {
              // message1 is still there, because we haven't fully acknowledged the Batch
              Message receive = consumer.receive();
              assertEquals("foo", receive.getStringProperty("test"));

              // message2 is still there
              Message receive2 = consumer.receive();
              assertEquals("foo2", receive2.getStringProperty("test"));

              assertNull(consumer.receive(100));

              // ack message2
              receive2.acknowledge();
              // ack message1
              receive.acknowledge();
            }
          }

          // no more messages
          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            assertNull(consumer.receive(100));
          }
        }
      }
    }
  }

  @Test
  public void testAutoNackWrongType() throws Exception {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext session = factory.createContext()) {
        Queue destination =
            session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

        session.createProducer().send(destination, "foo");

        try (JMSConsumer consumer = session.createConsumer(destination); ) {

          try {
            // automatically returned to the queue, wrong type
            consumer.receiveBody(Boolean.class);
            fail();
          } catch (MessageFormatRuntimeException ok) {
          }

          assertEquals("foo", consumer.receiveBody(String.class));
        }
      }
    }
  }
}
