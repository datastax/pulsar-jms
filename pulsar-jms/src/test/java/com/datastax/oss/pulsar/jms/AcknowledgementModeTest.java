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
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
@Slf4j
public class AcknowledgementModeTest {

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  @BeforeAll
  public static void before() throws Exception {
    cluster = new PulsarCluster(tempDir);
    cluster.start();
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  public void testAUTO_ACKNOWLEDGE() throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
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
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
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
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
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
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
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
  public void testAutoNackWrongType() throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
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
