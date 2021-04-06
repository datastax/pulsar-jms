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

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SimpleTest {

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
  public void sendMessageTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        try (Session session = connection.createSession(); ) {
          Destination destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          try (MessageProducer producer = session.createProducer(destination); ) {
            TextMessage textMsg = session.createTextMessage("foo");
            producer.send(textMsg);

            StreamMessage streamMessage = session.createStreamMessage();
            streamMessage.writeBytes("foo".getBytes(StandardCharsets.UTF_8));
            producer.send(streamMessage);

            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeInt(234);
            producer.send(bytesMessage);

            Message headerOnly = session.createMessage();
            headerOnly.setBooleanProperty("myproperty", true);
            producer.send(headerOnly);

            ObjectMessage objectMessage = session.createObjectMessage("test");
            producer.send(objectMessage);

            MapMessage mapMessage = session.createMapMessage();
            mapMessage.setBoolean("p1", true);
            producer.send(mapMessage);

            Message simpleAsync = session.createMessage();
            CompletableFuture<Message> res = new CompletableFuture<>();
            producer.send(
                simpleAsync,
                new CompletionListener() {
                  @Override
                  public void onCompletion(Message message) {
                    res.complete(message);
                  }

                  @Override
                  public void onException(Message message, Exception exception) {
                    if (message != simpleAsync) {
                      res.completeExceptionally(new IllegalArgumentException());
                      return;
                    }
                    res.completeExceptionally(exception);
                  }
                });
            assertTrue(res.get() == simpleAsync);
            assertNotNull(simpleAsync.getJMSMessageID());
            assertTrue(simpleAsync.getJMSTimestamp() > 0);
            assertTrue(simpleAsync.getJMSDeliveryTime() > 0);
          }
        }
      }
    }
  }

  @Test
  public void sendMessageReceiveFromQueue() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        try (Session session = connection.createSession(); ) {
          Destination destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer = session.createConsumer(destination); ) {

            try (MessageProducer producer = session.createProducer(destination); ) {
              TextMessage textMsg = session.createTextMessage("foo");
              producer.send(textMsg);
              ObjectMessage objectMsg = session.createObjectMessage("bar");
              producer.send(objectMsg);
              BytesMessage bytesMsg = session.createBytesMessage();
              bytesMsg.writeInt(1234);
              producer.send(bytesMsg);
              StreamMessage streamMessage = session.createStreamMessage();
              streamMessage.writeLong(1234);
              producer.send(streamMessage);
              MapMessage mapMessage = session.createMapMessage();
              mapMessage.setBoolean("foo", true);
              mapMessage.setString("bar", "test");
              producer.send(mapMessage);
              Message simpleMessage = session.createMessage();
              simpleMessage.setByteProperty("a", (byte) 1);
              simpleMessage.setLongProperty("b", 123232323233L);
              simpleMessage.setIntProperty("c", 1232323);
              simpleMessage.setStringProperty("d", "ttt");
              simpleMessage.setBooleanProperty("e", true);
              simpleMessage.setFloatProperty("f", 1.3f);
              simpleMessage.setDoubleProperty("g", 1.9d);
              simpleMessage.setShortProperty("h", (short) 89);
              // we are serializing Object properties with "toString"
              simpleMessage.setObjectProperty("i", "qqqq");
              producer.send(simpleMessage);
            }

            TextMessage msg = (TextMessage) consumer.receive();
            assertEquals("foo", msg.getText());
            ObjectMessage msg2 = (ObjectMessage) consumer.receive();
            assertEquals("bar", msg2.getObject());
            BytesMessage msg3 = (BytesMessage) consumer.receive();
            assertEquals(1234, msg3.readInt());
            StreamMessage msg4 = (StreamMessage) consumer.receive();
            assertEquals(1234l, msg4.readLong());
            MapMessage msg5 = (MapMessage) consumer.receive();
            assertEquals(true, msg5.getBoolean("foo"));
            assertEquals("test", msg5.getString("bar"));
            Message msg6 = consumer.receive();

            assertEquals((byte) 1, msg6.getByteProperty("a"));
            assertEquals(123232323233L, msg6.getLongProperty("b"));
            assertEquals(1232323, msg6.getIntProperty("c"));
            assertEquals("ttt", msg6.getStringProperty("d"));
            assertEquals(true, msg6.getBooleanProperty("e"));
            assertEquals(1.3f, msg6.getFloatProperty("f"), 0);
            assertEquals(1.9d, msg6.getDoubleProperty("g"), 0);
            assertEquals(89, msg6.getShortProperty("h"));
            // we are serializing Object properties as strings
            assertEquals("qqqq", msg6.getObjectProperty("i"));
          }
        }
      }
    }
  }
}
