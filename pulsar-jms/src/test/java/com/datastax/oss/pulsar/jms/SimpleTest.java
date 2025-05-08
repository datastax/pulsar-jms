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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.pulsar.jms.messages.PulsarSimpleMessage;
import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import jakarta.jms.BytesMessage;
import jakarta.jms.CompletionListener;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.JMSSecurityRuntimeException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;
import jakarta.jms.TextMessage;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

@Timeout(value = 1, unit = TimeUnit.MINUTES)
public class SimpleTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

  @Test
  public void testSystemPropertySetters() throws Exception {
    Message simpleMessage = new PulsarSimpleMessage();
    for (PulsarMessage.SystemMessageProperty prop : PulsarMessage.SystemMessageProperty.values()) {
      if (prop == PulsarMessage.SystemMessageProperty.JMSXGroupID) {
        simpleMessage.setStringProperty("JMSXGroupID", "groupId");
      } else {
        String name = prop.toString();
        assertThrows(
                IllegalArgumentException.class, () -> simpleMessage.setByteProperty(name, (byte) 1));
        assertThrows(
                IllegalArgumentException.class, () -> simpleMessage.setLongProperty(name, 123232323233L));
        assertThrows(
                IllegalArgumentException.class, () -> simpleMessage.setIntProperty(name, 1232323));
        assertThrows(
                IllegalArgumentException.class, () -> simpleMessage.setStringProperty(name, "ttt"));
        assertThrows(
                IllegalArgumentException.class, () -> simpleMessage.setBooleanProperty(name, true));
        assertThrows(
                IllegalArgumentException.class, () -> simpleMessage.setFloatProperty(name, 1.3f));
        assertThrows(
                IllegalArgumentException.class, () -> simpleMessage.setDoubleProperty(name, 1.9d));
        assertThrows(
                IllegalArgumentException.class, () -> simpleMessage.setShortProperty(name, (short) 89));
        assertThrows(
                IllegalArgumentException.class, () -> simpleMessage.setObjectProperty(name, 1.3d));
      }
    }
  }

  @Test
  public void sendMessageTest() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
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
  public void sendMessageTestJMSContext() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext()) {
        Destination destination =
            context.createTopic("persistent://public/default/test-" + UUID.randomUUID());

        TextMessage textMsg = context.createTextMessage("foo");
        context.createProducer().send(destination, textMsg);
        context.createProducer().send(destination, "foo");

        StreamMessage streamMessage = context.createStreamMessage();
        streamMessage.writeBytes("foo".getBytes(StandardCharsets.UTF_8));
        context.createProducer().send(destination, streamMessage);

        BytesMessage bytesMessage = context.createBytesMessage();
        bytesMessage.writeInt(234);
        context.createProducer().send(destination, bytesMessage);
        context.createProducer().send(destination, "foo".getBytes(StandardCharsets.UTF_8));

        Message headerOnly = context.createMessage();
        headerOnly.setBooleanProperty("myproperty", true);
        context.createProducer().send(destination, headerOnly);
        context.createProducer().send(destination, (Serializable) null);
        context.createProducer().send(destination, (byte[]) null);
        context.createProducer().send(destination, (Map<String, Object>) null);

        ObjectMessage objectMessage = context.createObjectMessage("test");
        context.createProducer().send(destination, objectMessage);
        context.createProducer().send(destination, new java.util.ArrayList<String>());

        MapMessage mapMessage = context.createMapMessage();
        mapMessage.setBoolean("p1", true);
        context.createProducer().send(destination, mapMessage);
        context.createProducer().send(destination, Collections.singletonMap("foo", "bar"));

        // CompletionListener
        Message simpleAsync = context.createMessage();
        CompletableFuture<Message> res = new CompletableFuture<>();
        context
            .createProducer()
            .setAsync(
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
                })
            .send(destination, simpleAsync);
        assertTrue(res.get() == simpleAsync);
        assertNotNull(simpleAsync.getJMSMessageID());
        assertTrue(simpleAsync.getJMSTimestamp() > 0);
        assertTrue(simpleAsync.getJMSDeliveryTime() > 0);
      }
    }
  }

  @Test
  public void sendMessageReceive() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
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
              simpleMessage.setJMSType("mytype");
              simpleMessage.setJMSCorrelationID("correlationid");
              simpleMessage.setObjectProperty("i", 1.3d);
              producer.send(simpleMessage, DeliveryMode.NON_PERSISTENT, 2, 0);

              Message simpleMessage2 = session.createMessage();
              simpleMessage2.setJMSCorrelationIDAsBytes(new byte[] {1, 2, 3});
              producer.send(simpleMessage2, DeliveryMode.PERSISTENT, 3, 0);
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
            assertEquals(2, msg6.getJMSPriority());
            assertEquals("mytype", msg6.getJMSType());
            assertEquals(DeliveryMode.NON_PERSISTENT, msg6.getJMSDeliveryMode());
            assertEquals("correlationid", msg6.getJMSCorrelationID());
            assertArrayEquals(
                "correlationid".getBytes(StandardCharsets.UTF_8),
                msg6.getJMSCorrelationIDAsBytes());
            // we are serializing Object properties as strings
            assertEquals(1.3d, msg6.getObjectProperty("i"));

            Message msg7 = consumer.receive();

            assertEquals(DeliveryMode.PERSISTENT, msg7.getJMSDeliveryMode());
            assertArrayEquals(new byte[] {1, 2, 3}, msg7.getJMSCorrelationIDAsBytes());
          }
        }
      }
    }
  }

  @Test
  public void sendMessageReceiveJMSContext() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext()) {
        Destination destination =
            context.createTopic("persistent://public/default/test-" + UUID.randomUUID());

        try (JMSConsumer consumer = context.createConsumer(destination); ) {
          JMSProducer producer = context.createProducer();
          producer.send(destination, "foo");
          producer.send(destination, (Serializable) "bar");
          // this is an example from the TCK
          ObjectMessage baz = context.createObjectMessage(new StringBuffer("baz"));
          producer.send(destination, baz);

          producer.send(destination, new byte[] {1, 2, 3});

          producer.send(destination, Collections.singletonMap("a", "b"));

          JMSProducer simpleMessage = producer;
          simpleMessage.setProperty("a", (byte) 1);
          simpleMessage.setProperty("b", 123232323233L);
          simpleMessage.setProperty("c", 1232323);
          simpleMessage.setProperty("d", "ttt");
          simpleMessage.setProperty("e", true);
          simpleMessage.setProperty("f", 1.3f);
          simpleMessage.setProperty("g", 1.9d);
          simpleMessage.setProperty("h", (short) 89);
          simpleMessage.setProperty("i", (Serializable) "qqqq");
          simpleMessage.send(destination, (Serializable) null);

          assertEquals("foo", consumer.receiveBody(String.class));
          assertEquals("bar", consumer.receiveBody(Serializable.class));
          // this is an example from the TCK
          assertEquals(
              new StringBuffer("baz").toString(),
              consumer.receiveBody(StringBuffer.class, 1000).toString());
          assertArrayEquals(new byte[] {1, 2, 3}, consumer.receiveBody(byte[].class));
          assertEquals(Collections.singletonMap("a", "b"), consumer.receiveBody(Map.class));
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

  @Test
  public void sendMessageReceiveJMSContext2ìMultipleTimes() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext()) {
        Destination destination =
            context.createQueue("persistent://public/default/test-" + UUID.randomUUID());

        try (JMSConsumer consumer = context.createConsumer(destination); ) {
          JMSProducer producer = context.createProducer();

          String message = "Where are you!";

          TextMessage expTextMessage = context.createTextMessage(message);
          expTextMessage.setStringProperty("COM_SUN_JMS_TESTNAME", "queueReceiveTests");
          producer.send(destination, expTextMessage);
          TextMessage actTextMessage = (TextMessage) consumer.receive();
          assertNotNull(actTextMessage);
          ;
          assertEquals(actTextMessage.getText(), expTextMessage.getText());

          // send and receive TextMessage again
          producer.send(destination, expTextMessage);
          actTextMessage = (TextMessage) consumer.receive(1000);
          assertNotNull(actTextMessage);
          assertEquals(actTextMessage.getText(), expTextMessage.getText());

          // send and receive TextMessage again
          producer.send(destination, expTextMessage);
          actTextMessage = (TextMessage) consumer.receiveNoWait();
          if (actTextMessage == null) {
            actTextMessage = (TextMessage) consumer.receive();
          }
          assertNotNull(actTextMessage);
          assertEquals(actTextMessage.getText(), expTextMessage.getText());

          actTextMessage = (TextMessage) consumer.receiveNoWait();
          assertNull(actTextMessage);
        }
      }
    }
  }

  @Test
  public void sendMessageTestWithDeliveryDelayJMSContext() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext()) {
        Queue destination =
            context.createQueue("persistent://public/default/test-" + UUID.randomUUID());
        try (JMSConsumer consumer = context.createConsumer(destination)) {

          context.createProducer().setDeliveryDelay(8000).send(destination, "foo");

          // message is not immediately available
          assertNull(consumer.receive(2000));

          Thread.sleep(7000);

          assertEquals("foo", consumer.receiveBody(String.class));
        }
      }
    }
  }

  @Test
  public void createSubContextTest() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext()) {
        context.createContext(JMSContext.AUTO_ACKNOWLEDGE).close();
        context.createContext(JMSContext.CLIENT_ACKNOWLEDGE).close();
        context.createContext(JMSContext.DUPS_OK_ACKNOWLEDGE).close();
      }
    }
  }

  @Test
  public void tckTest() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
        Destination destination =
            context.createQueue("persistent://public/default/test-" + UUID.randomUUID());

        String message = "Where are you!";
        StringBuffer expSbuffer = new StringBuffer("This is it!");

        // set up JmsTool for COMMON_Q setup
        Queue queue = (Queue) destination;

        // Create JMSProducer from JMSContext
        JMSProducer producer = context.createProducer();

        // Create JMSConsumer from JMSContext
        JMSConsumer consumer = context.createConsumer(destination);

        // Send and receive TextMessage
        TextMessage expTextMessage = context.createTextMessage(message);
        expTextMessage.setStringProperty("COM_SUN_JMS_TESTNAME", "queueReceiveBodyTests");
        producer.send(destination, expTextMessage);
        String actMessage = consumer.receiveBody(String.class);
        assertNotNull(actMessage);
        assertEquals(message, actMessage);

        // Send and receive ObjectMessage
        ObjectMessage expObjectMessage = context.createObjectMessage(expSbuffer);
        expObjectMessage.setStringProperty("COM_SUN_JMS_TESTNAME", "queueReceiveBodyTests");
        producer.send(destination, expObjectMessage);
        StringBuffer actSbuffer = consumer.receiveBody(StringBuffer.class, 5000);
        assertNotNull(actSbuffer);
        assertEquals(actSbuffer.toString(), expSbuffer.toString());

        // Send and receive BytesMessage
        BytesMessage bMsg = context.createBytesMessage();
        bMsg.setStringProperty("COM_SUN_JMS_TESTNAME", "queueReceiveBodyTests");
        bMsg.writeByte((byte) 1);
        bMsg.writeInt((int) 22);
        producer.send(destination, bMsg);
        byte[] bytes = consumer.receiveBody(byte[].class, 5000);
        assertNotNull(bytes);

        DataInputStream di = new DataInputStream(new ByteArrayInputStream(bytes));
        assertEquals(di.readByte(), (byte) 1);
        assertEquals(di.readInt(), 22);
        try {
          byte b = di.readByte();
          fail();
        } catch (EOFException e) {
          // expected
        }

        // Send and receive MapMessage
        MapMessage mMsg = context.createMapMessage();
        mMsg.setStringProperty("COM_SUN_JMS_TESTNAME", "queueReceiveBodyTests");
        mMsg.setBoolean("booleanvalue", true);
        mMsg.setInt("intvalue", (int) 10);
        producer.send(destination, mMsg);
        Map map = consumer.receiveBodyNoWait(Map.class);
        if (map == null) {
          for (int i = 0; i < 5; i++) {
            Thread.sleep(1000);
            map = consumer.receiveBodyNoWait(Map.class);
            if (map != null) break;
          }
        }
        assertNotNull(map);

        assertEquals(map.size(), 2);

        Iterator<String> it = map.keySet().iterator();
        String name = null;
        while (it.hasNext()) {
          name = (String) it.next();
          if (name.equals("booleanvalue")) {
            assertEquals((boolean) map.get(name), true);
          } else if (name.equals("intvalue")) {
            assertEquals((int) map.get(name), 10);
          } else {
            fail("Unexpected name of [" + name + "] in MapMessage");
          }
        }
      }
    }
  }

  @Test
  public void tckUsernamePasswordTest() throws Exception {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.tckUsername", "bob");
    properties.put("jms.tckPassword", "shhhh");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {}
      try (Connection connection = factory.createConnection()) {}
      try (JMSContext context =
          factory.createContext("bob", "shhhh", JMSContext.AUTO_ACKNOWLEDGE)) {}
      try (Connection connection = factory.createConnection("bob", "shhhh")) {}
      try (JMSContext context =
          factory.createContext("bad", "shhhh", JMSContext.AUTO_ACKNOWLEDGE)) {
        fail();
      } catch (JMSSecurityRuntimeException ok) {

      }
      try (Connection connection = factory.createConnection("bad", "shhhh")) {
        fail();
      } catch (JMSSecurityException ok) {
      }

      try (JMSContext connection = factory.createContext(null, null, JMSContext.AUTO_ACKNOWLEDGE)) {
        fail();
      } catch (JMSSecurityRuntimeException ok) {
      }
      try (Connection connection = factory.createConnection(null, null)) {
        fail();
      } catch (JMSSecurityException ok) {
      }
    }

    // new PulsarConnectionFactory
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection("invalid", "invalid")) {
        fail();
      } catch (JMSSecurityException ok) {
      }
      try (Connection connection = factory.createQueueConnection("invalid", "invalid")) {
        fail();
      } catch (JMSSecurityException ok) {
      }
      try (Connection connection = factory.createTopicConnection("invalid", "invalid")) {
        fail();
      } catch (JMSSecurityException ok) {
      }
    }
  }

  @Test
  public void systemNameSpaceTest() throws Exception {

    String simpleName = "test-" + UUID.randomUUID().toString();
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.systemNamespace", "pulsar/system");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          PulsarDestination destination =
              session.createQueue("persistent://pulsar/system/" + simpleName);

          PulsarDestination destinationWithSimpleName = session.createQueue(simpleName);

          assertEquals(destination.getName(), factory.applySystemNamespace(destination.getName()));

          try (MessageProducer producer = session.createProducer(destination); ) {
            TextMessage textMsg = session.createTextMessage("foo1");
            producer.send(textMsg);
          }

          try (MessageProducer producer = session.createProducer(destinationWithSimpleName); ) {
            TextMessage textMsg = session.createTextMessage("foo2");
            producer.send(textMsg);
          }

          try (MessageConsumer consumer = session.createConsumer(destination); ) {
            TextMessage msg1 = (TextMessage) consumer.receive();
            assertEquals("foo1", msg1.getText());
            TextMessage msg2 = (TextMessage) consumer.receive();
            assertEquals("foo2", msg2.getText());
          }
        }
      }
    }
  }
}
