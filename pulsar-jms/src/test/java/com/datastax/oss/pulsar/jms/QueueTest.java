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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class QueueTest {

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
  public void sendMessageReceiveFromQueue() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 = session.createConsumer(destination);
              MessageConsumer consumer2 = session.createConsumer(destination); ) {

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                producer.send(session.createTextMessage("foo-" + i));
              }
            }

            List<TextMessage> receivedFrom1 = new ArrayList<>();
            List<TextMessage> receivedFrom2 = new ArrayList<>();

            while (receivedFrom1.size() + receivedFrom2.size() < 10) {
              TextMessage msg = (TextMessage) consumer1.receive(100);
              if (msg != null) {
                log.info("received {} from 1", msg.getText());
                receivedFrom1.add(msg);
              }

              msg = (TextMessage) consumer2.receive(100);
              if (msg != null) {
                log.info("received {} from 2", msg.getText());
                receivedFrom2.add(msg);
              }
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());
            assertNull(consumer2.receiveNoWait());
          }
        }
      }
    }
  }

  @Test
  public void sendJMSRedeliveryCountTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageProducer producer = session.createProducer(destination); ) {
            producer.send(session.createTextMessage("foo"));
          }

          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {
            Message message = consumer1.receive();
            assertEquals("foo", message.getBody(String.class));
            assertEquals(1, message.getIntProperty("JMSXDeliveryCount"));
            assertFalse(message.getJMSRedelivered());
          }

          // close consumer, message not acked, so it must be redelivered

          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {
            Message message = consumer1.receive();
            assertEquals("foo", message.getBody(String.class));

            // Unfortunately Pulsar does not set properly the redelivery count
            // so these assertions are testing the bad behaviour
            assertEquals(1, message.getIntProperty("JMSXDeliveryCount"));
            assertFalse(message.getJMSRedelivered());
          }
        }
      }
    }
  }

  @Test
  public void testQueueBrowsers() throws Exception {
    int numMessages = 20;
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageProducer producer = session.createProducer(destination); ) {
            for (int i = 0; i < numMessages; i++) {
              producer.send(session.createTextMessage("foo-" + i));
            }
          }

          // scan from the beginning, no one consumed messages
          try (QueueBrowser browser = session.createBrowser(destination)) {
            Enumeration en = browser.getEnumeration();
            int i = 0;
            while (en.hasMoreElements()) {
              TextMessage msg = (TextMessage) en.nextElement();
              log.info("browsed {}", msg.getText());
              assertEquals("foo-" + i, msg.getText());
              i++;
            }
            assertEquals(numMessages, i);

            try {
              en.nextElement();
              fail("should throw NoSuchElementException");
            } catch (NoSuchElementException expected) {
            }
          }

          // scan again without calling hasMoreElements explicitly
          try (QueueBrowser browser = session.createBrowser(destination)) {
            Enumeration en = browser.getEnumeration();
            for (int i = 0; i < numMessages; i++) {
              TextMessage msg = (TextMessage) en.nextElement();
              assertEquals("foo-" + i, msg.getText());
            }
            assertFalse(en.hasMoreElements());
            try {
              en.nextElement();
              fail("should throw NoSuchElementException");
            } catch (NoSuchElementException expected) {
            }
          }

          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {
            // consume half queue
            for (int i = 0; i < numMessages / 2; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();
              log.info("consume {}", msg);
              assertEquals("foo-" + i, msg.getText());
            }
          }

          // browser unconsumed messages
          try (QueueBrowser browser = session.createBrowser(destination)) {
            Enumeration en = browser.getEnumeration();
            for (int i = numMessages / 2; i < numMessages; i++) {
              TextMessage msg = (TextMessage) en.nextElement();
              assertEquals("foo-" + i, msg.getText());
            }
          }

          // consume the rest of the queue
          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {
            // consume half queue
            for (int i = numMessages / 2; i < numMessages; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();
              log.info("consume2 {} {}", msg, msg.getJMSMessageID());
              assertEquals("foo-" + i, msg.getText());
            }
            assertNull(consumer1.receiveNoWait());
          }

          // now the queue is empty (but Pulsar "peek" still returns the last consumed message in
          // this case)
          try (QueueBrowser browser = session.createBrowser(destination)) {
            Enumeration en = browser.getEnumeration();
            TextMessage msg = (TextMessage) en.nextElement();
            log.info("next {} {}", msg, msg.getJMSMessageID());
            assertEquals("foo-" + (numMessages - 1), msg.getText());
            assertFalse(en.hasMoreElements());
          }
          // validate again this kind of bug
          try (QueueBrowser browser = session.createBrowser(destination)) {
            Enumeration en = browser.getEnumeration();
            TextMessage msg = (TextMessage) en.nextElement();
            log.info("next {} {}", msg, msg.getJMSMessageID());
            assertEquals("foo-" + (numMessages - 1), msg.getText());
            assertFalse(en.hasMoreElements());
          }

          // still validate that the queue is empty
          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {
            assertNull(consumer1.receive(1000));
          }

          // browse a brand new empty queue
          Queue destinationEmpty =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());
          try (QueueBrowser browser = session.createBrowser(destinationEmpty)) {
            Enumeration en = browser.getEnumeration();
            assertFalse(en.hasMoreElements());
            try {
              en.nextElement();
              fail("should throw NoSuchElementException");
            } catch (NoSuchElementException expected) {
            }
          }
        }
      }
    }
  }
}
