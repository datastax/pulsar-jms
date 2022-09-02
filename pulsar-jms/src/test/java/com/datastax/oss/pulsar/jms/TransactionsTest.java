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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import com.google.common.collect.ImmutableMap;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class TransactionsTest {

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
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();

        try (Session consumerSession = connection.createSession(); ) {
          Destination destination =
              consumerSession.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          try (MessageConsumer consumer = consumerSession.createConsumer(destination)) {

            try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

              try (MessageProducer producer = transaction.createProducer(destination); ) {
                TextMessage textMsg = transaction.createTextMessage("foo");
                producer.send(textMsg);
                producer.send(textMsg);
              }

              // message is not "visible" as transaction is not committed
              assertNull(consumer.receive(1000));
              transaction.commit();

              // message is now visible to consumers
              assertNotNull(consumer.receive());
              assertNotNull(consumer.receive());
            }
          }
        }
      }
    }
  }

  @Test
  public void autoRollbackTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();

        try (Session consumerSession = connection.createSession(); ) {
          Destination destination =
              consumerSession.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          try (MessageConsumer consumer = consumerSession.createConsumer(destination)) {

            try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

              try (MessageProducer producer = transaction.createProducer(destination); ) {
                TextMessage textMsg = transaction.createTextMessage("foo");
                producer.send(textMsg);
              }

              // message is not "visible" as transaction is not committed
              assertNull(consumer.receive(1000));

              // session closed -> auto rollback
            }
            // message is lost
            assertNull(consumer.receive(1000));
          }
        }
      }
    }
  }

  @Test
  public void rollbackProduceTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();

        try (Session consumerSession = connection.createSession(); ) {
          Destination destination =
              consumerSession.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          try (MessageConsumer consumer = consumerSession.createConsumer(destination)) {

            try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

              try (MessageProducer producer = transaction.createProducer(destination); ) {
                TextMessage textMsg = transaction.createTextMessage("foo");
                producer.send(textMsg);
              }

              // message is not "visible" as transaction is not committed
              assertNull(consumer.receive(1000));

              transaction.rollback();

              // message is lost
              assertNull(consumer.receive(1000));
            }
          }
        }
      }
    }
  }

  @Test
  public void consumeTransactionTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();

        try (Session producerSession = connection.createSession(); ) {
          Destination destination =
              producerSession.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

            try (MessageConsumer consumer = transaction.createConsumer(destination); ) {

              try (MessageProducer producer = producerSession.createProducer(destination); ) {
                TextMessage textMsg = producerSession.createTextMessage("foo");
                producer.send(textMsg);
              }

              Message receive = consumer.receive();
              assertEquals("foo", receive.getBody(String.class));
            }

            transaction.commit();

            // message has been committed by the transacted session
            try (MessageConsumer consumer = producerSession.createConsumer(destination); ) {
              assertNull(consumer.receive(1000));
            }
          }
        }
      }
    }
  }

  @Test
  public void multiCommitTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();

        try (Session producerSession = connection.createSession(); ) {
          Destination destination =
              producerSession.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

            try (MessageConsumer consumer = transaction.createConsumer(destination); ) {

              try (MessageProducer producer = producerSession.createProducer(destination); ) {
                producer.send(producerSession.createTextMessage("foo0"));
                producer.send(producerSession.createTextMessage("foo1"));
              }

              Message receive = consumer.receive();
              assertEquals("foo0", receive.getBody(String.class));
              transaction.commit();

              receive = consumer.receive();
              assertEquals("foo1", receive.getBody(String.class));
              transaction.commit();
            }

            // messages have been committed by the transacted session
            try (MessageConsumer consumer = producerSession.createConsumer(destination); ) {
              assertNull(consumer.receive(1000));
            }
          }
        }
      }
    }
  }

  @Test
  public void consumeRollbackTransactionTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("ackReceiptEnabled", true);
    properties.put("consumerConfig", consumerConfig);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();

        try (Session producerSession = connection.createSession(); ) {
          Destination destination =
              producerSession.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

            try (MessageConsumer consumer = transaction.createConsumer(destination); ) {

              try (MessageProducer producer = producerSession.createProducer(destination); ) {
                TextMessage textMsg = producerSession.createTextMessage("foo");
                producer.send(textMsg);
              }

              Message receive = consumer.receive();
              assertEquals("foo", receive.getBody(String.class));
            }

            // rollback transaction AFTER closing the Consumer
            transaction.rollback();

            // the consumer rolledback the transaction, now we can receive the message from
            // another client
            try (MessageConsumer consumer = producerSession.createConsumer(destination); ) {
              assertNotNull(consumer.receive());
            }
          }
        }
      }
    }
  }

  @Test
  public void consumeRollbackTransaction2Test() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("ackReceiptEnabled", true);
    properties.put("consumerConfig", consumerConfig);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();

        try (Session producerSession = connection.createSession(); ) {
          Destination destination =
              producerSession.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

            try (MessageConsumer consumer = transaction.createConsumer(destination); ) {

              try (MessageProducer producer = producerSession.createProducer(destination); ) {
                TextMessage textMsg = producerSession.createTextMessage("foo");
                producer.send(textMsg);
              }

              Message receive = consumer.receive();
              assertEquals("foo", receive.getBody(String.class));

              // rollback before closing Consumer
              transaction.rollback();
            }

            // the consumer rolledback the transaction, now we can receive the message from
            // another client
            try (MessageConsumer consumer = producerSession.createConsumer(destination); ) {
              assertNotNull(consumer.receive());
            }
          }
        }
      }
    }
  }

  @Test
  public void consumeAutoRollbackTransactionTestWithQueueBrowser() throws Exception {

    int numMessages = 10;
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection();
          Connection connection2 = factory.createConnection()) {
        connection.start();

        try (Session producerSession = connection.createSession(); ) {
          Queue destination =
              producerSession.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

            try (MessageConsumer consumer = transaction.createConsumer(destination); ) {

              try (MessageProducer producer = producerSession.createProducer(destination); ) {
                for (int i = 0; i < numMessages; i++) {
                  TextMessage textMsg = producerSession.createTextMessage("foo" + i);
                  producer.send(textMsg);
                }
              }

              try (QueueBrowser counter = producerSession.createBrowser(destination)) {
                int count = 0;
                for (Enumeration e = counter.getEnumeration(); e.hasMoreElements(); ) {
                  TextMessage msg = (TextMessage) e.nextElement();
                  assertEquals("foo" + count, msg.getText());
                  count++;
                }
                assertEquals(numMessages, count);
              }

              // transactional consumer, receives but it does not commit
              Message receive = consumer.receive();
              assertEquals("foo0", receive.getBody(String.class));

              // the QueueBrowser still sees the message
              try (QueueBrowser counter = producerSession.createBrowser(destination)) {
                int count = 0;
                for (Enumeration e = counter.getEnumeration(); e.hasMoreElements(); ) {
                  TextMessage msg = (TextMessage) e.nextElement();
                  assertEquals("foo" + count, msg.getText());
                  count++;
                }
                assertEquals(numMessages, count);
              }
            }

            connection.close();

            connection2.start();

            try (Session secondSession = connection2.createSession();
                MessageConsumer consumer = secondSession.createConsumer(destination); ) {
              assertNotNull(consumer.receive());

              // it looks like peekMessage is not following the subscription in realtime
              Thread.sleep(2000);

              // the QueueBrowser does not see the consumed message anymore
              try (QueueBrowser counter = secondSession.createBrowser(destination)) {
                // skip first message
                int count = 1;
                for (Enumeration e = counter.getEnumeration(); e.hasMoreElements(); ) {
                  TextMessage msg = (TextMessage) e.nextElement();
                  assertEquals("foo" + count, msg.getText());
                  count++;
                }
                assertEquals(numMessages, count);
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void rollbackReceivedMessages() throws Exception {

    int numMessages = 10;
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection();
          Connection connection2 = factory.createConnection()) {
        connection.start();

        try (Session producerSession = connection.createSession(); ) {
          Queue destination =
              producerSession.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

            try (MessageConsumer consumer = transaction.createConsumer(destination); ) {

              try (MessageProducer producer = producerSession.createProducer(destination); ) {
                for (int i = 0; i < numMessages; i++) {
                  TextMessage textMsg = producerSession.createTextMessage("foo" + i);
                  producer.send(textMsg);
                }
              }

              TextMessage receive = (TextMessage) consumer.receive();
              log.info("receive and commit {}", receive.getText());
              assertEquals(numMessages, countMessages(producerSession, destination));
              transaction.commit();
              assertEquals(numMessages - 1, countMessages(producerSession, destination));

              receive = (TextMessage) consumer.receive();
              log.info("receive and rollback {}", receive.getText());
              transaction.rollback();
              assertEquals(numMessages - 1, countMessages(producerSession, destination));

              receive = (TextMessage) consumer.receive();

              log.info("receive {}", receive.getText());
              assertEquals(numMessages - 1, countMessages(producerSession, destination));
              log.info("commit final");
              transaction.commit();
              assertEquals(numMessages - 2, countMessages(producerSession, destination));
            }
          }
        }
      }
    }
  }

  private static int countMessages(Session producerSession, Queue destination) throws JMSException {
    int count = 0;
    try (QueueBrowser counter = producerSession.createBrowser(destination)) {
      for (Enumeration e = counter.getEnumeration(); e.hasMoreElements(); ) {
        TextMessage msg = (TextMessage) e.nextElement();
        log.info("count {} msg {}", count, msg.getText());
        count++;
      }
    }
    return count;
  }

  @Test
  public void consumeRollbackTransactionTestWithQueueBrowser() throws Exception {

    int numMessages = 10;
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection();
          Connection connection2 = factory.createConnection()) {
        connection.start();

        try (Session producerSession = connection.createSession(); ) {
          Queue destination =
              producerSession.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

            try (MessageConsumer consumer = transaction.createConsumer(destination); ) {

              try (MessageProducer producer = producerSession.createProducer(destination); ) {
                for (int i = 0; i < numMessages; i++) {
                  TextMessage textMsg = producerSession.createTextMessage("foo" + i);
                  producer.send(textMsg);
                }
              }

              try (QueueBrowser counter = producerSession.createBrowser(destination)) {
                int count = 0;
                for (Enumeration e = counter.getEnumeration(); e.hasMoreElements(); ) {
                  TextMessage msg = (TextMessage) e.nextElement();
                  assertEquals("foo" + count, msg.getText());
                  count++;
                }
                assertEquals(numMessages, count);
              }

              // transactional consumer, receives but it does not commit
              Message receive = consumer.receive();
              assertEquals("foo0", receive.getBody(String.class));

              // the QueueBrowser still sees the message
              try (QueueBrowser counter = producerSession.createBrowser(destination)) {
                int count = 0;
                for (Enumeration e = counter.getEnumeration(); e.hasMoreElements(); ) {
                  TextMessage msg = (TextMessage) e.nextElement();
                  assertEquals("foo" + count, msg.getText());
                  count++;
                }
                assertEquals(numMessages, count);
              }
            }

            transaction.rollback();

            connection2.start();

            try (Session secondSession = connection2.createSession();
                MessageConsumer consumer = secondSession.createConsumer(destination); ) {
              assertNotNull(consumer.receive());

              // it looks like peekMessage is not following the subscription in realtime
              Thread.sleep(2000);

              // the QueueBrowser does not see the consumed message anymore
              try (QueueBrowser counter = secondSession.createBrowser(destination)) {
                // skip first message
                int count = 1;
                for (Enumeration e = counter.getEnumeration(); e.hasMoreElements(); ) {
                  TextMessage msg = (TextMessage) e.nextElement();
                  assertEquals("foo" + count, msg.getText());
                  count++;
                }
                assertEquals(numMessages, count);
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void sendMessageJMSContextTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext(JMSContext.SESSION_TRANSACTED)) {
        Destination destination =
            context.createQueue("persistent://public/default/test-" + UUID.randomUUID());
        int numMessages = 10;
        int sendMsgCounter = 0;
        for (int i = 0; i < numMessages; i++) {
          context.createProducer().send(destination, "foo-" + sendMsgCounter);
          sendMsgCounter++;
        }
        JMSConsumer consumer = context.createConsumer(destination);

        // Call rollback() to rollback the sent messages
        context.rollback();

        assertNull((TextMessage) consumer.receive(1000));

        for (int i = 0; i < numMessages; i++) {
          context.createProducer().send(destination, "foo" + sendMsgCounter);
          sendMsgCounter++;
        }

        // Call commit() to commit the sent messages
        context.commit();

        int receiveCount = 10;
        for (int i = 0; i < numMessages; i++) {
          Message received = consumer.receive(1000);
          assertNotNull(received);
          assertEquals("foo" + receiveCount, received.getBody(String.class));
          receiveCount++;
        }

        // no more messages
        assertNull((TextMessage) consumer.receive(1000));

        // acknowledge
        context.commit();
      }
    }
  }

  @Test
  public void sendMessageWithBatchingTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batchingEnabled", true);
    properties.put("producerConfig", producerConfig);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();

        try (Session consumerSession = connection.createSession(); ) {
          Destination destination =
              consumerSession.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          try (MessageConsumer consumer = consumerSession.createConsumer(destination)) {

            try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {

              try (MessageProducer producer = transaction.createProducer(destination); ) {
                TextMessage textMsg = transaction.createTextMessage("foo");
                producer.send(
                    textMsg,
                    new CompletionListener() {
                      @Override
                      public void onCompletion(Message message) {}

                      @Override
                      public void onException(Message message, Exception e) {}
                    });
                producer.send(
                    textMsg,
                    new CompletionListener() {
                      @Override
                      public void onCompletion(Message message) {}

                      @Override
                      public void onException(Message message, Exception e) {}
                    });
              }

              // message is not "visible" as transaction is not committed
              assertNull(consumer.receive(1000));

              transaction.commit();

              // message is now visible to consumers

              // verify that the two messages are part of the same batch
              PulsarMessage message1 = (PulsarMessage) consumer.receive();
              org.apache.pulsar.client.api.Message<?> receivedPulsarMessage1 =
                  message1.getReceivedPulsarMessage();
              BatchMessageIdImpl messageId1 =
                  (BatchMessageIdImpl) receivedPulsarMessage1.getMessageId();

              PulsarMessage message2 = (PulsarMessage) consumer.receive();
              org.apache.pulsar.client.api.Message<?> receivedPulsarMessage2 =
                  message2.getReceivedPulsarMessage();
              BatchMessageIdImpl messageId2 =
                  (BatchMessageIdImpl) receivedPulsarMessage2.getMessageId();
              log.info("ids {} {}", messageId1, messageId2);

              assertEquals(messageId1.getLedgerId(), messageId2.getLedgerId());
              assertEquals(messageId1.getEntryId(), messageId2.getEntryId());
              assertEquals(messageId1.getBatchIndex() + 1, messageId2.getBatchIndex());
            }
          }
        }
      }
    }
  }

  @Test
  public void emulatedTransactionsTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "false");
    properties.put("jms.emulateTransactions", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();

        try (Session consumerSession = connection.createSession(Session.SESSION_TRANSACTED); ) {
          Destination destination =
              consumerSession.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          try (MessageConsumer consumer = consumerSession.createConsumer(destination)) {

            try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED); ) {
              assertTrue(transaction.getTransacted());
              try (MessageProducer producer = transaction.createProducer(destination); ) {
                TextMessage textMsg = transaction.createTextMessage("foo");
                producer.send(textMsg);
                producer.send(textMsg);
              }

              transaction.commit();

              assertNotNull(consumer.receive());
              assertNotNull(consumer.receive());

              consumerSession.commit();
            }
          }
        }
      }
    }
  }

  @Test
  public void messageListenerTest() throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();

        try (Session consumerSession = connection.createSession(Session.SESSION_TRANSACTED); ) {
          Destination destination =
              consumerSession.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          List<Message> received = new CopyOnWriteArrayList<>();
          try (MessageConsumer consumer = consumerSession.createConsumer(destination)) {
            consumer.setMessageListener(
                new MessageListener() {
                  @Override
                  public void onMessage(Message message) {
                    log.info("Received message {}", message);
                    received.add(message);
                  }
                });

            try (Session producerSession = connection.createSession(Session.SESSION_TRANSACTED); ) {

              try (MessageProducer producer = producerSession.createProducer(destination); ) {
                TextMessage textMsg = producerSession.createTextMessage("foo");
                producer.send(textMsg);
              }

              // message is not "visible" as producer transaction is not committed
              Awaitility.await().during(4, TimeUnit.SECONDS).until(() -> received.isEmpty());

              producerSession.commit();

              // message is now visible to consumers
              Awaitility.await().until(() -> !received.isEmpty());

              received.clear();

              // rollback
              consumerSession.rollback();

              // receive the message again
              Awaitility.await().until(() -> !received.isEmpty());

              received.clear();
              consumerSession.commit();

              // verify no message is received anymore
              Awaitility.await().during(4, TimeUnit.SECONDS).until(() -> received.isEmpty());

              // verify no other consumer is able to receive the message
              try (Session otherConsumer = connection.createSession(Session.AUTO_ACKNOWLEDGE);
                  MessageConsumer consumer1 = otherConsumer.createConsumer(destination)) {
                assertNull(consumer1.receive(1000));
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void messageListenerWithEmulatedTransactionsTest() throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "false");
    properties.put("jms.emulateTransactions", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();

        try (Session consumerSession = connection.createSession(Session.SESSION_TRANSACTED); ) {
          Destination destination =
              consumerSession.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          List<Message> received = new CopyOnWriteArrayList<>();
          try (MessageConsumer consumer = consumerSession.createConsumer(destination)) {
            consumer.setMessageListener(
                new MessageListener() {
                  @Override
                  public void onMessage(Message message) {
                    log.info("Received message {}", message);
                    received.add(message);
                  }
                });

            try (Session producerSession = connection.createSession(Session.SESSION_TRANSACTED); ) {

              try (MessageProducer producer = producerSession.createProducer(destination); ) {
                TextMessage textMsg = producerSession.createTextMessage("foo");
                producer.send(textMsg);
              }

              // message is "visible" as producer transaction is not committed but
              // we are only emulating transactions and so the message is sent immediately
              Awaitility.await().until(() -> !received.isEmpty());

              // commit producer (useless in this case)
              producerSession.commit();

              received.clear();

              // rollback the consumer session
              consumerSession.rollback();

              // receive the message again
              Awaitility.await().until(() -> !received.isEmpty());

              received.clear();
              consumerSession.commit();

              // verify no message is received anymore
              Awaitility.await().during(4, TimeUnit.SECONDS).until(() -> received.isEmpty());

              // verify no other consumer is able to receive the message
              try (Session otherConsumer = connection.createSession(Session.AUTO_ACKNOWLEDGE);
                  MessageConsumer consumer1 = otherConsumer.createConsumer(destination)) {
                assertNull(consumer1.receive(1000));
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void consumeProduceScenario() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    properties.put("jms.clientId", "my-id");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();

        try (Session someSession = connection.createSession(); ) {
          String name = "persistent://public/default/test" + UUID.randomUUID();
          Destination destination = someSession.createQueue(name);

          // use another consumer to read the topic out of the transaction
          // we use a named subscription, in JMS it is a durableConsumer on a JMS Topic
          Topic topic = someSession.createTopic(name);

          try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED);
              Session otherSession = connection.createSession(Session.AUTO_ACKNOWLEDGE);
              MessageConsumer consumerOtherSession =
                  otherSession.createDurableConsumer(topic, "other-sub")) {

            String sentMessageID;
            try (MessageConsumer transactionConsumer = transaction.createConsumer(destination); ) {

              try (MessageProducer transactionProducer =
                  transaction.createProducer(destination); ) {
                TextMessage textMsg = someSession.createTextMessage("foo");
                transactionProducer.send(textMsg);
                log.info("sent {}", textMsg.getJMSMessageID());
                // the message ID is assigned during "send"
                sentMessageID = textMsg.getJMSMessageID();
                transaction.commit();
              }

              Message receive = transactionConsumer.receive();
              assertEquals("foo", receive.getBody(String.class));
              log.info("received {}", receive.getJMSMessageID());
              assertEquals(sentMessageID, receive.getJMSMessageID());
            }
            ;
            transaction.commit();

            Message receiveOtherSession = consumerOtherSession.receive();
            assertEquals("foo", receiveOtherSession.getBody(String.class));
            log.info("receivedOtherSession {}", receiveOtherSession.getJMSMessageID());
            assertEquals(sentMessageID, receiveOtherSession.getJMSMessageID());

            // message has been committed by the transacted session
            try (MessageConsumer consumer = someSession.createConsumer(destination); ) {
              assertNull(consumer.receive(1000));
            }
          }
        }
      }
    }
  }

  @Test
  public void testMixedProducesScenario() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    properties.put("jms.clientId", "my-id");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();

        try (Session someSession = connection.createSession(); ) {
          String name = "persistent://public/default/test" + UUID.randomUUID();
          Destination destination = someSession.createQueue(name);

          // use another consumer to read the topic out of the transaction
          // we use a named subscription, in JMS it is a durableConsumer on a JMS Topic
          Topic topic = someSession.createTopic(name);

          try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED);
              Session otherSession = connection.createSession(Session.AUTO_ACKNOWLEDGE);
              MessageConsumer consumerOtherSession =
                  otherSession.createDurableConsumer(topic, "other-sub")) {

            String sentMessageID;
            String sentMessageID2;
            try (MessageConsumer transactionConsumer = transaction.createConsumer(destination); ) {

              try (MessageProducer transactionProducer1 = transaction.createProducer(destination);
                  MessageProducer producerNoTransaction =
                      otherSession.createProducer(destination); ) {

                TextMessage textMsg = someSession.createTextMessage("foo1");
                transactionProducer1.send(textMsg);
                log.info("sent {}", textMsg.getJMSMessageID());
                // the message ID is assigned during "send"
                sentMessageID = textMsg.getJMSMessageID();

                TextMessage textMsg2 = someSession.createTextMessage("foo2");
                producerNoTransaction.send(textMsg2);
                log.info("sent {}", textMsg2.getJMSMessageID());
                // the message ID is assigned during "send"
                sentMessageID2 = textMsg2.getJMSMessageID();

                transaction.commit();
              }

              Message receive = transactionConsumer.receive();
              assertEquals("foo1", receive.getBody(String.class));
              log.info("received {}", receive.getJMSMessageID());
              assertEquals(sentMessageID, receive.getJMSMessageID());

              Message receive2 = transactionConsumer.receive();
              assertEquals("foo2", receive2.getBody(String.class));
              log.info("received {}", receive2.getJMSMessageID());
              assertEquals(sentMessageID2, receive2.getJMSMessageID());
            }

            transaction.commit();

            Message receiveOtherSession = consumerOtherSession.receive();
            assertEquals("foo1", receiveOtherSession.getBody(String.class));
            log.info("receivedOtherSession {}", receiveOtherSession.getJMSMessageID());
            assertEquals(sentMessageID, receiveOtherSession.getJMSMessageID());

            receiveOtherSession = consumerOtherSession.receive();
            assertEquals("foo2", receiveOtherSession.getBody(String.class));
            log.info("receivedOtherSession {}", receiveOtherSession.getJMSMessageID());
            assertEquals(sentMessageID2, receiveOtherSession.getJMSMessageID());
          }
        }
      }
    }
  }

  @Test
  public void testMixedConsumersOnSharedSubscription() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    properties.put("jms.clientId", "my-id");
    properties.put(
        "consumerConfig",
        ImmutableMap.of(
            "receiverQueueSize", 1,
            "ackTimeoutMillis", 1000));
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();

        try (Session someSession = connection.createSession(); ) {
          String name = "persistent://public/default/test" + UUID.randomUUID();
          Topic destination = someSession.createTopic(name);

          try (Session transaction = connection.createSession(Session.SESSION_TRANSACTED);
              Session otherSession = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
              MessageConsumer consumerOtherSession =
                  otherSession.createSharedDurableConsumer(destination, "the-shared-sub");
              MessageConsumer transactionConsumer =
                  transaction.createSharedDurableConsumer(destination, "the-shared-sub"); ) {
            try (MessageProducer producerNoTransaction =
                otherSession.createProducer(destination); ) {

              TextMessage textMsg = someSession.createTextMessage("foo1");
              producerNoTransaction.send(textMsg);

              TextMessage textMsg2 = someSession.createTextMessage("foo2");
              producerNoTransaction.send(textMsg2);
            }

            // this looks simple but it isn't simple indeed
            // because the broker may send the message to the second consumer
            // but if we don't ack it and actTimeoutMillis elapses
            // the message is automatically negativelity acknoledged
            // and the broker dispatches the message again
            // so eventually the message will be received by the first consumer
            Message receive = transactionConsumer.receive();
            Message receive2 = consumerOtherSession.receive();
            receive2.acknowledge();

            transaction.rollback();
            // closing consumer 1 in order to speed up the test
            transactionConsumer.close();

            // the first message one is to be delivered again
            Message receive3 = consumerOtherSession.receive();
            receive3.acknowledge();

            assertEquals(receive.getBody(String.class), receive3.getBody(String.class));
            assertEquals(receive.getJMSMessageID(), receive3.getJMSMessageID());
          }
        }
      }
    }
  }

  @Test
  public void sendMessageWithPartitionStickKeyTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("enableTransaction", "true");
    properties.put("jms.transactionsStickyPartitions", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      String topicName = "persistent://public/default/test-" + UUID.randomUUID();

      try (Connection connection = factory.createConnection()) {
        connection.start();

        // create a topic with 4 partitions
        factory.getPulsarAdmin().topics().createPartitionedTopic(topicName, 4);

        try (Session consumerSession = connection.createSession(); ) {
          Destination destination = consumerSession.createQueue(topicName);

          // each session will have a stickyKey id, that is incremental
          // so the test case is consistently sending the messages
          // to the same partitions
          try (MessageConsumer consumer = consumerSession.createConsumer(destination);
              Session transaction1 = connection.createSession(Session.SESSION_TRANSACTED);
              Session transaction2 = connection.createSession(Session.SESSION_TRANSACTED);
              Session transaction3 = connection.createSession(Session.SESSION_TRANSACTED);
              Session transaction4 = connection.createSession(Session.SESSION_TRANSACTED);
              MessageProducer producer1 = transaction1.createProducer(destination);
              MessageProducer producer2 = transaction2.createProducer(destination);
              MessageProducer producer3 = transaction3.createProducer(destination);
              MessageProducer producer4 = transaction4.createProducer(destination); ) {

            for (int i = 0; i < 10; i++) {
              producer1.send(transaction1.createTextMessage("foo1"));
              producer2.send(transaction2.createTextMessage("foo2"));
              producer3.send(transaction3.createTextMessage("foo3"));
              producer4.send(transaction4.createTextMessage("foo4"));
            }

            transaction1.commit();
            transaction2.commit();
            transaction3.commit();
            transaction4.commit();

            Map<String, List<Message>> messagesByPartition = new HashMap<>();
            for (int i = 0; i < 10 * 4; i++) {
              PulsarMessage message = (PulsarMessage) consumer.receive();
              String receivedTopicName = message.getReceivedPulsarMessage().getTopicName();
              log.info("message {} {}", receivedTopicName, message);
              messagesByPartition
                  .computeIfAbsent(receivedTopicName, (t) -> new ArrayList<>())
                  .add(message);
              message.acknowledge();
            }
            assertEquals(messagesByPartition.size(), 4);
            messagesByPartition
                .entrySet()
                .forEach(
                    entry -> {
                      assertEquals(10, entry.getValue().size());
                      // verify that all the messages on the topic
                      // have been sent by the same producer (same content)
                      try {
                        String first = entry.getValue().get(0).getBody(String.class);
                        for (Message msg : entry.getValue()) {
                          assertEquals(first, msg.getBody(String.class));
                        }
                      } catch (JMSException err) {
                        throw new RuntimeException(err);
                      }
                    });

            messagesByPartition.clear();

            try (Session transaction5 = connection.createSession(Session.SESSION_TRANSACTED);
                MessageProducer producer5 = transaction5.createProducer(destination); ) {
              // now we use only one transaction,
              // but after every commit we must go to a different partition
              for (int i = 0; i < 8; i++) {
                producer5.send(transaction5.createTextMessage("foo1"));
                transaction5.commit();
              }

              for (int i = 0; i < 8; i++) {
                PulsarMessage message = (PulsarMessage) consumer.receive();
                String receivedTopicName = message.getReceivedPulsarMessage().getTopicName();
                log.info("messageAfter {} {}", receivedTopicName, message);
                messagesByPartition
                    .computeIfAbsent(receivedTopicName, (t) -> new ArrayList<>())
                    .add(message);
                message.acknowledge();
              }
              assertEquals(messagesByPartition.size(), 4);
              messagesByPartition
                  .entrySet()
                  .forEach(
                      entry -> {
                        assertEquals(2, entry.getValue().size());
                      });

              messagesByPartition.clear();
            }
          }
        }
      }
    }
  }
}
