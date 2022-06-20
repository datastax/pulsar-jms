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
import java.nio.file.Path;
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
}
