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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
public class MessageListenerTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

  @ParameterizedTest(name = "sessionListenersThreads {0}")
  @ValueSource(ints = {0, 4})
  public void receiveWithListener(int sessionListenersThreads) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.sessionListenersThreads", sessionListenersThreads);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {
            SimpleMessageListener listener = new SimpleMessageListener();
            consumer1.setMessageListener(listener);

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                producer.send(session.createTextMessage("foo-" + i));
              }
            }
            // wait for messages to arrive
            await().until(listener.receivedMessages::size, equalTo(10));

            for (int i = 0; i < 10; i++) {
              assertEquals("foo-" + i, listener.receivedMessages.get(i).getBody(String.class));
            }
          }
        }
      }
    }
  }

  @ParameterizedTest(name = "sessionListenersThreads {0}")
  @ValueSource(ints = {0, 4})
  public void listenerForbiddenMethods(int sessionListenersThreads) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.sessionListenersThreads", sessionListenersThreads);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {
            CompletableFuture<Message> res = new CompletableFuture<>();
            MessageListener listener =
                new MessageListener() {
                  @Override
                  public void onMessage(Message message) {
                    try {
                      session.close();
                      res.complete(message);
                    } catch (Throwable t) {
                      res.completeExceptionally(t);
                    }
                  }
                };
            consumer1.setMessageListener(listener);

            try (MessageProducer producer = session.createProducer(destination); ) {
              producer.send(session.createTextMessage("foo"));
            }

            try {
              res.get();
            } catch (ExecutionException err) {
              assertThat(err.getCause(), instanceOf(IllegalStateException.class));
            }
          }

          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {
            CompletableFuture<Message> res = new CompletableFuture<>();
            MessageListener listener =
                new MessageListener() {
                  @Override
                  public void onMessage(Message message) {
                    try {
                      connection.stop();
                      res.complete(message);
                    } catch (Throwable t) {
                      res.completeExceptionally(t);
                    }
                  }
                };
            consumer1.setMessageListener(listener);

            try (MessageProducer producer = session.createProducer(destination); ) {
              producer.send(session.createTextMessage("foo"));
            }

            try {
              res.get();
            } catch (ExecutionException err) {
              assertThat(err.getCause(), instanceOf(IllegalStateException.class));
            }
          }
        }
      }
    }
  }

  @ParameterizedTest(name = "sessionListenersThreads {0}")
  @ValueSource(ints = {0, 4})
  public void multipleListenersSameSession(int sessionListenersThreads) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.sessionListenersThreads", sessionListenersThreads);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          Queue destination2 =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 = session.createConsumer(destination);
              MessageConsumer consumer2 = session.createConsumer(destination2); ) {
            SimpleMessageListener listener1 = new SimpleMessageListener();
            consumer1.setMessageListener(listener1);

            SimpleMessageListener listener2 = new SimpleMessageListener();
            consumer2.setMessageListener(listener2);

            try (MessageProducer producer = session.createProducer(null); ) {
              for (int i = 0; i < 10; i++) {
                producer.send(destination, session.createTextMessage("foo-" + i + "-1"));
                producer.send(destination2, session.createTextMessage("foo-" + i + "-2"));
              }
            }
            // wait for messages to arrive
            await().until(listener1.receivedMessages::size, equalTo(10));
            await().until(listener2.receivedMessages::size, equalTo(10));

            for (int i = 0; i < 10; i++) {
              assertEquals(
                  "foo-" + i + "-1", listener1.receivedMessages.get(i).getBody(String.class));
              assertEquals(
                  "foo-" + i + "-2", listener2.receivedMessages.get(i).getBody(String.class));
            }
          }
        }
      }
    }
  }

  @ParameterizedTest(name = "sessionListenersThreads {0}")
  @ValueSource(ints = {0, 4})
  public void testJMSContextWithListener(int sessionListenersThreads) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.sessionListenersThreads", sessionListenersThreads);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext()) {

        Queue destination =
            context.createQueue("persistent://public/default/test-" + UUID.randomUUID());

        try (JMSConsumer consumer1 = context.createConsumer(destination); ) {
          SimpleMessageListener listener = new SimpleMessageListener();
          consumer1.setMessageListener(listener);

          for (int i = 0; i < 10; i++) {
            context.createProducer().send(destination, "foo-" + i);
          }

          // wait for messages to arrive
          await().until(listener.receivedMessages::size, equalTo(10));

          for (int i = 0; i < 10; i++) {
            assertEquals("foo-" + i, listener.receivedMessages.get(i).getBody(String.class));
          }
        }
      }
    }
  }

  @ParameterizedTest(name = "sessionListenersThreads {0}")
  @ValueSource(ints = {0, 4})
  public void testJMSContextWithListenerBadMethods(int sessionListenersThreads) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.sessionListenersThreads", sessionListenersThreads);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {

      try (JMSContext context2 = factory.createContext()) {

        try (JMSContext context = factory.createContext()) {
          Queue destination =
              context2.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (JMSConsumer consumer1 = context.createConsumer(destination); ) {
            CompletableFuture<Message> res = new CompletableFuture<>();
            MessageListener listener =
                new MessageListener() {
                  @Override
                  public void onMessage(Message message) {
                    try {
                      context.close();
                      res.complete(message);
                    } catch (Throwable t) {
                      res.completeExceptionally(t);
                    }
                  }
                };
            consumer1.setMessageListener(listener);

            context2.createProducer().send(destination, "foo");

            // context.close is not allowed in the listener
            try {
              res.get();
            } catch (ExecutionException err) {
              assertThat(err.getCause(), instanceOf(IllegalStateRuntimeException.class));
            }
          }

          try (JMSConsumer consumer2 = context2.createConsumer(destination); ) {
            CompletableFuture<Message> res = new CompletableFuture<>();
            MessageListener listener =
                new MessageListener() {
                  @Override
                  public void onMessage(Message message) {
                    try {
                      context2.stop();
                      res.complete(message);
                    } catch (Throwable t) {
                      res.completeExceptionally(t);
                    }
                  }
                };
            consumer2.setMessageListener(listener);
            context2.createProducer().send(destination, "foo");

            // context.stop is not allowed
            try {
              res.get();
            } catch (ExecutionException err) {
              assertThat(err.getCause(), instanceOf(IllegalStateRuntimeException.class));
            }
          }
        }
      }
    }
  }

  @ParameterizedTest(name = "sessionListenersThreads {0}")
  @ValueSource(ints = {0, 4})
  public void testJMSContextAsyncCompletionListenerBadMethods(int sessionListenersThreads)
      throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.sessionListenersThreads", sessionListenersThreads);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {

      try (JMSContext context2 = factory.createContext()) {
        Queue destination =
            context2.createQueue("persistent://public/default/test-" + UUID.randomUUID());

        try (JMSContext context = factory.createContext()) {

          CompletableFuture<Message> resSendAndClose = new CompletableFuture<>();
          context2
              .createProducer()
              .setAsync(
                  new CompletionListener() {
                    @Override
                    public void onCompletion(Message message) {
                      try {
                        context.close();
                        resSendAndClose.complete(message);
                      } catch (Throwable t) {
                        resSendAndClose.completeExceptionally(t);
                      }
                    }

                    @Override
                    public void onException(Message message, Exception e) {
                      onCompletion(message);
                    }
                  })
              .send(destination, "foo");

          // context.close is not allowed in the listener
          try {
            resSendAndClose.get();
          } catch (ExecutionException err) {
            assertThat(err.getCause(), instanceOf(IllegalStateRuntimeException.class));
          }

          CompletableFuture<Message> resSendAndStop = new CompletableFuture<>();
          context2
              .createProducer()
              .setAsync(
                  new CompletionListener() {
                    @Override
                    public void onCompletion(Message message) {
                      try {
                        context.stop();
                        resSendAndStop.complete(message);
                      } catch (Throwable t) {
                        resSendAndStop.completeExceptionally(t);
                      }
                    }

                    @Override
                    public void onException(Message message, Exception e) {
                      onCompletion(message);
                    }
                  })
              .send(destination, "foo");

          // context.stop is not allowed in the listener
          try {
            resSendAndStop.get();
          } catch (ExecutionException err) {
            assertThat(err.getCause(), instanceOf(IllegalStateRuntimeException.class));
          }
        }
      }
    }
  }

  @ParameterizedTest(name = "sessionListenersThreads {0}")
  @ValueSource(ints = {0, 4})
  public void queueSendRecvMessageListenerTest(int sessionListenersThreads) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.sessionListenersThreads", sessionListenersThreads);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {

      Queue destination = new PulsarQueue("persistent://public/default/test-" + UUID.randomUUID());

      JMSContext context = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
      JMSContext contextToSendMsg = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
      JMSContext contextToCreateMsg = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE);

      JMSProducer producer = contextToSendMsg.createProducer();

      JMSConsumer consumer = context.createConsumer(destination);

      // Creates a new consumer on the specified destination that
      // will deliver messages to the specified MessageListener.
      SimpleMessageListener listener = new SimpleMessageListener();
      consumer.setMessageListener(listener);

      // send and receive TextMessage
      TextMessage expTextMessage = contextToCreateMsg.createTextMessage("foo");
      expTextMessage.setStringProperty("COM_SUN_JMS_TESTNAME", "queueSendRecvMessageListenerTest");
      producer.send(destination, expTextMessage);

      await().until(listener.receivedMessages::size, equalTo(1));

      TextMessage actTextMessage = (TextMessage) listener.receivedMessages.get(0);
      assertEquals(actTextMessage.getText(), expTextMessage.getText());

      assertNotNull(consumer.getMessageListener());
    }
  }

  @ParameterizedTest(name = "sessionListenersThreads {0}")
  @ValueSource(ints = {0, 4})
  public void closeConsumerOnMessageListener(int sessionListenersThreads) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.sessionListenersThreads", sessionListenersThreads);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {

      Queue destination = new PulsarQueue("persistent://public/default/test-" + UUID.randomUUID());

      JMSContext context = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
      JMSProducer producer = context.createProducer();
      JMSConsumer consumer = context.createConsumer(destination);

      AtomicInteger count = new AtomicInteger();
      AtomicReference<Message> received = new AtomicReference<>();
      consumer.setMessageListener(
          new MessageListener() {
            @Override
            public void onMessage(Message message) {
              count.incrementAndGet();
              consumer.close();
              received.set(message);
            }
          });

      producer.send(destination, "test");

      await().until(received::get, i -> i != null);

      TextMessage actTextMessage = (TextMessage) received.get();
      assertEquals(actTextMessage.getText(), "test");

      // the consumer is closed you cannot call "getMessageListener"
      assertThrows(IllegalStateRuntimeException.class, consumer::getMessageListener);

      producer.send(destination, "test2");

      // assert that the consumer did not receive other messages
      Thread.sleep(2000);
      assertEquals(1, count.get());
    }
  }

  @ParameterizedTest(name = "sessionListenersThreads {0}")
  @ValueSource(ints = {0, 4})
  public void messageListenerInternalError(int sessionListenersThreads) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.sessionListenersThreads", sessionListenersThreads);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {

      Queue destination = new PulsarQueue("persistent://public/default/test-" + UUID.randomUUID());

      JMSContext context = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
      JMSProducer producer = context.createProducer();
      JMSConsumer consumer = context.createConsumer(destination);

      AtomicInteger count = new AtomicInteger();
      AtomicReference<Message> received = new AtomicReference<>();
      consumer.setMessageListener(
          new MessageListener() {
            @Override
            public void onMessage(Message message) {
              log.debug("Received #" + count + " -> " + message);
              if (count.incrementAndGet() == 1) {
                throw new RuntimeException("Error !");
              }
              received.set(message);
            }
          });

      producer.send(destination, "test");

      await().until(received::get, i -> i != null);

      TextMessage actTextMessage = (TextMessage) received.get();
      assertEquals(actTextMessage.getText(), "test");
    }
  }

  @ParameterizedTest(name = "sessionListenersThreads {0}")
  @ValueSource(ints = {0, 4})
  public void closeSessionMessageListenerStops(int sessionListenersThreads) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.sessionListenersThreads", sessionListenersThreads);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
        Connection connection = factory.createConnection();
        Session session1 = connection.createSession();
        Session session2 = connection.createSession()) {
      connection.start();
      Queue destination = new PulsarQueue("persistent://public/default/test-" + UUID.randomUUID());

      MessageProducer producer = session2.createProducer(destination);

      List<Message> received = new CopyOnWriteArrayList<>();
      session1
          .createConsumer(destination)
          .setMessageListener(
              new MessageListener() {
                @Override
                public void onMessage(Message message) {
                  received.add(message);
                }
              });

      producer.send(session2.createTextMessage("test"));

      await().until(() -> received.size() == 1);
      producer.send(session2.createTextMessage("test"));
      await().until(() -> received.size() == 2);

      session1.close();

      producer.send(session2.createTextMessage("test"));

      // assert that the consumer did not receive other messages
      Thread.sleep(2000);
      assertEquals(2, received.size());
    }
  }
}
