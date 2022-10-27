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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class ConnectionConsumerTest {

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  @BeforeAll
  public static void before() throws Exception {
    cluster =
        new PulsarCluster(
            tempDir,
            c -> {
              c.setTransactionCoordinatorEnabled(false);
            });
    cluster.start();
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  private interface ConnectionConsumerBuilder {
    ConnectionConsumer build(
        Connection connection,
        Destination destination,
        String selector,
        ServerSessionPool serverSessionPool,
        int maxMessages)
        throws Exception;
  }

  @Test
  public void createConnectionConsumerQueue() throws Exception {
    simpleTest(
        (Connection connection,
            Destination destination,
            String selector,
            ServerSessionPool serverSessionPool,
            int maxMessages) -> {
          return connection.createConnectionConsumer(
              destination, selector, serverSessionPool, maxMessages);
        },
        false);
  }

  @Test
  public void createConnectionConsumerTopic() throws Exception {
    simpleTest(
        (Connection connection,
            Destination destination,
            String selector,
            ServerSessionPool serverSessionPool,
            int maxMessages) -> {
          return connection.createConnectionConsumer(
              destination, selector, serverSessionPool, maxMessages);
        },
        true);
  }

  @Test
  public void createDurableConnectionConsumer() throws Exception {
    simpleTest(
        (Connection connection,
            Destination destination,
            String selector,
            ServerSessionPool serverSessionPool,
            int maxMessages) -> {
          return connection.createDurableConnectionConsumer(
              (Topic) destination, "subname", selector, serverSessionPool, maxMessages);
        },
        true);
  }

  @Test
  public void createSharedConnectionConsumer() throws Exception {
    simpleTest(
        (Connection connection,
            Destination destination,
            String selector,
            ServerSessionPool serverSessionPool,
            int maxMessages) -> {
          return connection.createSharedConnectionConsumer(
              (Topic) destination, "subname", selector, serverSessionPool, maxMessages);
        },
        true);
  }

  @Test
  public void createSharedDurableConnectionConsumer() throws Exception {
    simpleTest(
        (Connection connection,
            Destination destination,
            String selector,
            ServerSessionPool serverSessionPool,
            int maxMessages) -> {
          return connection.createSharedDurableConnectionConsumer(
              (Topic) destination, "subname", selector, serverSessionPool, maxMessages);
        },
        true);
  }

  private void simpleTest(ConnectionConsumerBuilder builder, boolean topic) throws Exception {
    simpleTest(
        properties -> {
          properties.put("jms.sessionListenersThreads", 0);
          properties.put("jms.connectionConsumerParallelism", 1);
        },
        builder,
        topic);
    simpleTest(
        properties -> {
          properties.put("jms.sessionListenersThreads", 0);
          properties.put("jms.connectionConsumerParallelism", 4);
        },
        builder,
        topic);

    simpleTest(
        properties -> {
          properties.put("jms.sessionListenersThreads", 4);
          properties.put("jms.connectionConsumerParallelism", 1);
        },
        builder,
        topic);

    simpleTest(
        properties -> {
          properties.put("jms.sessionListenersThreads", 4);
          properties.put("jms.connectionConsumerParallelism", 4);
        },
        builder,
        topic);
  }

  private void simpleTest(
      java.util.function.Consumer<Map<String, Object>> configuration,
      ConnectionConsumerBuilder builder,
      boolean topic)
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideEmulation", true);
    properties.put("jms.sessionListenersThreads", 0);
    properties.put("jms.connectionConsumerParallelism", 1);
    // required for createDurableConnectionConsumer
    properties.put("jms.clientId", "test");
    configuration.accept(properties);

    int parallelism = Integer.parseInt(properties.get("jms.connectionConsumerParallelism") + "");

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
        Connection connection = factory.createConnection();
        Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE); ) {
      connection.start();
      Destination destination =
          topic
              ? session.createTopic("persistent://public/default/test-" + UUID.randomUUID())
              : session.createQueue("persistent://public/default/test-" + UUID.randomUUID());
      SimpleMessageListener listener = new SimpleMessageListener();

      int numSessions = 5;
      int maxMessages = 10;
      String selector = null;
      DummyServerSessionPool serverSessionPool =
          new DummyServerSessionPool(
              numSessions, maxMessages, connection, destination, selector, listener, builder);

      serverSessionPool.start();

      try (MessageProducer producer = session.createProducer(destination); ) {
        for (int i = 0; i < 10; i++) {
          producer.send(session.createTextMessage("foo-" + i));
        }
      }
      // wait for messages to arrive
      await().until(listener.receivedMessages::size, equalTo(10));

      if (parallelism <= 1) {
        // strict ordering
        for (int i = 0; i < 10; i++) {
          assertEquals("foo-" + i, listener.receivedMessages.get(i).getBody(String.class));
        }
      } else {
        for (int i = 0; i < 10; i++) {
          String txt = "foo-" + i;
          assertTrue(
              listener
                  .receivedMessages
                  .stream()
                  .anyMatch(
                      p -> {
                        try {
                          return p.getBody(String.class).equals(txt);
                        } catch (JMSException e) {
                          return false;
                        }
                      }));
        }
      }

      serverSessionPool.close();
    }
  }

  private static class DummyServerSessionPool implements ServerSessionPool {
    private int numSessions;
    private int maxMessages;
    private Connection connection;

    private Destination destination;
    private String selector;

    private MessageListener code;

    private BlockingQueue<ServerSessionImpl> sessions;
    private List<ServerSessionImpl> allSessions;
    private ConnectionConsumer connectionConsumer;
    private ConnectionConsumerBuilder builder;

    private ExecutorService workManager = Executors.newCachedThreadPool();

    public DummyServerSessionPool(
        int numSessions,
        int maxMessages,
        Connection connection,
        Destination destination,
        String selector,
        MessageListener code,
        ConnectionConsumerBuilder builder) {
      this.numSessions = numSessions;
      this.maxMessages = maxMessages;
      this.connection = connection;
      this.destination = destination;
      this.selector = selector;
      this.code = code;
      this.sessions = new ArrayBlockingQueue<>(numSessions);
      this.allSessions = new CopyOnWriteArrayList<>();
      this.builder = builder;
    }

    public void start() throws Exception {
      setupSessions();
      setupConsumer();
    }

    public void setupSessions() throws Exception {
      for (int i = 0; i < numSessions; i++) {
        Session session = connection.createSession();
        session.setMessageListener(code);
        ServerSessionImpl serverSession = new ServerSessionImpl(session, code);
        sessions.add(serverSession);
        allSessions.add(serverSession);
      }
    }

    public void setupConsumer() throws Exception {

      this.connectionConsumer = builder.build(connection, destination, selector, this, maxMessages);
    }

    public void close() throws Exception {
      if (connectionConsumer != null) {
        connectionConsumer.close();
      }
      for (ServerSessionImpl session : allSessions) {
        session.close();
      }
      workManager.shutdown();
    }

    @Override
    public ServerSession getServerSession() throws JMSException {
      try {
        ServerSession session = sessions.take();
        log.info("picked session {}", session);
        return session;
      } catch (Exception err) {
        throw Utils.handleException(err);
      }
    }

    private class ServerSessionImpl implements ServerSession {
      private Session session;
      private MessageListener code;

      public ServerSessionImpl(Session session, MessageListener code) {
        this.session = session;
        this.code = code;
      }

      @Override
      public Session getSession() throws JMSException {
        return session;
      }

      @Override
      public void start() throws JMSException {
        // Simulate the container WorkManager
        workManager.submit(
            () -> {
              try {
                log.info("executing session {}", this);
                session.run();
              } finally {
                log.info("returning session {}", this);
                sessions.add(this);
              }
            });
      }

      public void close() throws Exception {
        session.close();
      }
    }
  }
}
