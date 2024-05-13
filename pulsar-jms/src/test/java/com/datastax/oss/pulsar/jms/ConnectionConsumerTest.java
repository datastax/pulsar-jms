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
package io.streamnative.oss.pulsar.jms;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.streamnative.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.internal.util.reflection.Whitebox;

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
              c.setEntryFilterNames(Collections.emptyList());
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
    executeTest(
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
    executeTest(
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
    executeTest(
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
    executeTest(
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
    executeTest(
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

  private void executeTest(ConnectionConsumerBuilder builder, boolean topic) throws Exception {
    executeTest(builder, topic, 1000, 10, null, 15, false);
    executeTest(builder, topic, 1000, 10, "(foo='bar') or (1=1)", 15, false);
    executeTest(builder, topic, 100, 10, null, 1, false);
    executeTest(builder, topic, 100, 5, null, 1, true);
  }

  private void executeTest(
      ConnectionConsumerBuilder builder,
      boolean topic,
      int numMessages,
      int numSessions,
      String selector,
      int maxMessages,
      boolean maxMessagesLimitParallelism)
      throws Exception {
    long start = System.currentTimeMillis();
    log.info(
        "ExecuteTest {} {} {} {} {} {}",
        topic,
        numMessages,
        numSessions,
        selector,
        maxMessages,
        maxMessagesLimitParallelism);
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideEmulation", true);
    properties.put("jms.maxMessagesLimitsParallelism", maxMessagesLimitParallelism);

    // required for createDurableConnectionConsumer
    properties.put("jms.clientId", "test");
    properties.put("producerConfig", ImmutableMap.of("batchingEnabled", false));

    String topicName;
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
        Connection connection = factory.createConnection();
        Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE); ) {
      connection.start();
      Destination destination =
          topic
              ? session.createTopic("persistent://public/default/test-" + UUID.randomUUID())
              : session.createQueue("persistent://public/default/test-" + UUID.randomUUID());
      topicName = factory.getPulsarTopicName(destination);
      SimpleMessageListener listener = new SimpleMessageListener();

      DummyServerSessionPool serverSessionPool =
          new DummyServerSessionPool(
              numSessions, maxMessages, connection, destination, selector, listener, builder);

      serverSessionPool.start();

      try (MessageProducer producer = session.createProducer(destination); ) {
        List<CompletableFuture<Void>> handles = new ArrayList<>(numMessages);
        for (int i = 0; i < numMessages; i++) {
          CompletableFuture<Void> handle = new CompletableFuture<>();
          handles.add(handle);
          producer.send(
              session.createTextMessage("foo-" + i),
              new CompletionListener() {
                @Override
                public void onCompletion(Message message) {
                  handle.complete(null);
                }

                @Override
                public void onException(Message message, Exception e) {
                  handle.completeExceptionally(e);
                }
              });
          if (i % 100 == 0) {
            FutureUtil.waitForAll(handles).get();
            handles.clear();
          }
        }
        FutureUtil.waitForAll(handles).get();
        handles.clear();
      }

      // wait for messages to arrive
      await()
          .atMost(20, TimeUnit.SECONDS)
          .until(listener.receivedMessages::size, equalTo(numMessages));

      if (maxMessagesLimitParallelism && maxMessages == 1) {
        // strict ordering
        for (int i = 0; i < numMessages; i++) {
          String txt = "foo-" + i;
          String msg = listener.receivedMessages.get(i).getBody(String.class);
          assertEquals(msg, txt);
        }
      } else {
        for (int i = 0; i < numMessages; i++) {
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

      Awaitility.await()
          .untilAsserted(
              () -> {
                TopicStats stats =
                    cluster.getService().getAdminClient().topics().getStats(topicName);
                assertEquals(0, stats.getBacklogSize());
              });

      // stop waiting for messages
      connection.stop();

      List<PulsarSession> sessions =
          (List<PulsarSession>) Whitebox.getInternalState(connection, "sessions");
      int numSessionsWithConsumers = 0;
      for (PulsarSession s : sessions) {
        List<PulsarMessageConsumer> consumers =
            (List<PulsarMessageConsumer>) Whitebox.getInternalState(s, "consumers");
        if (!consumers.isEmpty()) {
          assertEquals(1, consumers.size());
          assertEquals(selector, consumers.get(0).getMessageSelector());
          numSessionsWithConsumers++;
        }
      }
      assertEquals(1, numSessionsWithConsumers);

      assertEquals(numSessions + 1 + 1, sessions.size());

      serverSessionPool.close();

      long end = System.currentTimeMillis();
      log.info("ExecuteTest time {} ms", end - start);
    }
  }

  @Test
  public void testStopTimeoutWithMessageDrivenOnMessageCallbackStuck() throws Exception {
    long start = System.currentTimeMillis();

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideEmulation", true);
    properties.put("jms.maxMessagesLimitsParallelism", false);
    // we should not be stuck even if connectionConsumerStopTimeout is 0
    properties.put("jms.connectionConsumerStopTimeout", 0);

    // required for createDurableConnectionConsumer
    properties.put("jms.clientId", "test");
    properties.put("producerConfig", ImmutableMap.of("batchingEnabled", false));

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
        Connection connection = factory.createConnection();
        Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE); ) {
      connection.start();
      Destination destination =
          session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

      CompletableFuture<Void> onMessageEntered = new CompletableFuture<>();
      CompletableFuture<Void> condition = new CompletableFuture<>();
      SimpleMessageListener listener =
          new SimpleMessageListener() {
            @Override
            public void onMessage(Message message) {
              onMessageEntered.complete(null);
              // this method blocks
              log.info("here", new Exception("here").fillInStackTrace());
              condition.join();
              super.onMessage(message);
            }
          };

      DummyServerSessionPool serverSessionPool =
          new DummyServerSessionPool(
              10,
              10,
              connection,
              destination,
              null,
              listener,
              new ConnectionConsumerBuilder() {
                @Override
                public ConnectionConsumer build(
                    Connection connection,
                    Destination destination,
                    String selector,
                    ServerSessionPool serverSessionPool,
                    int maxMessages)
                    throws Exception {
                  return connection.createConnectionConsumer(
                      destination, selector, serverSessionPool, maxMessages);
                }
              });

      serverSessionPool.start();

      try (MessageProducer producer = session.createProducer(destination); ) {
        producer.send(session.createTextMessage("foo-0"));
      }

      onMessageEntered.join();

      serverSessionPool.close();

      // unlock the listener
      condition.complete(null);

      // verify that the listener was called
      await()
          .atMost(Integer.MAX_VALUE, TimeUnit.SECONDS)
          .until(listener.receivedMessages::size, equalTo(1));

      long end = System.currentTimeMillis();
      log.info("ExecuteTest time {} ms", end - start);
    }
  }

  @Test
  public void testStopTimeoutWithSpoolThreadStuck() throws Exception {
    long start = System.currentTimeMillis();

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideEmulation", true);
    properties.put("jms.maxMessagesLimitsParallelism", false);
    properties.put("jms.connectionConsumerStopTimeout", 2000);

    // required for createDurableConnectionConsumer
    properties.put("jms.clientId", "test");
    properties.put("producerConfig", ImmutableMap.of("batchingEnabled", false));

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
        Connection connection = factory.createConnection();
        Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE); ) {
      connection.start();
      Destination destination =
          session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

      CompletableFuture<Void> beforeSubmitTask = new CompletableFuture<>();
      CompletableFuture<Void> submitTaskEntered = new CompletableFuture<>();
      SimpleMessageListener listener = new SimpleMessageListener();

      DummyServerSessionPool serverSessionPool =
          new DummyServerSessionPool(
              10,
              10,
              connection,
              destination,
              null,
              listener,
              new ConnectionConsumerBuilder() {
                @Override
                public ConnectionConsumer build(
                    Connection connection,
                    Destination destination,
                    String selector,
                    ServerSessionPool serverSessionPool,
                    int maxMessages)
                    throws Exception {
                  return connection.createConnectionConsumer(
                      destination, selector, serverSessionPool, maxMessages);
                }
              }) {
            @Override
            public void submitTask(Runnable task) {
              submitTaskEntered.complete(null);
              // simulate stuck ServerPool
              beforeSubmitTask.join();
              super.submitTask(task);
            }
          };

      serverSessionPool.start();

      try (MessageProducer producer = session.createProducer(destination); ) {
        producer.send(session.createTextMessage("foo-0"));
      }

      submitTaskEntered.join();

      serverSessionPool.close();

      // verify that we exited "close()" even if the spool thread was alive
      assertTrue(
          ((PulsarConnectionConsumer) serverSessionPool.connectionConsumer).isSpoolThreadAlive());

      // unlock the serverpool
      beforeSubmitTask.complete(null);

      long end = System.currentTimeMillis();
      log.info("ExecuteTest time {} ms", end - start);
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

    public void submitTask(Runnable task) {
      try {
        workManager.submit(task);
      } catch (RejectedExecutionException expected) {
        log.info("Task {} was rejected, because the Pool is closed", task);
      }
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
        // log.info("picked session {}", session);
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
        submitTask(
            () -> {
              try {
                // log.info("executing session {}", this);
                session.run();
              } finally {
                // log.info("returning session {}", this);
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
