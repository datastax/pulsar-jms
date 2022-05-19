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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ConnectionConsumerTest {

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

  private interface ConnectionConsumerBuilder {
    ConnectionConsumer build(
        Connection connection,
        Destination destination,
        String selector,
        ServerSessionPool serverSessionPool)
        throws Exception;
  }

  @Test
  public void createConnectionConsumerQueue() throws Exception {
    simpleTest(
        (Connection connection,
            Destination destination,
            String selector,
            ServerSessionPool serverSessionPool) -> {
          return connection.createConnectionConsumer(destination, selector, serverSessionPool, 10);
        },
        false);
  }

  @Test
  public void createConnectionConsumerTopic() throws Exception {
    simpleTest(
        (Connection connection,
            Destination destination,
            String selector,
            ServerSessionPool serverSessionPool) -> {
          return connection.createConnectionConsumer(destination, selector, serverSessionPool, 10);
        },
        true);
  }

  @Test
  public void createDurableConnectionConsumer() throws Exception {
    simpleTest(
        (Connection connection,
            Destination destination,
            String selector,
            ServerSessionPool serverSessionPool) -> {
          return connection.createDurableConnectionConsumer(
              (Topic) destination, "subname", selector, serverSessionPool, 10);
        },
        true);
  }

  @Test
  public void createSharedConnectionConsumer() throws Exception {
    simpleTest(
        (Connection connection,
            Destination destination,
            String selector,
            ServerSessionPool serverSessionPool) -> {
          return connection.createSharedConnectionConsumer(
              (Topic) destination, "subname", selector, serverSessionPool, 10);
        },
        true);
  }

  @Test
  public void createSharedDurableConnectionConsumer() throws Exception {
    simpleTest(
        (Connection connection,
            Destination destination,
            String selector,
            ServerSessionPool serverSessionPool) -> {
          return connection.createSharedDurableConnectionConsumer(
              (Topic) destination, "subname", selector, serverSessionPool, 10);
        },
        true);
  }

  private void simpleTest(ConnectionConsumerBuilder builder, boolean topic) throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideEmulation", true);
    // required for createDurableConnectionConsumer
    properties.put("jms.clientId", "test");

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
        Connection connection = factory.createConnection();
        Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE); ) {
      connection.start();
      Destination destination =
          topic
              ? session.createTopic("persistent://public/default/test-" + UUID.randomUUID())
              : session.createQueue("persistent://public/default/test-" + UUID.randomUUID());
      SimpleMessageListener listener = new SimpleMessageListener();
      session.setMessageListener(listener);

      AtomicBoolean startCalled = new AtomicBoolean();
      ServerSessionPool serverSessionPool =
          new ServerSessionPool() {
            @Override
            public ServerSession getServerSession() throws JMSException {
              return new ServerSession() {
                @Override
                public Session getSession() throws JMSException {
                  return session;
                }

                @Override
                public void start() throws JMSException {
                  startCalled.set(true);
                }
              };
            }
          };

      ConnectionConsumer connectionConsumer =
          builder.build(connection, destination, "TRUE", serverSessionPool);

      assertTrue(startCalled.get());

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

      connectionConsumer.close();
    }
  }
}
