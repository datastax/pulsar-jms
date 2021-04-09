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
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class MessageListenerTest {

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
  public void receiveWithListener() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
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

  @Test
  public void listenerForbiddenCloseSessionMethod() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
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
        }
      }
    }
  }

  @Test
  public void multipleListenersSameSession() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
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

  @Test
  public void testJMSContextWithListener() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
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
}
