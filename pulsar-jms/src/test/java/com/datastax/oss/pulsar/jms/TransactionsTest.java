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
import static org.junit.jupiter.api.Assertions.assertNull;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Disabled("these tests hang if you run them with the other tests")
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
              }

              // message is not "visible" as transaction is not committed
              assertNull(consumer.receive(1000));

              transaction.commit();

              // message is now visible to consumers
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
  public void consumeRollbackTransactionTest() throws Exception {

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
}
