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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.pulsar.jms.messages.PulsarTextMessage;
import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class TopicTest {

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
  public void sendMessageReceiveFromTopic() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 = session.createConsumer(destination);
              MessageConsumer consumer2 = session.createConsumer(destination); ) {

            assertNotSame(consumer2, consumer1);

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                producer.send(session.createTextMessage("foo-" + i));
              }
            }

            // all of the two consumers receive all of the messages, in order
            for (int i = 0; i < 10; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();
              log.info("consumer {} received {}", consumer1, msg.getText());
              assertEquals("foo-" + i, msg.getText());
            }

            for (int i = 0; i < 10; i++) {
              TextMessage msg = (TextMessage) consumer2.receive();
              log.info("consumer {} received {}", consumer2, msg.getText());
              assertEquals("foo-" + i, msg.getText());
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
  public void useTopicSubscriberApiWithSharedSubscription() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.clientId", "the-id");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession();
            Session session2 = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (TopicSubscriber consumer1 =
                  session.createDurableSubscriber(destination, "subscription1");
              TopicSubscriber consumer2a =
                  session.createDurableSubscriber(destination, "subscription2")) {

            // it is not possible to create a consumer sharing the same subscription
            try {
              session.createDurableSubscriber(destination, "subscription2");
              fail("should not create two createDurableSubscriber on the same subscription");
            } catch (IllegalStateException err) {
            }

            // it is not possible to create a consumer sharing the same subscription (broker side
            // check)
            try {
              session2.createDurableSubscriber(destination, "subscription2");
              fail("should not create two createDurableSubscriber on the same subscription");
            } catch (JMSException err) {
              assertTrue(err.getCause() instanceof PulsarClientException.ConsumerBusyException);
            }

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                producer.send(session.createTextMessage("foo-" + i));
              }
            }

            // consumer1 receives all messages, in order
            for (int i = 0; i < 10; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();
              log.info("consumer {} received {}", consumer1, msg.getText());
              assertEquals("foo-" + i, msg.getText());
            }

            // let consumer2a receive the first half of the message
            for (int i = 0; i < 5; i++) {
              TextMessage msg = (TextMessage) consumer2a.receive();
              log.info("consumer {} received {}", consumer2a, msg.getText());
              assertEquals("foo-" + i, msg.getText());
            }

            // closing consumer 2a
            consumer2a.close();

            // let consumer2b receive the second half of the message
            try (TopicSubscriber consumer2b =
                session.createDurableSubscriber(destination, "subscription2")) {
              for (int i = 5; i < 10; i++) {
                TextMessage msg = (TextMessage) consumer2b.receive();
                log.info("consumer {} received {}", consumer2b, msg.getText());
                assertEquals("foo-" + i, msg.getText());
              }
              assertNull(consumer2b.receiveNoWait());
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());
          }
        }
      }
    }
  }

  @Test
  public void simpleDurableConsumerTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context1 = factory.createContext();
          JMSContext context2 = factory.createContext()) {
        Topic topic = context1.createTopic("persistent://public/default/test-" + UUID.randomUUID());
        String durableSubscriptionName = "simpleDurableConsumerTest";
        context2.setClientID("testclient");

        JMSConsumer consumer2 = context2.createDurableConsumer(topic, durableSubscriptionName);

        JMSProducer producer = context1.createProducer();

        TextMessage messageSent = context2.createTextMessage("just a test");
        messageSent.setStringProperty("COM_SUN_JMS_TESTNAME", durableSubscriptionName);
        producer.send(topic, messageSent);
        TextMessage messageReceived = (TextMessage) consumer2.receive(5000);

        // Check to see if correct message received
        assertEquals(messageReceived.getText(), messageSent.getText());
      }
    }
  }

  @Test
  public void testSharedDurableConsumer() throws Exception {
    testSharedDurableConsumer(SubscriptionType.Shared);
  }

  @Test
  public void testKeySharedDurableConsumer() throws Exception {
    testSharedDurableConsumer(SubscriptionType.Key_Shared);
  }

  private void testSharedDurableConsumer(SubscriptionType subscriptionType) throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.topicSharedSubscriptionType", subscriptionType.name());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession();
            Session session2 = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 =
                  session.createSharedDurableConsumer(destination, "subscription1");
              MessageConsumer consumer2a =
                  session2.createSharedDurableConsumer(destination, "subscription2");
              // sharing the same subscription (from two different sessions)
              MessageConsumer consumer2b =
                  session.createSharedDurableConsumer(destination, "subscription2");
              MessageConsumer consumer3 =
                  session2.createSharedDurableConsumer(destination, "subscription3"); ) {

            assertEquals(
                subscriptionType, ((PulsarMessageConsumer) consumer1).getSubscriptionType());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                producer.send(session.createTextMessage("foo-" + i));
              }
            }

            // consumer1 receives all messages, in order
            for (int i = 0; i < 10; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();
              log.info("consumer {} received {}", consumer1, msg.getText());
              assertEquals("foo-" + i, msg.getText());
            }

            // consumer3, receive a few messages, then close the consumer
            for (int i = 0; i < 5; i++) {
              TextMessage msg = (TextMessage) consumer3.receive();
              log.info("consumer {} received {}", consumer3, msg.getText());
              assertEquals("foo-" + i, msg.getText());
            }
            consumer3.close();

            // consumer again from subscription3
            try (MessageConsumer consumer3b =
                session.createSharedDurableConsumer(destination, "subscription3"); ) {
              for (int i = 5; i < 10; i++) {
                TextMessage msg = (TextMessage) consumer3b.receive();
                log.info("consumer {} received {}", consumer3b, msg.getText());
                assertEquals("foo-" + i, msg.getText());
              }
            }

            List<Message> received = new ArrayList<>();

            while (received.size() < 10) {
              TextMessage msg = (TextMessage) consumer2a.receive(100);
              if (msg != null) {
                log.info("consumer {} received {}", consumer2a, msg.getText());
                received.add(msg);
              }
              msg = (TextMessage) consumer2b.receive(100);
              if (msg != null) {
                log.info("consumer {} received {}", consumer2b, msg.getText());
                received.add(msg);
              }
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());
            assertNull(consumer2a.receiveNoWait());
            assertNull(consumer2b.receiveNoWait());
          }
        }
      }
    }
  }

  @Test
  public void testSharedNonDurableConsumer() throws Exception {
    testSharedNonDurableConsumer(SubscriptionType.Shared);
  }

  @Test
  public void testKeySharedNonDurableConsumer() throws Exception {
    testSharedNonDurableConsumer(SubscriptionType.Key_Shared);
  }

  private void testSharedNonDurableConsumer(SubscriptionType subscriptionType) throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.topicSharedSubscriptionType", subscriptionType.name());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession();
            Session session2 = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 =
                  session.createSharedConsumer(destination, "subscription1");
              MessageConsumer consumer2a =
                  session.createSharedConsumer(destination, "subscription2");
              // sharing the same subscription
              MessageConsumer consumer2b =
                  session2.createSharedConsumer(destination, "subscription2");
              MessageConsumer consumer3 =
                  session.createSharedConsumer(destination, "subscription3"); ) {
            assertEquals(
                subscriptionType, ((PulsarMessageConsumer) consumer1).getSubscriptionType());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                producer.send(session.createTextMessage("foo-" + i));
              }
            }

            // consumer1 receives all messages, in order
            for (int i = 0; i < 10; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();
              log.info("consumer {} received {}", consumer1, msg.getText());
              assertEquals("foo-" + i, msg.getText());
            }

            // consumer3, receive a few messages, then close the consumer
            for (int i = 0; i < 5; i++) {
              TextMessage msg = (TextMessage) consumer3.receive();
              log.info("consumer {} received {}", consumer1, msg.getText());
              assertEquals("foo-" + i, msg.getText());
            }
            consumer3.close();

            // consumer again from subscription3, we lost the messages
            try (MessageConsumer consumer3b =
                session.createSharedConsumer(destination, "subscription3"); ) {
              assertNull(consumer3b.receive(1000));
            }

            List<Message> received = new ArrayList<>();

            while (received.size() < 10) {
              TextMessage msg = (TextMessage) consumer2a.receive(100);
              if (msg != null) {
                log.info("consumer {} received {}", consumer2a, msg.getText());
                received.add(msg);
              }
              msg = (TextMessage) consumer2b.receive(100);
              if (msg != null) {
                log.info("consumer {} received {}", consumer2b, msg.getText());
                received.add(msg);
              }
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());
            assertNull(consumer2a.receiveNoWait());
            assertNull(consumer2b.receiveNoWait());
          }
        }
      }
    }
  }

  @Test
  public void testUseKeySharedSubscriptionTypeforTopicConsumer() throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.useExclusiveSubscriptionsForSimpleConsumers", "false");
    properties.put("jms.topicSharedSubscriptionType", SubscriptionType.Key_Shared);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession();
            Session session2 = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {
            assertEquals(
                SubscriptionType.Key_Shared,
                ((PulsarMessageConsumer) consumer1).getSubscriptionType());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                producer.send(session.createTextMessage("foo-" + i));
              }
            }

            // consumer1 receives all messages, in order
            for (int i = 0; i < 10; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();
              log.info("consumer {} received {}", consumer1, msg.getText());
              assertEquals("foo-" + i, msg.getText());
            }
          }
        }
      }
    }
  }

  @Test
  public void testKeySharedWithBatching() throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.topicSharedSubscriptionType", SubscriptionType.Key_Shared);
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batcherBuilder", "KEY_BASED");
    producerConfig.put("batchingEnabled", "true");
    producerConfig.put("batchingMaxPublishDelayMicros", "1000000");
    producerConfig.put("batchingMaxMessages", "1000000");

    properties.put("producerConfig", producerConfig);

    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("receiverQueueSize", 1);
    properties.put("consumerConfig", consumerConfig);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 =
                  session.createSharedDurableConsumer(destination, "subscription1");
              MessageConsumer consumer2 =
                  session.createSharedDurableConsumer(destination, "subscription1"); ) {
            assertEquals(
                SubscriptionType.Key_Shared,
                ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertEquals(
                SubscriptionType.Key_Shared,
                ((PulsarMessageConsumer) consumer2).getSubscriptionType());

            CountDownLatch counter = new CountDownLatch(10);
            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 100; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                textMessage.setStringProperty("JMSXGroupID", "key" + (i % 10));
                textMessage.setIntProperty("ordinal", i);
                producer.send(
                    textMessage,
                    new CompletionListener() {
                      @Override
                      public void onCompletion(Message message) {
                        counter.countDown();
                      }

                      @Override
                      public void onException(Message message, Exception exception) {}
                    });
              }
            }
            assertTrue(counter.await(10, TimeUnit.SECONDS));

            Map<String, List<PulsarTextMessage>> receivedByKey = new HashMap<>();
            Map<String, MessageConsumer> consumerByKey = new HashMap<>();

            int totalReceived = 0;
            while (totalReceived < 100) {
              log.info("total {}", totalReceived);
              PulsarTextMessage msg = (PulsarTextMessage) consumer1.receive(100);
              if (msg != null) {
                handleReceivedMessage(msg, consumer1, receivedByKey, consumerByKey);
                totalReceived++;
              }
              msg = (PulsarTextMessage) consumer2.receive(100);
              if (msg != null) {
                handleReceivedMessage(msg, consumer2, receivedByKey, consumerByKey);
                totalReceived++;
              }
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());
            assertNull(consumer2.receiveNoWait());
            assertEquals(100, totalReceived);

            // verify per-key ordering
            receivedByKey.forEach(
                (k, messages) -> {
                  int last = -1;
                  for (PulsarTextMessage msg : messages) {
                    try {
                      int ordinal = msg.getIntProperty("ordinal");
                      assertEquals(msg.getReceivedPulsarMessage().getKey(), k);
                      log.info("key {} ordinal {}", k, ordinal);
                      assertTrue(ordinal > last);
                      last = ordinal;
                    } catch (JMSException e) {
                      throw new RuntimeException(e);
                    }
                  }
                });
          }
        }
      }
    }
  }

  private void handleReceivedMessage(
      PulsarTextMessage msg,
      MessageConsumer consumer,
      Map<String, List<PulsarTextMessage>> receivedByKey,
      Map<String, MessageConsumer> consumerByKey) {
    log.info(
        "consumer {} received {} {} {}",
        consumer,
        msg,
        msg.getReceivedPulsarMessage().getKey(),
        msg.getReceivedPulsarMessage().getMessageId());
    assertTrue(msg.getReceivedPulsarMessage().getMessageId() instanceof BatchMessageIdImpl);
    String key = msg.getReceivedPulsarMessage().getKey();
    receivedByKey.compute(
        key,
        (k, v) -> {
          if (v == null) {
            v = new ArrayList<>();
          }
          v.add(msg);
          return v;
        });
    assertNotNull(msg.getReceivedPulsarMessage().getKey());
    consumerByKey.compute(
        key,
        (k, v) -> {
          if (v == null) {
            return consumer;
          }
          // verify that each key is received always by the same consumer
          assertSame(consumer, v);
          return consumer;
        });
  }
}
