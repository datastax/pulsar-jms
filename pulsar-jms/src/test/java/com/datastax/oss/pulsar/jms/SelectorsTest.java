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
import static org.junit.jupiter.api.Assertions.assertNull;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class SelectorsTest {

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
  public void sendMessageReceiveFromQueueWithSelector() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideFeatures", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 =
              session.createConsumer(destination, "lastMessage=TRUE"); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertEquals("lastMessage=TRUE", consumer1.getMessageSelector());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                if (i == 9) {
                  textMessage.setBooleanProperty("lastMessage", true);
                }
                producer.send(textMessage);
              }
            }

            TextMessage textMessage = (TextMessage) consumer1.receive();
            assertEquals("foo-9", textMessage.getText());

            // no more messages
            assertNull(consumer1.receiveNoWait());
          }
        }
      }
    }
  }

  @Test
  public void sendMessageReceiveFromTopicWithSelector() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideFeatures", "false");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 =
              session.createConsumer(destination, "lastMessage=TRUE"); ) {
            assertEquals(
                SubscriptionType.Exclusive,
                ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertEquals("lastMessage=TRUE", consumer1.getMessageSelector());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                if (i == 9) {
                  textMessage.setBooleanProperty("lastMessage", true);
                }
                producer.send(textMessage);
              }
            }

            TextMessage textMessage = (TextMessage) consumer1.receive();
            assertEquals("foo-9", textMessage.getText());

            // no more messages
            assertNull(consumer1.receiveNoWait());
          }
        }
      }
    }
  }

  @Test
  public void sendMessageReceiveFromExclusiveSubscriptionWithSelector() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideFeatures", "false");
    properties.put("jms.clientId", "id");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 =
              session.createDurableConsumer(destination, "sub1", "lastMessage=TRUE", false); ) {
            assertEquals(
                SubscriptionType.Exclusive,
                ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertEquals("lastMessage=TRUE", consumer1.getMessageSelector());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                if (i == 9) {
                  textMessage.setBooleanProperty("lastMessage", true);
                }
                producer.send(textMessage);
              }
            }

            TextMessage textMessage = (TextMessage) consumer1.receive();
            assertEquals("foo-9", textMessage.getText());

            // no more messages
            assertNull(consumer1.receiveNoWait());
          }
        }
      }
    }
  }

  @Test
  public void sendMessageReceiveFromSharedSubscriptionWithSelector() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideFeatures", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 =
              session.createSharedDurableConsumer(destination, "sub1", "lastMessage=TRUE"); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertEquals("lastMessage=TRUE", consumer1.getMessageSelector());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                if (i == 9) {
                  textMessage.setBooleanProperty("lastMessage", true);
                }
                producer.send(textMessage);
              }
            }

            TextMessage textMessage = (TextMessage) consumer1.receive();
            assertEquals("foo-9", textMessage.getText());

            // no more messages
            assertNull(consumer1.receiveNoWait());
          }
        }
      }
    }
  }
}
