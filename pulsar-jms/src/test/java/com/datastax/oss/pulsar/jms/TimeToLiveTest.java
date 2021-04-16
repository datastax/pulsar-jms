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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
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
public class TimeToLiveTest {

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
  public void sendMessageReceiveFromQueueWithTimeToLive() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideFeatures", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection(); ) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                // 1 second timeToLive to some of the messages
                long timeToLive;
                if (i % 2 == 0) {
                  timeToLive = 1000;
                } else {
                  timeToLive = 0;
                }
                producer.send(
                    textMessage, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, timeToLive);
                if (timeToLive > 0) {
                  assertTrue(textMessage.getJMSExpiration() > 0);
                }
              }
            }

            // wait for messages to expire
            Thread.sleep(2000);

            // only foo-1, foo-3, foo-5... can be received
            for (int i = 0; i < 5; i++) {
              TextMessage textMessage = (TextMessage) consumer1.receive();
              assertEquals("foo-" + (i * 2 + 1), textMessage.getText());
            }
          }
        }
      }
    }
  }

  @Test
  public void sendMessageReceiveFromTopicWithTimeToLive() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.enableClientSideFeatures", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection(); ) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {
            assertEquals(
                SubscriptionType.Exclusive,
                ((PulsarMessageConsumer) consumer1).getSubscriptionType());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                // 1 second timeToLive to some of the messages
                long timeToLive;
                if (i % 2 == 0) {
                  timeToLive = 1000;
                } else {
                  timeToLive = 0;
                }
                producer.send(
                    textMessage, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, timeToLive);
                if (timeToLive > 0) {
                  assertTrue(textMessage.getJMSExpiration() > 0);
                }
              }
            }

            // wait for messages to expire
            Thread.sleep(2000);

            // only foo-1, foo-3, foo-5... can be received
            for (int i = 0; i < 5; i++) {
              TextMessage textMessage = (TextMessage) consumer1.receive();
              assertEquals("foo-" + (i * 2 + 1), textMessage.getText());
            }
          }
        }
      }
    }
  }
}
