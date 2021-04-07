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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class UnsubscribeTest {

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
  public void unsubscribeTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        try (Session session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());
          TextMessage textMsg = session.createTextMessage("foo");
          try (MessageProducer producer = session.createProducer(destination); ) {
            try (MessageConsumer consumer =
                session.createSharedDurableConsumer(destination, "sub1")) {
              producer.send(textMsg);
              producer.send(textMsg);
              producer.send(textMsg);
              producer.send(textMsg);
              assertNotNull(consumer.receive());
              assertNotNull(consumer.receive());
              // leave two messages to be consumed
            }
            try (MessageConsumer consumer =
                session.createSharedDurableConsumer(destination, "sub1")) {
              assertNotNull(consumer.receive());
            }
            //            // deleting the subscription
            session.unsubscribe("sub1");

            // recreate the subscription, it will receive only new messages
            // no more messages
            try (MessageConsumer consumer =
                session.createSharedDurableConsumer(destination, "sub1")) {
              assertNull(consumer.receive(1000));

              producer.send(textMsg);
              assertNotNull(consumer.receive());
            }
          }
        }
      }
    }
  }
}
