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

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

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
        try (Session session = connection.createSession(); ) {
          Topic destination = session.createTopic("persistent://public/default/test-"+ UUID.randomUUID());

          try (MessageConsumer consumer1 = session.createConsumer(destination);
               MessageConsumer consumer2 = session.createConsumer(destination);) {
            assertNotSame(consumer2, consumer1);

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                producer.send(session.createTextMessage("foo-"+i));
              }
            }

           // all of the two consumers receive all of the messages, in order
            for (int i = 0; i < 10; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();
              log.info("consumer {} received {}", consumer1, msg.getText());
              assertEquals("foo-"+i, msg.getText());
            }

            for (int i = 0; i < 10; i++) {
              TextMessage msg = (TextMessage) consumer2.receive();
              log.info("consumer {} received {}", consumer2, msg.getText());
              assertEquals("foo-"+i, msg.getText());
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());
            assertNull(consumer2.receiveNoWait());

          }
        }
      }
    }
  }
}
