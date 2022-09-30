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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import com.google.common.collect.ImmutableMap;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class PriorityTest {

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
  public void basicPriorityTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.useServerSideFiltering", true);

    properties.put("consumerConfig", ImmutableMap.of("emulateJMSPriority", true));

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          int numMessages = 100;
          try (MessageProducer producer = session.createProducer(destination); ) {
            for (int i = 0; i < numMessages; i++) {
              TextMessage textMessage = session.createTextMessage("foo-" + i);
              if (i < numMessages / 2) {
                // the first messages are lower priority
                producer.setPriority(1);
              } else {
                producer.setPriority(9);
              }
              log.info("send {} prio {}", textMessage.getText(), producer.getPriority());
              producer.send(textMessage);
            }
          }

          try (MessageConsumer consumer1 = session.createConsumer(destination); ) {
            List<TextMessage> received = new ArrayList<>();
            for (int i = 0; i < numMessages; i++) {
              TextMessage msg = (TextMessage) consumer1.receive();
              log.info("got msg {} prio {}", msg.getText(), msg.getJMSPriority());
              received.add(msg);
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());

            // verify that higher priority messages arrived before the others
            int lastPriority = Integer.MAX_VALUE;
            for (TextMessage msg : received) {
              int priority = msg.getJMSPriority();
              log.info("received {} priority {}", msg.getText(), priority);
              assertTrue(priority <= lastPriority);
              lastPriority = priority;
            }
          }
        }
      }
    }
  }
}
