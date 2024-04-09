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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import javax.jms.Connection;
import javax.jms.JMSContext;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Slf4j
public class NoLocalTest {
  @RegisterExtension
  static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

  private static Stream<Arguments> combinations() {
    return Stream.of(
        Arguments.of(true, true),
        Arguments.of(false, true),
        Arguments.of(true, false),
        Arguments.of(false, false));
  }

  @ParameterizedTest(name = "{index} useServerSideFiltering {0} enableBatching {1}")
  @MethodSource("combinations")
  public void sendMessageReceiveFromQueueWithNoLocal(
      boolean useServerSideFiltering, boolean enableBatching) throws Exception {
    useServerSideFiltering = false;
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.enableClientSideEmulation", !useServerSideFiltering);
    properties.put("jms.useServerSideFiltering", useServerSideFiltering);
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batchingEnabled", enableBatching);
    properties.put("producerConfig", producerConfig);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection(); ) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 = session.createConsumer(destination, null, true); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertTrue(((PulsarMessageConsumer) consumer1).getNoLocal());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                textMessage.setStringProperty("aa", "foo-" + i);
                producer.send(textMessage);
              }
            }
            // no message
            assertNull(consumer1.receiveNoWait());

            try (JMSContext connection2 = factory.createContext()) {
              connection2.createProducer().send(destination, "test");
            }

            // we must be able to receive the message from the second connection
            TextMessage textMessage = (TextMessage) consumer1.receive();
            assertEquals("test", textMessage.getText());
          }
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} useServerSideFiltering {0} enableBatching {1}")
  @MethodSource("combinations")
  public void sendMessageReceiveFromTopicWithNoLocal(
      boolean useServerSideFiltering, boolean enableBatching) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.enableClientSideEmulation", !useServerSideFiltering);
    properties.put("jms.useServerSideFiltering", useServerSideFiltering);
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batchingEnabled", enableBatching);
    properties.put("producerConfig", producerConfig);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection(); ) {
        connection.setClientID("clientId1");
        connection.start();
        try (Session session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 = session.createConsumer(destination, null, true); ) {
            assertEquals(
                SubscriptionType.Exclusive,
                ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertTrue(((PulsarMessageConsumer) consumer1).getNoLocal());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                producer.send(textMessage);
              }
            }
            // no message
            assertNull(consumer1.receiveNoWait());

            try (JMSContext connection2 = factory.createContext()) {
              connection2.createProducer().send(destination, "test");
            }

            // we must be able to receive the message from the second connection
            TextMessage textMessage = (TextMessage) consumer1.receive();
            assertEquals("test", textMessage.getText());
          }
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} useServerSideFiltering {0} enableBatching {1}")
  @MethodSource("combinations")
  public void sendMessageReceiveFromExclusiveSubscriptionWithSelector(
      boolean useServerSideFiltering, boolean enableBatching) throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.enableClientSideEmulation", !useServerSideFiltering);
    properties.put("jms.useServerSideFiltering", useServerSideFiltering);
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batchingEnabled", enableBatching);
    properties.put("producerConfig", producerConfig);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.setClientID("clientId1");
        connection.start();
        try (Session session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 =
              session.createDurableConsumer(destination, "sub1", null, true); ) {
            assertEquals(
                SubscriptionType.Exclusive,
                ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertTrue(((PulsarMessageConsumer) consumer1).getNoLocal());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                producer.send(textMessage);
              }
            }
            // no message
            assertNull(consumer1.receiveNoWait());

            try (JMSContext connection2 = factory.createContext()) {
              connection2.createProducer().send(destination, "test");
            }

            // we must be able to receive the message from the second connection
            TextMessage textMessage = (TextMessage) consumer1.receive();
            assertEquals("test", textMessage.getText());
          }
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} useServerSideFiltering {0} enableBatching {1}")
  @MethodSource("combinations")
  public void sendMessageReceiveFromSharedSubscriptionWithNoLocal(
      boolean useServerSideFiltering, boolean enableBatching) throws Exception {
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.enableClientSideEmulation", !useServerSideFiltering);
    properties.put("jms.useServerSideFiltering", useServerSideFiltering);
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batchingEnabled", enableBatching);
    properties.put("producerConfig", producerConfig);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.setClientID("clientId1");
        connection.start();
        try (Session session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumer1 =
              session.createDurableSubscriber(destination, "sub1", null, true); ) {
            assertEquals(
                SubscriptionType.Exclusive,
                ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertTrue(((PulsarMessageConsumer) consumer1).getNoLocal());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                producer.send(textMessage);
              }
            }
            // no message
            assertNull(consumer1.receiveNoWait());

            try (JMSContext connection2 = factory.createContext()) {
              connection2.createProducer().send(destination, "test");
            }

            // we must be able to receive the message from the second connection
            TextMessage textMessage = (TextMessage) consumer1.receive();
            assertEquals("test", textMessage.getText());
          }
        }
      }
    }
  }

  private static Stream<Arguments> acknowledgeRejectedMessagesTestCombinations() {
    return Stream.of(
        Arguments.of(true, true, true),
        Arguments.of(false, true, true),
        Arguments.of(true, false, true),
        Arguments.of(false, false, true),
        Arguments.of(true, true, false),
        Arguments.of(false, true, false),
        Arguments.of(true, false, false),
        Arguments.of(false, false, false));
  }

  @ParameterizedTest(
    name = "{index} useServerSideFiltering {0} enableBatching {1} acknowledgeRejectedMessages {2}"
  )
  @MethodSource("acknowledgeRejectedMessagesTestCombinations")
  public void acknowledgeRejectedMessagesTest(
      boolean useServerSideFiltering, boolean enableBatching, boolean acknowledgeRejectedMessages)
      throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.enableClientSideEmulation", !useServerSideFiltering);
    properties.put("jms.useServerSideFiltering", useServerSideFiltering);
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batchingEnabled", enableBatching);
    properties.put("producerConfig", producerConfig);
    properties.put("jms.acknowledgeRejectedMessages", acknowledgeRejectedMessages);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.setClientID("clientId1");
        connection.start();
        try (Session session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (MessageConsumer consumerNoLocal =
              session.createConsumer(destination, null, true); ) {
            assertEquals(
                SubscriptionType.Shared, // this is a Queue, so the subscription is always shared
                ((PulsarMessageConsumer) consumerNoLocal).getSubscriptionType());
            assertTrue(((PulsarMessageConsumer) consumerNoLocal).getNoLocal());

            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 10; i++) {
                TextMessage textMessage = session.createTextMessage("foo-" + i);
                producer.send(textMessage);
              }
            }
            // no message
            assertNull(consumerNoLocal.receive(3000));
          }
          if (!acknowledgeRejectedMessages) {
            try (MessageConsumer consumerAllowLocal =
                session.createConsumer(destination, null, false); ) {
              for (int i = 0; i < 10; i++) {
                assertNotNull(consumerAllowLocal.receive());
              }
            }
          } else {
            try (MessageConsumer consumerAllowLocal =
                session.createConsumer(destination, null, false); ) {
              assertNull(consumerAllowLocal.receive(1000));
            }
          }
        }
      }
    }
  }
}
