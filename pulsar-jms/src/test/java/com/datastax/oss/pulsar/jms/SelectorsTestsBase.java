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

import com.datastax.oss.pulsar.jms.messages.PulsarTextMessage;
import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.jms.CompletionListener;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.TextMessage;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class SelectorsTestsBase {

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  private final boolean useServerSideFiltering;
  private final boolean enableBatching;

  public SelectorsTestsBase(boolean useServerSideFiltering, boolean enableBatching) {
    this.useServerSideFiltering = useServerSideFiltering;
    this.enableBatching = enableBatching;
  }

  @BeforeAll
  public void before() throws Exception {
    cluster = new PulsarCluster(tempDir, true, false);
    cluster.start();
  }

  @AfterAll
  public void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  private Map<String, Object> buildProperties() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());

    properties.put("jms.useServerSideFiltering", useServerSideFiltering);
    properties.put("jms.enableClientSideEmulation", !useServerSideFiltering);

    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batchingEnabled", enableBatching);
    properties.put("producerConfig", producerConfig);
    return properties;
  }

  @Test
  public void sendMessageReceiveFromQueue() throws Exception {
    Map<String, Object> properties = buildProperties();

    // ensure that we don't ask for enableClientSideEmulation in this case
    properties.put("jms.enableClientSideEmulation", !useServerSideFiltering);
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Queue destination =
              session.createQueue("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumer1 =
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

            if (useServerSideFiltering) {
              assertEquals(1, consumer1.getReceivedMessages());
              assertEquals(0, consumer1.getSkippedMessages());
            } else {
              assertEquals(10, consumer1.getReceivedMessages());
              assertEquals(9, consumer1.getSkippedMessages());
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());
          }
        }
      }
    }
  }

  @Test
  public void sendMessageReceiveFromTopicWithSelector() throws Exception {

    Map<String, Object> properties = buildProperties();

    // we never need enableClientSideEmulation for an Exclusive subscription
    properties.put("jms.enableClientSideEmulation", "false");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          cluster
              .getService()
              .getAdminClient()
              .topics()
              .createNonPartitionedTopic(destination.getTopicName());

          try (PulsarMessageConsumer consumer1 =
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

            if (useServerSideFiltering) {
              assertEquals(1, consumer1.getReceivedMessages());
              assertEquals(0, consumer1.getSkippedMessages());
            } else {
              assertEquals(10, consumer1.getReceivedMessages());
              assertEquals(9, consumer1.getSkippedMessages());
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());
          }
        }
      }
    }
  }

  @Test
  public void sendMessageReceiveFromExclusiveSubscriptionWithSelector() throws Exception {

    Map<String, Object> properties = buildProperties();

    // we never require enableClientSideEmulation for an Exclusive subscription
    // because it is always safe
    properties.put("jms.enableClientSideEmulation", "false");
    properties.put("jms.clientId", "id");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumer1 =
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

            if (useServerSideFiltering) {
              assertEquals(1, consumer1.getReceivedMessages());
              assertEquals(0, consumer1.getSkippedMessages());
            } else {
              assertEquals(10, consumer1.getReceivedMessages());
              assertEquals(9, consumer1.getSkippedMessages());
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());
          }
        }
      }
    }
  }

  @Test
  public void sendMessageReceiveFromSharedSubscriptionWithSelector() throws Exception {

    Map<String, Object> properties = buildProperties();

    // we ask the user to set enableClientSideEmulation for shared subscriptions
    // because it is always safe
    properties.put("jms.enableClientSideEmulation", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumer1 =
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

            if (useServerSideFiltering) {
              assertEquals(1, consumer1.getReceivedMessages());
              assertEquals(0, consumer1.getSkippedMessages());
            } else {
              assertEquals(10, consumer1.getReceivedMessages());
              assertEquals(9, consumer1.getSkippedMessages());
            }

            // no more messages
            assertNull(consumer1.receiveNoWait());
          }
        }
      }
    }
  }

  @Test
  public void sendBatchWithMoreThenOneMessage() throws Exception {
    Map<String, Object> properties = buildProperties();
    if (enableBatching) {
      // ensure that we create batches with more than 1 message
      Map<String, Object> producerConfig = (Map<String, Object>) properties.get("producerConfig");
      producerConfig.put("batchingMaxPublishDelayMicros", "1000000");
      // each batch will contain 5 messages
      producerConfig.put("batchingMaxMessages", "5");
    }

    // since we are counting the number of transmissions, ensure that we don't receive the
    // same message twice
    properties.put("jms.acknowledgeRejectedMessages", true);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumer1 =
              session.createSharedDurableConsumer(destination, "sub1", "keepMessage=TRUE"); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertEquals("keepMessage=TRUE", consumer1.getMessageSelector());
            List<CompletableFuture<Message>> handles = new ArrayList<>();
            List<String> expected = new ArrayList<>();
            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 100; i++) {
                String text = "foo-" + i;
                TextMessage textMessage = session.createTextMessage(text);
                if (i % 5 == 0) {
                  expected.add(text);
                  textMessage.setBooleanProperty("keepMessage", true);
                }
                CompletableFuture<Message> handle = new CompletableFuture<>();
                producer.send(
                    textMessage,
                    new CompletionListener() {
                      @Override
                      public void onCompletion(Message message) {
                        handle.complete(message);
                      }

                      @Override
                      public void onException(Message message, Exception e) {
                        handle.completeExceptionally(e);
                      }
                    });
                handles.add(handle);
              }
            }

            CompletableFuture.allOf(handles.toArray(new CompletableFuture[0])).get();

            for (String text : expected) {
              PulsarTextMessage textMessage = (PulsarTextMessage) consumer1.receive();
              assertEquals(text, textMessage.getText());

              // ensure that it is a batch message
              assertEquals(
                  enableBatching,
                  textMessage.getReceivedPulsarMessage().getMessageId()
                      instanceof BatchMessageIdImpl);
            }

            // no more messages (this also drains some remaining messages to be skipped)
            assertNull(consumer1.receive(1000));

            if (useServerSideFiltering) {
              if (enableBatching) {
                // unfortunately the server could not reject any batch
                assertEquals(100, consumer1.getReceivedMessages());
                assertEquals(100 - expected.size(), consumer1.getSkippedMessages());
              } else {
                // this is the best case, no batching, so the client
                // receives exactly only the messages that match the filter
                assertEquals(expected.size(), consumer1.getReceivedMessages());
                assertEquals(0, consumer1.getSkippedMessages());
              }
            } else {
              assertEquals(100, consumer1.getReceivedMessages());
              assertEquals(100 - expected.size(), consumer1.getSkippedMessages());
            }
          }
        }
      }
    }
  }

  @Test
  public void sendBatchWithAllMessagesFullyMatchingFilter() throws Exception {
    Map<String, Object> properties = buildProperties();
    if (enableBatching) {
      // ensure that we create batches with more than 1 message
      Map<String, Object> producerConfig = (Map<String, Object>) properties.get("producerConfig");
      producerConfig.put("batchingMaxPublishDelayMicros", "1000000");
      // each batch will contain 5 messages
      producerConfig.put("batchingMaxMessages", "5");
    }

    // since we are counting the number of transmissions, ensure that we don't receive the
    // same message twice
    properties.put("jms.acknowledgeRejectedMessages", true);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (PulsarConnection connection = factory.createConnection()) {
        connection.start();
        try (PulsarSession session = connection.createSession(); ) {
          Topic destination =
              session.createTopic("persistent://public/default/test-" + UUID.randomUUID());

          try (PulsarMessageConsumer consumer1 =
              session.createSharedDurableConsumer(destination, "sub1", "keepMessage=TRUE"); ) {
            assertEquals(
                SubscriptionType.Shared, ((PulsarMessageConsumer) consumer1).getSubscriptionType());
            assertEquals("keepMessage=TRUE", consumer1.getMessageSelector());
            List<CompletableFuture<Message>> handles = new ArrayList<>();
            List<String> expected = new ArrayList<>();
            try (MessageProducer producer = session.createProducer(destination); ) {
              for (int i = 0; i < 100; i++) {
                String text = "foo-" + i;
                TextMessage textMessage = session.createTextMessage(text);
                if (i >= 50) { // the last 10 batches can be filtered out completely
                  expected.add(text);
                  textMessage.setBooleanProperty("keepMessage", true);
                }
                CompletableFuture<Message> handle = new CompletableFuture<>();
                producer.send(
                    textMessage,
                    new CompletionListener() {
                      @Override
                      public void onCompletion(Message message) {
                        handle.complete(message);
                      }

                      @Override
                      public void onException(Message message, Exception e) {
                        handle.completeExceptionally(e);
                      }
                    });
                handles.add(handle);
              }
            }

            CompletableFuture.allOf(handles.toArray(new CompletableFuture[0])).get();

            for (String text : expected) {
              PulsarTextMessage textMessage = (PulsarTextMessage) consumer1.receive();
              assertEquals(text, textMessage.getText());

              // ensure that it is a batch message
              assertEquals(
                  enableBatching,
                  textMessage.getReceivedPulsarMessage().getMessageId()
                      instanceof BatchMessageIdImpl);
            }

            // no more messages (this also drains some remaining messages to be skipped)
            assertNull(consumer1.receive(1000));

            if (useServerSideFiltering) {
              // even with batching the client
              // receives exactly only the messages that match the filter
              assertEquals(expected.size(), consumer1.getReceivedMessages());
              assertEquals(0, consumer1.getSkippedMessages());
            } else {
              assertEquals(100, consumer1.getReceivedMessages());
              assertEquals(100 - expected.size(), consumer1.getSkippedMessages());
            }
          }
        }
      }
    }
  }
}
