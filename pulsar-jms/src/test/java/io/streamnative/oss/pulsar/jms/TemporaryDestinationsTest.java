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
package io.streamnative.oss.pulsar.jms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamnative.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class TemporaryDestinationsTest {

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  @BeforeAll
  public static void before() throws Exception {
    cluster =
        new PulsarCluster(
            tempDir,
            (config) -> {
              config.setTransactionCoordinatorEnabled(false);
              config.setAllowAutoTopicCreation(false);
            });
    cluster.start();
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  public void useTemporaryQueueTest() throws Exception {
    useTemporaryDestinationTest(session -> Utils.noException(() -> session.createTemporaryQueue()));
  }

  @Test
  public void useTemporaryTopicTest() throws Exception {
    useTemporaryDestinationTest(session -> Utils.noException(() -> session.createTemporaryTopic()));
  }

  private void useTemporaryDestinationTest(Function<Session, Destination> temporaryDestinationMaker)
      throws Exception {

    String temporaryDestinationName;
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("jms.forceDeleteTemporaryDestinations", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          String name = "persistent://public/default/test-" + UUID.randomUUID();
          Queue serverAddress = session.createQueue(name);

          cluster.getService().getAdminClient().topics().createNonPartitionedTopic(name);

          try (MessageProducer producerClient = session.createProducer(serverAddress); ) {

            Destination clientAddress = temporaryDestinationMaker.apply(session);
            temporaryDestinationName =
                factory.applySystemNamespace(((PulsarDestination) clientAddress).topicName);

            // verify topic exists
            assertTrue(
                cluster
                    .getService()
                    .getAdminClient()
                    .topics()
                    .getList("public/default")
                    .contains(temporaryDestinationName));

            // subscribe on the temporary queue
            try (MessageConsumer consumerClient = session.createConsumer(clientAddress); ) {

              // send a request
              Message request = session.createTextMessage("request");
              request.setJMSReplyTo(clientAddress);
              producerClient.send(request);

              // on the server, receive the request
              try (MessageConsumer serverSideConsumer = session.createConsumer(serverAddress)) {
                Message message = serverSideConsumer.receive();
                assertEquals("request", message.getBody(String.class));

                Destination jmsReplyTo = message.getJMSReplyTo();
                assertEquals(jmsReplyTo, clientAddress);

                Message response = session.createTextMessage("response");
                try (MessageProducer serverSideTemporaryProducer =
                    session.createProducer(clientAddress); ) {
                  serverSideTemporaryProducer.send(response);
                }
              }

              // on the client receive the response
              Message theResponse = consumerClient.receive();
              assertEquals("response", theResponse.getBody(String.class));
            }
          }
        }
      }
    }

    List<String> topics = cluster.getService().getAdminClient().topics().getList("public/default");
    log.info("Topics {}", topics);

    // verify topic does not exist anymore, as it is deleted on Connection close()
    assertFalse(topics.contains(temporaryDestinationName));
  }
}
