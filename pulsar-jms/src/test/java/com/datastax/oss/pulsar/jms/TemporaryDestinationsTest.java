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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
public class TemporaryDestinationsTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer =
      new PulsarContainerExtension()
          .withEnv("PULSAR_PREFIX_allowAutoTopicCreation", "false")
          .withEnv("PULSAR_PREFIX_transactionCoordinatorEnabled", "false");

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
    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    properties.put("jms.forceDeleteTemporaryDestinations", "true");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(); ) {
          String name = "persistent://public/default/test-" + UUID.randomUUID();
          Queue serverAddress = session.createQueue(name);

          pulsarContainer.getAdmin().topics().createNonPartitionedTopic(name);

          try (MessageProducer producerClient = session.createProducer(serverAddress); ) {

            Destination clientAddress = temporaryDestinationMaker.apply(session);
            temporaryDestinationName =
                factory.applySystemNamespace(((PulsarDestination) clientAddress).topicName);

            // verify topic exists
            assertTrue(
                pulsarContainer
                    .getAdmin()
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

    List<String> topics = pulsarContainer.getAdmin().topics().getList("public/default");
    log.debug("Topics {}", topics);

    // verify topic does not exist anymore, as it is deleted on Connection close()
    assertFalse(topics.contains(temporaryDestinationName));
  }
}
