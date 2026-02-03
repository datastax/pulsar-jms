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
package com.datastax.oss.pulsar.jms.tests;

import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import com.datastax.oss.pulsar.jms.PulsarJMSConsumer;
import com.datastax.oss.pulsar.jms.PulsarMessageConsumer;
// it is important that Jackson Annotations are not shaded
import com.fasterxml.jackson.annotation.JsonInclude;
// but Jackson Databind is shaded
import com.datastax.oss.pulsar.jms.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.impl.auth.AuthenticationToken;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Session;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

@Slf4j
public class DockerTest {

  private static final String TEST_PULSAR_DOCKER_IMAGE_NAME =
      System.getProperty("testPulsarDockerImageName");
  public static final String PULSAR_407 = "datastax/lunastreaming:4.0.7_2";

  @TempDir Path tempDir;


  @Test
  public void testPulsar407() throws Exception {
    test(PULSAR_407, false);
  }

  @Test
  public void testPulsar407NoAuthentication() throws Exception {
    test(PULSAR_407, false, false, false);
  }

  @Test
  public void testPulsar407Transactions() throws Exception {
    test(PULSAR_407, true);
  }

  @Test
  public void testPulsar407ServerSideSelectors() throws Exception {
    test(PULSAR_407, false, true);
  }

  @Test
  public void testGenericPulsar() throws Exception {
    assumeTrue(TEST_PULSAR_DOCKER_IMAGE_NAME != null && !TEST_PULSAR_DOCKER_IMAGE_NAME.isEmpty());
    test(TEST_PULSAR_DOCKER_IMAGE_NAME, false);
  }

  @Test
  public void testGenericPulsarTransactions() throws Exception {
    assumeTrue(TEST_PULSAR_DOCKER_IMAGE_NAME != null && !TEST_PULSAR_DOCKER_IMAGE_NAME.isEmpty());
    test(TEST_PULSAR_DOCKER_IMAGE_NAME, true);
  }

  private void test(String image, boolean transactions) throws Exception {
    test(image, transactions, false);
  }

  private void test(String image, boolean transactions, boolean useServerSideFiltering)
      throws Exception {
    test(image, transactions, useServerSideFiltering, true);
  }

  private void test(
      String image,
      boolean transactions,
      boolean useServerSideFiltering,
      boolean enableAuthentication)
      throws Exception {
    log.debug("Classpath: {}", System.getProperty("java.class.path"));
    try (PulsarContainer pulsarContainer =
        new PulsarContainer(
            image, transactions, useServerSideFiltering, enableAuthentication, tempDir); ) {
      pulsarContainer.start();
      Map<String, Object> properties = new HashMap<>();
      properties.put("brokerServiceUrl", pulsarContainer.getPulsarBrokerUrl());
      properties.put("webServiceUrl", pulsarContainer.getHttpServiceUrl());
      properties.put("enableTransaction", transactions);
      if (useServerSideFiltering) {
        properties.put("jms.useServerSideFiltering", true);
      }

      // here we are using the repackaged Pulsar client and actually the class name is
      assertTrue(
          AuthenticationToken.class.getName().startsWith("com.datastax.oss.pulsar.jms.shaded"));

      org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper objectMapper =
          new org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper();
      objectMapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);

      ObjectMapper objectMapper2 = new ObjectMapper();
      objectMapper2.setSerializationInclusion(JsonInclude.Include.ALWAYS);
      objectMapper2.writeValueAsString(new HashMap<>());

      if (enableAuthentication) {
        properties.put("authPlugin", "org.apache.pulsar.client.impl.auth.AuthenticationToken");
        String token =
            IOUtils.toString(
                DockerTest.class.getResourceAsStream("/token.jwt"), StandardCharsets.UTF_8);
        properties.put("authParams", "token:" + token.trim());
      }

      try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
          JMSContext context =
              factory.createContext(
                  transactions ? JMSContext.SESSION_TRANSACTED : JMSContext.CLIENT_ACKNOWLEDGE);
          JMSContext context2 =
              factory.createContext(
                  transactions ? JMSContext.SESSION_TRANSACTED : JMSContext.CLIENT_ACKNOWLEDGE);
          JMSContext context3 = factory.createContext(JMSContext.CLIENT_ACKNOWLEDGE)) {
        Destination queue = context.createQueue("test");
        context.createProducer().send(queue, "foo");
        if (transactions) {
          context.commit();
        }
        assertEquals("foo", context2.createConsumer(queue).receiveBody(String.class));
        if (transactions) {
          context2.commit();
        }

        // test selectors
        Destination topic = context3.createQueue("testTopic");
        try (JMSConsumer consumerWithSelector =
            context3.createConsumer(topic, "keepMessage=TRUE")) {
          context3.createProducer().setProperty("keepMessage", false).send(topic, "skipMe");
          context3.createProducer().setProperty("keepMessage", true).send(topic, "keepMe");

          assertEquals("keepMe", consumerWithSelector.receiveBody(String.class));
          PulsarJMSConsumer pulsarJMSConsumer = (PulsarJMSConsumer) consumerWithSelector;
          PulsarMessageConsumer inner = pulsarJMSConsumer.asPulsarMessageConsumer();

          if (useServerSideFiltering) {
            // the message is not sent to the client at all
            assertEquals(1, inner.getReceivedMessages());
            assertEquals(0, inner.getSkippedMessages());
          } else {
            // the client actually received both the messages and then skipped one
            assertEquals(2, inner.getReceivedMessages());
            assertEquals(1, inner.getSkippedMessages());
          }
        }
      }

      if (enableAuthentication) {
        Map<String, Object> propertiesForPasswordInConnect = new HashMap<>(properties);
        propertiesForPasswordInConnect.put("jms.useCredentialsFromCreateConnection", "true");
        String password = (String) propertiesForPasswordInConnect.remove("authParams");
        assertNotNull(password);
        try (PulsarConnectionFactory factoryConnectUsernamePassword =
            new PulsarConnectionFactory(propertiesForPasswordInConnect); ) {

          // verify that it works with createConnection
          try (Connection connection =
                  factoryConnectUsernamePassword.createConnection("myself", password);
              Session session = connection.createSession()) {
            session
                .createProducer(session.createTopic("testAuth"))
                .send(session.createTextMessage("foo"));
          }

          // verify that it works with createContext
          try (JMSContext context =
              factoryConnectUsernamePassword.createContext("myself", password)) {
            context.createProducer().send(context.createTopic("testAuth2"), "foo");

            // verify create subcontext (no need to pass username/password)
            try (JMSContext subContext = context.createContext(JMSContext.CLIENT_ACKNOWLEDGE)) {
              subContext.createProducer().send(subContext.createTopic("testAuth2"), "foo");
            }
          }

          try {
            factoryConnectUsernamePassword.createConnection("someoneelse", password).close();
            fail();
          } catch (IllegalStateException ok) {
          }
          try {
            factoryConnectUsernamePassword.createContext("someoneelse", password).close();
            fail();
          } catch (IllegalStateRuntimeException ok) {
          }

          try {
            factoryConnectUsernamePassword.createConnection("myself", "differentpassword").close();
            fail();
          } catch (IllegalStateException ok) {
          }
          try {
            factoryConnectUsernamePassword.createContext("myself", "differentpassword").close();
            fail();
          } catch (IllegalStateRuntimeException ok) {
          }

          try {
            factoryConnectUsernamePassword.createConnection().close();
            fail();
          } catch (IllegalStateException ok) {
          }
          try {
            factoryConnectUsernamePassword.createContext().close();
            fail();
          } catch (IllegalStateRuntimeException ok) {
          }
        }
      }
    }
  }
}
