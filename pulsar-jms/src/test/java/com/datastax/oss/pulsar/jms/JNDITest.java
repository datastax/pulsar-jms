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

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.oss.pulsar.jms.jndi.PulsarInitialContextFactory;
import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import javax.naming.CompositeName;
import javax.naming.CompoundName;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.InvalidNameException;
import javax.naming.Name;
import org.apache.pulsar.client.api.Producer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class JNDITest {

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
  public void basicJDNITest() throws Exception {

    Properties properties = new Properties();
    properties.setProperty(
        Context.INITIAL_CONTEXT_FACTORY, PulsarInitialContextFactory.class.getName());
    properties.setProperty(Context.PROVIDER_URL, cluster.getAddress());

    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("producerName", "the-name");
    properties.put("webServiceUrl", cluster.getAddress());
    properties.put("producerConfig", producerConfig);

    String queueName = "test-" + UUID.randomUUID();
    String topicName = "test-" + UUID.randomUUID();
    javax.naming.Context jndiContext = new InitialContext(properties);
    PulsarConnectionFactory factory = null;
    try {
      // this factory is disposed when closing the InitialContext
      factory = (PulsarConnectionFactory) jndiContext.lookup("ConnectionFactory");
      try (PulsarConnection connection =
          factory.createConnection(); ) { // trigger factory initialization
        PulsarDestination queue = (PulsarDestination) jndiContext.lookup("queues/" + queueName);
        PulsarDestination topic = (PulsarDestination) jndiContext.lookup("topics/" + topicName);

        Producer<byte[]> producer = factory.getProducerForDestination(queue, false);
        // test that configuration is fully passed
        assertEquals("the-name", producer.getProducerName());

        assertEquals(queue, new PulsarQueue(queueName));
        assertEquals(topic, new PulsarTopic(topicName));

        PulsarDestination fullyQualifiedTopic =
            (PulsarDestination)
                jndiContext.lookup("topics/persistent://public/default/" + topicName);

        assertEquals(
            fullyQualifiedTopic, new PulsarTopic("persistent://public/default/" + topicName));

        assertThrows(InvalidNameException.class, () -> jndiContext.lookup(""));
        assertThrows(InvalidNameException.class, () -> jndiContext.lookup("something/foo"));
        assertThrows(InvalidNameException.class, () -> jndiContext.lookup("queues"));
        assertThrows(InvalidNameException.class, () -> jndiContext.lookup("topics"));
        assertThrows(InvalidNameException.class, () -> jndiContext.lookup("/"));
        assertThrows(InvalidNameException.class, () -> jndiContext.lookup("topics/"));
        assertThrows(InvalidNameException.class, () -> jndiContext.lookup("queues/"));

        PulsarDestination topicFromCompositeName =
            (PulsarDestination) jndiContext.lookup(new CompositeName("topics/" + topicName));
        assertEquals(topicFromCompositeName, new PulsarTopic(topicName));

        Name compoundName = new CompoundName("topics/" + topicName, new Properties());
        PulsarDestination topicFromCompoundName =
            (PulsarDestination) jndiContext.lookup(compoundName);
        assertEquals(topicFromCompoundName, new PulsarTopic(topicName));
      }

    } finally {
      jndiContext.close();
    }
    assertTrue(factory != null && factory.isClosed());
  }
}
