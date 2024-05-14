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

import io.streamnative.oss.pulsar.jms.utils.PulsarCluster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
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
public class SerializableConnectionFactoryTest {
  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  @BeforeAll
  public static void before() throws Exception {
    cluster =
        new PulsarCluster(tempDir, (config) -> config.setTransactionCoordinatorEnabled(false));
    cluster.start();
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  public void test() throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());

    try (PulsarConnectionFactory factory1 = new PulsarConnectionFactory(properties);
        PulsarConnectionFactory factory2 = new PulsarConnectionFactory(properties); ) {

      ConnectionFactory[] array = new ConnectionFactory[] {factory1, factory2};

      try (Connection connection1 = factory1.createConnection();
          Connection connection2 = factory2.createConnection();
          Session session1 = connection1.createSession(Session.AUTO_ACKNOWLEDGE);
          Session session2 = connection2.createSession(Session.AUTO_ACKNOWLEDGE)) {

        // use the factories, to produce and consume
        Queue queue = session1.createQueue("test");

        Queue queue2 = session1.createQueue("test2");

        session1.createProducer(queue).send(session1.createTextMessage("foo0"));
        session1.createProducer(queue).send(session1.createTextMessage("foo1"));
        session1.createProducer(queue).send(session1.createTextMessage("foo2"));

        try (MessageConsumer consumer = session2.createConsumer(queue)) {
          connection2.start();
          assertEquals("foo0", consumer.receive().getBody(String.class));
        }

        // serialise
        byte[] serialised = serializeObject(array);

        // deserialize
        ConnectionFactory[] array2 = (ConnectionFactory[]) deserializeObject(serialised);

        // verify that the connections work
        int i = 1;

        assertEquals(2, array2.length);
        for (int j = 0; i < array2.length; j++) {
          ConnectionFactory factory = array2[j];
          try (Connection con = factory.createConnection();
              Session session = con.createSession(Session.AUTO_ACKNOWLEDGE);
              MessageConsumer consumer = session.createConsumer(queue); ) {
            con.start();

            // consume from previously created Queue
            assertEquals("foo" + i, consumer.receive().getBody(String.class));
            i++;

            // use the Pulsar Producer
            try (MessageProducer producer = session.createProducer(queue2); ) {
              producer.send(session.createTextMessage("bar"));
            }

            // use Pulsar Consumer
            try (MessageConsumer consumer2 = session.createConsumer(queue2); ) {
              assertEquals("bar", consumer2.receive().getBody(String.class));
            }
          }

          // dispose
          ((PulsarConnectionFactory) factory).close();
        }
      }
    }
  }

  private static Object deserializeObject(byte[] serialised) throws Exception {
    try (ByteArrayInputStream in = new ByteArrayInputStream(serialised);
        ObjectInputStream ii = new ObjectInputStream(in)) {
      return ii.readObject();
    }
  }

  private static byte[] serializeObject(Object object) throws Exception {
    byte[] serialised;
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(out); ) {
      oo.writeObject(object);
      oo.flush();
      serialised = out.toByteArray();
    }
    return serialised;
  }
}
