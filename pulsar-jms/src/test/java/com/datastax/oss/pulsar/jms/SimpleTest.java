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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SimpleTest {

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
  public void sendMessageTest() throws Exception {

    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", cluster.getAddress());
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        try (Session session = connection.createSession(); ) {
          Destination destination = session.createTopic("persistent://public/default/test");
          try (MessageProducer producer = session.createProducer(destination); ) {
            TextMessage textMsg = session.createTextMessage("foo");
            producer.send(textMsg);

            StreamMessage streamMessage = session.createStreamMessage();
            streamMessage.writeBoolean(true);
            streamMessage.writeChar('a');
            streamMessage.writeInt(123);
            streamMessage.writeLong(1244l);
            streamMessage.writeShort((short) 213);
            streamMessage.writeByte((byte) 1);
            streamMessage.writeBytes("foo".getBytes(StandardCharsets.UTF_8));
            streamMessage.writeDouble(1.2d);
            streamMessage.writeFloat(1.5f);
            streamMessage.writeBytes("foo".getBytes(StandardCharsets.UTF_8), 2, 1);
            streamMessage.writeObject("test");

            producer.send(streamMessage);

            streamMessage.reset();
            assertEquals(true, streamMessage.readBoolean());
            assertEquals('a', streamMessage.readChar());
            assertEquals(123, streamMessage.readInt());
            assertEquals(1244l, streamMessage.readLong());
            assertEquals((short) 213, streamMessage.readShort());
            assertEquals((byte) 1, streamMessage.readByte());
            byte[] buffer = new byte[3];
            assertEquals(3, streamMessage.readBytes(buffer));
            assertArrayEquals("foo".getBytes(StandardCharsets.UTF_8), buffer);
            assertEquals(1.2d, streamMessage.readDouble(), 0);
            assertEquals(1.5f, streamMessage.readFloat(), 0);
            ;
            buffer = new byte[1];
            assertEquals(1, streamMessage.readBytes(buffer));
            assertArrayEquals("o".getBytes(StandardCharsets.UTF_8), buffer);
            assertEquals("test", streamMessage.readObject());

            // additional spec validations for edge cases
            assertEquals(-1, streamMessage.readBytes(null));
            assertEquals(-1, streamMessage.readBytes(buffer));
          }
        }
      }
    }
  }
}
