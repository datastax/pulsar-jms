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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import javax.jms.MessageEOFException;
import org.junit.jupiter.api.Test;

public class PulsarBufferedMessageTest {

  @Test
  public void testBufferedMessage() throws Exception {
    PulsarMessage.PulsarBufferedMessage streamMessage = new PulsarMessage.PulsarBytesMessage();
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

    streamMessage.writeUTF("bar");
    streamMessage.writeByte((byte) 32);
    streamMessage.writeShort((short) 33333);

    streamMessage.writeObject("test");

    // switch to read only mode
    streamMessage.reset();

    assertEquals(55, streamMessage.getBodyLength());
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

    buffer = new byte[1];
    assertEquals(1, streamMessage.readBytes(buffer));
    assertArrayEquals("o".getBytes(StandardCharsets.UTF_8), buffer);

    assertEquals("bar", streamMessage.readUTF());
    assertEquals(32, streamMessage.readUnsignedByte());
    assertEquals(33333, streamMessage.readUnsignedShort());

    assertEquals("test", streamMessage.readObject());

    // additional EOF cases
    assertEquals(-1, streamMessage.readBytes(null));
    assertEquals(-1, streamMessage.readBytes(buffer));

    try {
      streamMessage.readBoolean();
      fail();
    } catch (MessageEOFException expected) {
    }

    // switch to write mode again and reset
    streamMessage.clearBody();
    streamMessage.writeUTF("test");

    // switch to readmode
    streamMessage.reset();
    assertEquals("test", streamMessage.readUTF());

    try {
      streamMessage.readString();
      fail();
    } catch (MessageEOFException expected) {
    }
  }
}
