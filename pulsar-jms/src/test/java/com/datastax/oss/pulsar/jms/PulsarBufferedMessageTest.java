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
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.StreamMessage;

import com.datastax.oss.pulsar.jms.messages.PulsarBufferedMessage;
import com.datastax.oss.pulsar.jms.messages.PulsarStreamMessage;
import org.junit.jupiter.api.Test;

public class PulsarBufferedMessageTest {

  @Test
  public void testPulsarBytesMessage() throws Exception {
    PulsarBufferedMessage streamMessage = new PulsarBytesMessage();
    testMessage(streamMessage);
  }

  @Test
  public void testPulsarStreamMessage() throws Exception {
    PulsarBufferedMessage streamMessage = new PulsarStreamMessage();
    testMessage(streamMessage);
  }

  private void testMessage(PulsarBufferedMessage msg) throws JMSException {
    msg.writeBoolean(true);
    msg.writeChar('a');
    msg.writeInt(123);
    msg.writeLong(1244l);
    msg.writeShort((short) 213);
    msg.writeByte((byte) 1);
    msg.writeBytes("foo".getBytes(StandardCharsets.UTF_8));
    msg.writeDouble(1.2d);
    msg.writeFloat(1.5f);
    msg.writeBytes("foo".getBytes(StandardCharsets.UTF_8), 2, 1);

    msg.writeUTF("bar");
    msg.writeByte((byte) 32);
    msg.writeShort((short) 33333);

    msg.writeObject("test");

    // switch to read only mode
    msg.reset();
    if (msg instanceof BytesMessage) {
      assertEquals(48, msg.getBodyLength());
    } else {
      assertEquals(70, msg.getBodyLength());
    }
    assertEquals(true, msg.readBoolean());
    assertEquals('a', msg.readChar());
    assertEquals(123, msg.readInt());
    assertEquals(1244l, msg.readLong());
    assertEquals((short) 213, msg.readShort());
    assertEquals((byte) 1, msg.readByte());
    byte[] buffer = new byte[3];
    if (msg instanceof BytesMessage) {
      assertEquals(3, ((BytesMessage) msg).readBytes(buffer));
    } else {
      assertEquals(3, ((StreamMessage) msg).readBytes(buffer));
    }
    assertArrayEquals("foo".getBytes(StandardCharsets.UTF_8), buffer);
    assertEquals(1.2d, msg.readDouble(), 0);
    assertEquals(1.5f, msg.readFloat(), 0);

    buffer = new byte[1];
    if (msg instanceof BytesMessage) {
      assertEquals(1, ((BytesMessage) msg).readBytes(buffer));
    } else {
      assertEquals(1, ((StreamMessage) msg).readBytes(buffer));
    }
    assertArrayEquals("o".getBytes(StandardCharsets.UTF_8), buffer);

    assertEquals("bar", msg.readUTF());
    assertEquals(32, msg.readUnsignedByte());
    assertEquals(33333, msg.readUnsignedShort());

    if (msg instanceof BytesMessage) {
      assertEquals("test", msg.readString());
      // additional EOF cases
      assertEquals(-1, ((BytesMessage) msg).readBytes(null));
      assertEquals(-1, ((BytesMessage) msg).readBytes(buffer));
    } else {
      assertEquals("test", ((StreamMessage) msg).readObject());
      // additional EOF cases
      assertEquals(-1, ((StreamMessage) msg).readBytes(null));
      assertEquals(-1, ((StreamMessage) msg).readBytes(buffer));
    }

    try {
      msg.readBoolean();
      fail();
    } catch (MessageEOFException expected) {
    }

    // switch to write mode again and reset
    msg.clearBody();
    msg.writeUTF("test");

    // switch to readmode
    msg.reset();
    assertEquals("test", msg.readUTF());

    try {
      msg.readString();
      fail();
    } catch (MessageEOFException expected) {
    }
  }
}
