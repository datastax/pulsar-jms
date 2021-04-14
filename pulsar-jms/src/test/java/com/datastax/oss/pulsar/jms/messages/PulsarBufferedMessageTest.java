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
package com.datastax.oss.pulsar.jms.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.StreamMessage;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class PulsarBufferedMessageTest {

  @Test
  public void testPulsarBytesMessage() throws Exception {
    testMessage(new PulsarBytesMessage());
  }

  @Test
  public void testPulsarStreamMessage() throws Exception {
    testMessage(new PulsarStreamMessage());
  }

  private void testMessage(PulsarStreamMessage msg) throws JMSException {
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

    assertEquals(70, msg.getBodyLength());
    assertEquals(true, msg.readBoolean());
    assertEquals('a', msg.readChar());
    assertEquals(123, msg.readInt());
    assertEquals(1244l, msg.readLong());
    assertEquals((short) 213, msg.readShort());
    assertEquals((byte) 1, msg.readByte());
    byte[] buffer = new byte[3];

    assertEquals(3, ((StreamMessage) msg).readBytes(buffer));

    assertArrayEquals("foo".getBytes(StandardCharsets.UTF_8), buffer);
    assertEquals(1.2d, msg.readDouble(), 0);
    assertEquals(1.5f, msg.readFloat(), 0);

    buffer = new byte[1];

    assertEquals(1, ((StreamMessage) msg).readBytes(buffer));

    assertArrayEquals("o".getBytes(StandardCharsets.UTF_8), buffer);

    assertEquals("bar", msg.readUTF());
    assertEquals(32, msg.readUnsignedByte());
    assertEquals(33333, msg.readUnsignedShort());

    assertEquals("test", ((StreamMessage) msg).readObject());
    // additional EOF cases
    assertEquals(-1, ((StreamMessage) msg).readBytes(null));
    assertEquals(-1, ((StreamMessage) msg).readBytes(buffer));

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

  private void testMessage(PulsarBytesMessage msg) throws JMSException {
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

    assertEquals(48, msg.getBodyLength());

    assertEquals(true, msg.readBoolean());
    assertEquals('a', msg.readChar());
    assertEquals(123, msg.readInt());
    assertEquals(1244l, msg.readLong());
    assertEquals((short) 213, msg.readShort());
    assertEquals((byte) 1, msg.readByte());
    byte[] buffer = new byte[3];
    assertEquals(3, ((BytesMessage) msg).readBytes(buffer));

    assertArrayEquals("foo".getBytes(StandardCharsets.UTF_8), buffer);
    assertEquals(1.2d, msg.readDouble(), 0);
    assertEquals(1.5f, msg.readFloat(), 0);

    buffer = new byte[1];
    assertEquals(1, ((BytesMessage) msg).readBytes(buffer));

    assertArrayEquals("o".getBytes(StandardCharsets.UTF_8), buffer);

    assertEquals("bar", msg.readUTF());
    assertEquals(32, msg.readUnsignedByte());
    assertEquals(33333, msg.readUnsignedShort());

    assertEquals("test", msg.readString());
    // additional EOF cases
    assertEquals(-1, ((BytesMessage) msg).readBytes(null));
    assertEquals(-1, ((BytesMessage) msg).readBytes(buffer));

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

  @Test
  public void streamMessageTestFromTCK() throws Exception {
    StreamMessage messageSent = new PulsarStreamMessage();
    byte bValue = 127;
    boolean abool = false;
    byte[] bValues = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    byte[] bValues2 = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
    byte[] bValuesReturned = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    byte[] bValuesReturned2 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    char charValue = 'Z';
    short sValue = 32767;
    long lValue = 9223372036854775807L;
    double dValue = 6.02e23;
    float fValue = 6.02e23f;
    int iValue = 6;
    boolean pass = true;
    String myString = "text";
    String sTesting = "Testing StreamMessages";

    messageSent.writeBytes(bValues2, 0, bValues.length);
    messageSent.writeBoolean(abool);
    messageSent.writeByte(bValue);
    messageSent.writeBytes(bValues);
    messageSent.writeChar(charValue);
    messageSent.writeDouble(dValue);
    messageSent.writeFloat(fValue);
    messageSent.writeInt(iValue);
    messageSent.writeLong(lValue);
    messageSent.writeObject(sTesting);
    messageSent.writeShort(sValue);
    messageSent.writeString(myString);
    messageSent.writeObject(null);

    StreamMessage messageReceived = messageSent;
    messageReceived.reset();

    int nCount;
    do {
      nCount = messageReceived.readBytes(bValuesReturned2);
      log.info("nCount is " + nCount);
      if (nCount != -1) {
        for (int i = 0; i < bValuesReturned2.length; i++) {
          if (bValuesReturned2[i] != bValues2[i]) {
            log.info("Fail: byte[] " + i + " is not valid");
            pass = false;
          } else {
            log.info("PASS: byte[]" + i + " is valid");
          }
        }
      }
    } while (nCount >= bValuesReturned2.length);

    if (messageReceived.readBoolean() == abool) {
      log.info("Pass: boolean returned ok");
    } else {
      log.info("Fail: boolean not returned as expected");
      pass = false;
    }

    if (messageReceived.readByte() == bValue) {
      log.info("Pass: Byte returned ok");
    } else {
      log.info("Fail: Byte not returned as expected");
      pass = false;
    }

    do {
      nCount = messageReceived.readBytes(bValuesReturned);
      log.info("nCount is " + nCount);
      if (nCount != -1) {
        for (int i = 0; i < bValuesReturned2.length; i++) {
          if (bValuesReturned2[i] != bValues2[i]) {
            log.info("Fail: byte[] " + i + " is not valid");
            pass = false;
          } else {
            log.info("PASS: byte[]" + i + " is valid");
          }
        }
      }
    } while (nCount >= bValuesReturned2.length);

    if (messageReceived.readChar() == charValue) {
      log.info("Pass: correct char");
    } else {
      log.info("Fail: char not returned as expected");
      pass = false;
    }

    if (messageReceived.readDouble() == dValue) {
      log.info("Pass: correct double");
    } else {
      log.info("Fail: double not returned as expected");
      pass = false;
    }

    if (messageReceived.readFloat() == fValue) {
      log.info("Pass: correct float");
    } else {
      log.info("Fail: float not returned as expected");
      pass = false;
    }

    if (messageReceived.readInt() == iValue) {
      log.info("Pass: correct int");
    } else {
      log.info("Fail: int not returned as expected");
      pass = false;
    }

    if (messageReceived.readLong() == lValue) {
      log.info("Pass: correct long");
    } else {
      log.info("Fail: long not returned as expected");
      pass = false;
    }

    if (messageReceived.readObject().equals(sTesting)) {
      log.info("Pass: correct object");
    } else {
      log.info("Fail: object not returned as expected");
      pass = false;
    }

    if (messageReceived.readShort() == sValue) {
      log.info("Pass: correct short");
    } else {
      log.info("Fail: short not returned as expected");
      pass = false;
    }

    if (messageReceived.readString().equals(myString)) {
      log.info("Pass: correct string");
    } else {
      log.info("Fail: string not returned as expected");
      pass = false;
    }

    if (messageReceived.readObject() == null) {
      log.info("Pass: correct object");
    } else {
      log.info("Fail: object not returned as expected");
      pass = false;
    }

    assertTrue(pass);
  }
}
