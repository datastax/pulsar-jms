package com.datastax.oss.pulsar.jms;

import org.junit.jupiter.api.Test;

import javax.jms.MessageEOFException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class PulsarBufferedMessageTest {

    @Test
    public void testBufferedMessage() throws Exception {
        PulsarMessage.PulsarBufferedMessage streamMessage = new PulsarMessage.PulsarBufferedMessage();
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
