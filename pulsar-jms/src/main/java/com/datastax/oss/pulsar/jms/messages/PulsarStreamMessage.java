package com.datastax.oss.pulsar.jms.messages;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.StreamMessage;
import java.io.EOFException;
import java.io.IOException;

public final class PulsarStreamMessage extends PulsarBufferedMessage implements StreamMessage {

    public PulsarStreamMessage(byte[] payload) throws JMSException {
        super(payload);
    }

    public PulsarStreamMessage() {
    }

    @Override
    protected String messageType() {
        return "stream";
    }

    protected void checkType(byte type, byte expected) throws JMSException {
        if (type != expected) {
            throw new MessageFormatException("Invalid type " + type + ", expected " + expected);
        }
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        throw new MessageFormatException("getBody not available on StreamMessage");
    }

    @Override
    protected void writeDataType(byte dataType) throws IOException {
        dataOutputStream.writeByte(dataType);
    }

    @Override
    protected void writeArrayLen(int len) throws IOException {
        dataOutputStream.writeInt(len);
    }

    @Override
    protected int readArrayLen() throws IOException {
        return dataInputStream.readInt();
    }

    @Override
    protected byte readDataType() throws IOException {
        return dataInputStream.readByte();
    }

    /**
     * Reads a byte array field from the stream message into the specified {@code byte[]} object
     * (the read buffer).
     *
     * <p>To read the field value, {@code readBytes} should be successively called until it returns
     * a value less than the length of the read buffer. The value of the bytes in the buffer
     * following the last byte read is undefined.
     *
     * <p>If {@code readBytes} returns a value equal to the length of the buffer, a subsequent
     * {@code readBytes} call must be made. If there are no more bytes to be read, this call returns
     * -1.
     *
     * <p>If the byte array field value is null, {@code readBytes} returns -1.
     *
     * <p>If the byte array field value is empty, {@code readBytes} returns 0.
     *
     * <p>Once the first {@code readBytes} call on a {@code byte[]} field value has been made, the
     * full value of the field must be read before it is valid to read the next field. An attempt to
     * read the next field before that has been done will throw a {@code MessageFormatException}.
     *
     * <p>To read the byte field value into a new {@code byte[]} object, use the {@code readObject}
     * method.
     *
     * @param value the buffer into which the data is read
     * @return the total number of bytes read into the buffer, or -1 if there is no more data
     * because the end of the byte field has been reached
     * @throws JMSException                if the JMS provider fails to read the message due to some internal
     *                                     error.
     * @throws MessageEOFException         if unexpected end of message stream has been reached.
     * @throws MessageFormatException      if this type conversion is invalid.
     * @throws MessageNotReadableException if the message is in write-only mode.
     * @see #readObject()
     */
    public int readBytes(byte[] value) throws JMSException {
        checkReadable();
        try {
            if (remainingByteArrayLen > 0) {
                if (value == null) {
                    return -1;
                }
                int read = dataInputStream.read(value, 0, value.length);
                remainingByteArrayLen = remainingByteArrayLen - read;
                return read;
            } else {
                checkType(readDataType(), TYPE_BYTES);
                remainingByteArrayLen = readArrayLen();
                if (value == null) {
                    return -1;
                }
                int read = dataInputStream.read(value, 0, value.length);
                remainingByteArrayLen = remainingByteArrayLen - read;
                return read;
            }
        } catch (EOFException err) {
            return -1;
        } catch (Exception err) {
            throw handleException(err);
        }
    }

    /**
     * Reads an object from the stream message.
     *
     * <p>This method can be used to return, in objectified format, an object in the Java
     * programming language ("Java object") that has been written to the stream with the equivalent
     * {@code writeObject} method call, or its equivalent primitive <code>write<I>type</I></code>
     * method.
     *
     * <p>Note that byte values are returned as {@code byte[]}, not {@code Byte[]}.
     *
     * <p>An attempt to call {@code readObject} to read a byte field value into a new {@code byte[]}
     * object before the full value of the byte field has been read will throw a {@code
     * MessageFormatException}.
     *
     * @return a Java object from the stream message, in objectified format (for example, if the
     * object was written as an {@code int}, an {@code Integer} is returned)
     * @throws JMSException                if the JMS provider fails to read the message due to some internal
     *                                     error.
     * @throws MessageEOFException         if unexpected end of message stream has been reached.
     * @throws MessageFormatException      if this type conversion is invalid.
     * @throws MessageNotReadableException if the message is in write-only mode.
     * @see #readBytes(byte[] value)
     */
    @Override
    public Object readObject() throws JMSException {
        return super.readObject();
    }
}
