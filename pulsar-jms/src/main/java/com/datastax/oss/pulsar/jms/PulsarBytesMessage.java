package com.datastax.oss.pulsar.jms;

import com.datastax.oss.pulsar.jms.messages.PulsarBufferedMessage;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import java.io.IOException;

final class PulsarBytesMessage extends PulsarBufferedMessage implements BytesMessage {
    public PulsarBytesMessage(byte[] payload) throws JMSException {
        super(payload);
    }

    /**
     * Used by JMSProducer
     *
     * @param payload
     * @return
     * @throws JMSException
     */
    com.datastax.oss.pulsar.jms.PulsarBytesMessage fill(byte[] payload) throws JMSException {
        if (payload != null) {
            this.writeBytes(payload);
        }
        return this;
    }

    protected void checkType(byte type, byte expected) throws JMSException {
    }

    public PulsarBytesMessage() {
    }

    @Override
    protected String messageType() {
        return "bytes";
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        if (c != byte[].class) {
            throw new MessageFormatException("only class byte[]");
        }
        reset();
        try {
            if (originalMessage != null) {
                return (T) originalMessage;
            }
            return (T) stream.toByteArray();
        } finally {
            reset();
        }
    }

    @Override
    protected void writeDataType(byte dataType) {
    }

    @Override
    protected void writeArrayLen(int len) {
    }

    @Override
    protected int readArrayLen() throws IOException {
        return 0;
    }

    /**
     * Reads a byte array from the bytes message stream.
     *
     * <p>If the length of array {@code value} is less than the number of bytes remaining to be read
     * from the stream, the array should be filled. A subsequent call reads the next increment, and
     * so on.
     *
     * <p>If the number of bytes remaining in the stream is less than the length of array {@code
     * value}, the bytes should be read into the array. The return value of the total number of
     * bytes read will be less than the length of the array, indicating that there are no more bytes
     * left to be read from the stream. The next read of the stream returns -1.
     *
     * @param value the buffer into which the data is read
     * @return the total number of bytes read into the buffer, or -1 if there is no more data
     * because the end of the stream has been reached
     * @throws JMSException                if the JMS provider fails to read the message due to some internal
     *                                     error.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    @Override
    public int readBytes(byte[] value) throws JMSException {
        checkReadable();
        if (value == null) {
            return -1;
        }
        try {
            return dataInputStream.read(value);
        } catch (Exception err) {
            throw handleException(err);
        }
    }

    @Override
    protected byte readDataType() {
        return 0;
    }
}
