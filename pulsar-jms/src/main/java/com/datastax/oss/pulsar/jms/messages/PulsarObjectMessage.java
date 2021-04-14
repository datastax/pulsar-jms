package com.datastax.oss.pulsar.jms.messages;

import com.datastax.oss.pulsar.jms.PulsarMessage;
import com.datastax.oss.pulsar.jms.Utils;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.ObjectMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public final class PulsarObjectMessage extends PulsarMessage implements ObjectMessage {

    private Serializable object;

    public PulsarObjectMessage(Serializable object) throws JMSException {
        this.object = object;
    }

    public PulsarObjectMessage(byte[] originalMessage) throws JMSException {
        if (originalMessage == null) {
            this.object = null;
        } else {
            try {
                ObjectInputStream input =
                        new ObjectInputStream(new ByteArrayInputStream(originalMessage));
                this.object = (Serializable) input.readUnshared();
            } catch (Exception err) {
                throw Utils.handleException(err);
            }
        }
    }

    public PulsarObjectMessage() {
    }

    @Override
    protected String messageType() {
        return "object";
    }

    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        return c.isAssignableFrom(Serializable.class) || (object != null && c.isInstance(object));
    }

    @Override
    public void clearBody() throws JMSException {
        this.object = null;
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        if (object == null) {
            return null;
        }
        return Utils.invoke(() -> c.cast(object));
    }

    @Override
    protected void prepareForSend(TypedMessageBuilder<byte[]> producer) throws JMSException {
        if (object == null) {
            producer.value(null);
            return;
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oo = new ObjectOutputStream(out);
            oo.writeUnshared(object);
            oo.flush();
            oo.close();
            producer.value(out.toByteArray());
        } catch (Exception err) {
            throw Utils.handleException(err);
        }
    }

    /**
     * Sets the serializable object containing this message's data. It is important to note that an
     * {@code ObjectMessage} contains a snapshot of the object at the time {@code setObject()} is
     * called; subsequent modifications of the object will have no effect on the {@code
     * ObjectMessage} body.
     *
     * @param object the message's data
     * @throws JMSException                 if the JMS provider fails to set the object due to some internal error.
     * @throws MessageFormatException       if object serialization fails.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    @Override
    public void setObject(Serializable object) {
        this.object = object;
    }

    /**
     * Gets the serializable object containing this message's data. The default value is null.
     *
     * @return the serializable object containing this message's data
     * @throws JMSException           if the JMS provider fails to get the object due to some internal error.
     * @throws MessageFormatException if object deserialization fails.
     */
    @Override
    public Serializable getObject() {
        return object;
    }

    @Override
    public String toString() {
        if (object == null) {
            return "PulsarObjectMessage{null," + properties + "}";
        } else {
            return "PulsarObjectMessage{" + object + "," + object.getClass() + "," + properties + "}";
        }
    }
}
