package com.datastax.oss.pulsar.jms.messages;

import com.datastax.oss.pulsar.jms.PulsarMessage;
import com.datastax.oss.pulsar.jms.Utils;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.jms.TextMessage;
import java.nio.charset.StandardCharsets;

public final class PulsarTextMessage extends PulsarMessage implements TextMessage {
    private String text;

    public PulsarTextMessage(byte[] payload) {
        if (payload == null) {
            this.text = null;
        } else {
            this.text = new String(payload, StandardCharsets.UTF_8);
        }
    }

    public PulsarTextMessage(String text) {
        this.text = text;
    }

    @Override
    protected String messageType() {
        return "text";
    }

    @Override
    public boolean isBodyAssignableTo(Class c) {
        return c == String.class;
    }

    @Override
    public void clearBody() throws JMSException {
        this.text = null;
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return Utils.invoke(() -> c.cast(text));
    }

    @Override
    protected void prepareForSend(TypedMessageBuilder<byte[]> producer) throws JMSException {
        if (text == null) {
            producer.value(null);
        } else {
            producer.value(text.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Sets the string containing this message's data.
     *
     * @param string the {@code String} containing the message's data
     * @throws JMSException                 if the JMS provider fails to set the text due to some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    @Override
    public void setText(String string) throws JMSException {
        this.text = string;
    }

    /**
     * Gets the string containing this message's data. The default value is null.
     *
     * @return the {@code String} containing the message's data
     * @throws JMSException if the JMS provider fails to get the text due to some internal error.
     */
    @Override
    public String getText() throws JMSException {
        return text;
    }

    @Override
    public String toString() {
        return "PulsarTextMessage{" + text + "," + properties + "}";
    }
}
