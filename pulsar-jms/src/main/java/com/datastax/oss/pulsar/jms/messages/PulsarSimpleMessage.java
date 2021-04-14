package com.datastax.oss.pulsar.jms.messages;

import com.datastax.oss.pulsar.jms.PulsarMessage;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import javax.jms.JMSException;

public final class PulsarSimpleMessage extends PulsarMessage {
    @Override
    public void clearBody() throws JMSException {
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return null;
    }

    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        return false;
    }

    @Override
    protected void prepareForSend(TypedMessageBuilder<byte[]> producer) throws JMSException {
        // null value
        producer.value(null);
    }

    @Override
    protected String messageType() {
        return "header";
    }

    public String toString() {
        return "SimpleMessage{" + properties + "}";
    }
}
