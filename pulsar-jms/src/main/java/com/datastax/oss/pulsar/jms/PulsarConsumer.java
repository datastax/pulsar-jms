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

import org.apache.pulsar.client.api.Consumer;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import java.util.concurrent.TimeUnit;

public class PulsarConsumer implements MessageConsumer {
    private final String name;
    private final PulsarSession session;
    private final PulsarDestination destination;
    private Consumer<byte[]> consumer;
    private MessageListenerWrapper messageListenerWrapper;

    public PulsarConsumer(String name, PulsarDestination destination, PulsarSession session) {
        this.name = name;
        this.session = session;
        this.destination = destination;
    }

    /**
     * Gets this message consumer's message selector expression.
     *
     * @return this message consumer's message selector, or null if no message selector exists for the message consumer
     * (that is, if the message selector was not set or was set to null or the empty string)
     * @throws JMSException if the JMS provider fails to get the message selector due to some internal error.
     */
    @Override
    public String getMessageSelector() throws JMSException {
        return null;
    }

    /**
     * Gets the {@code MessageConsumer}'s {@code MessageListener}.
     *
     * <p>
     * This method must not be used in a Java EE web or EJB application. Doing so may cause a {@code JMSException} to be
     * thrown though this is not guaranteed.
     *
     * @return the {@code MessageConsumer}'s {@code MessageListener}, or null if one was not set
     * @throws JMSException if the JMS provider fails to get the {@code MessageListener} for one of the following
     *                      reasons:
     *                      <ul>
     *                      <li>an internal error has occurred or
     *                      <li>this method has been called in a Java EE web or EJB application (though it is not guaranteed that an exception is
     *                      thrown in this case)
     *                      </ul>
     * @see MessageConsumer#setMessageListener(MessageListener)
     */
    @Override
    public MessageListener getMessageListener() throws JMSException {
        return messageListenerWrapper.getListener();
    }

    /**
     * Sets the {@code MessageConsumer}'s {@code MessageListener}.
     *
     * <p>
     * Setting the the {@code MessageListener} to null is the equivalent of unsetting the {@code MessageListener} for the
     * {@code MessageConsumer}.
     *
     * <p>
     * The effect of calling this method while messages are being consumed by an existing listener or the
     * {@code MessageConsumer} is being used to consume messages synchronously is undefined.
     *
     * <p>
     * This method must not be used in a Java EE web or EJB application. Doing so may cause a {@code JMSException} to be
     * thrown though this is not guaranteed.
     *
     * @param listener the listener to which the messages are to be delivered
     * @throws JMSException if the JMS provider fails to set the {@code MessageConsumer}'s {@code MessageListener} for
     *                      one of the following reasons:
     *                      <ul>
     *                      <li>an internal error has occurred or
     *                      <li>this method has been called in a Java EE web or EJB application (though it is not guaranteed that an exception is
     *                      thrown in this case)
     *                      </ul>
     * @see MessageConsumer#getMessageListener()
     */
    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        if (messageListenerWrapper != null) {
            throw new IllegalStateException("You cannot set the listener twice");
        }
        this.messageListenerWrapper = new MessageListenerWrapper(listener, session);
        consumer = session.getFactory().getConsumerForDestination(destination, name, messageListenerWrapper, session.getAcknowledgeMode());
    }

    /**
     * Receives the next message produced for this message consumer.
     *
     * <p>
     * This call blocks indefinitely until a message is produced or until this message consumer is closed.
     *
     * <p>
     * If this {@code receive} is done within a transaction, the consumer retains the message until the transaction commits.
     *
     * @return the next message produced for this message consumer, or null if this message consumer is concurrently closed
     * @throws JMSException if the JMS provider fails to receive the next message due to some internal error.
     */
    @Override
    public Message receive() throws JMSException {
        if (messageListenerWrapper != null) {
            throw new IllegalStateException("cannot receive if you have a messageListener");
        }

        if (consumer == null) {
            consumer = session.getFactory().getConsumerForDestination(destination, name, null, session.getAcknowledgeMode());
        }

        try {
            org.apache.pulsar.client.api.Message<byte[]> message = consumer.receive();
            return PulsarMessage.decode(consumer, message);
        } catch (Exception err) {
            throw Utils.handleException(err);
        }
    }

    /**
     * Receives the next message that arrives within the specified timeout interval.
     *
     * <p>
     * This call blocks until a message arrives, the timeout expires, or this message consumer is closed. A {@code timeout}
     * of zero never expires, and the call blocks indefinitely.
     *
     * @param timeout the timeout value (in milliseconds)
     * @return the next message produced for this message consumer, or null if the timeout expires or this message consumer
     * is concurrently closed
     * @throws JMSException if the JMS provider fails to receive the next message due to some internal error.
     */
    @Override
    public Message receive(long timeout) throws JMSException {
        if (messageListenerWrapper != null) {
            throw new IllegalStateException("cannot receive if you have a messageListener");
        }

        if (consumer == null) {
            consumer = session.getFactory().getConsumerForDestination(destination, name, null, session.getAcknowledgeMode());
        }
        try {
            org.apache.pulsar.client.api.Message<byte[]> message = consumer.receive((int) timeout, TimeUnit.MILLISECONDS);
            return PulsarMessage.decode(consumer, message);
        } catch (Exception err) {
            throw Utils.handleException(err);
        }
    }

    /**
     * Receives the next message if one is immediately available.
     *
     * @return the next message produced for this message consumer, or null if one is not available
     * @throws JMSException if the JMS provider fails to receive the next message due to some internal error.
     */
    @Override
    public Message receiveNoWait() throws JMSException {
        if (messageListenerWrapper != null) {
            throw new IllegalStateException("cannot receive if you have a messageListener");
        }

        if (consumer == null) {
            consumer = session.getFactory().getConsumerForDestination(destination, name, null, session.getAcknowledgeMode());
        }
        try {
            // there is no receiveNoWait in Pulsar, so we are setting a very small timeout
            org.apache.pulsar.client.api.Message<byte[]> message = consumer.receive(1, TimeUnit.MILLISECONDS);
            return PulsarMessage.decode(consumer, message);
        } catch (Exception err) {
            throw Utils.handleException(err);
        }
    }

    /**
     * Closes the message consumer.
     *
     * <p>
     * Since a provider may allocate some resources on behalf of a {@code MessageConsumer} outside the Java virtual machine,
     * clients should close them when they are not needed. Relying on garbage collection to eventually reclaim these
     * resources may not be timely enough.
     *
     * <p>
     * This call will block until a {@code receive} call in progress on this consumer has completed. A blocked
     * {@code receive} call returns null when this message consumer is closed.
     *
     * <p>
     * If this method is called whilst a message listener is in progress in another thread then it will block until the
     * message listener has completed.
     *
     * <p>
     * This method may be called from a message listener's {@code onMessage} method on its own consumer. After this method
     * returns the {@code onMessage} method will be allowed to complete normally.
     *
     * <p>
     * This method is the only {@code MessageConsumer} method that can be called concurrently.
     *
     * @throws JMSException if the JMS provider fails to close the consumer due to some internal error.
     */
    @Override
    public void close() throws JMSException {
        Utils.checkNotOnListener(session);
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception err) {
                throw Utils.handleException(err);
            }
        }
    }
}
