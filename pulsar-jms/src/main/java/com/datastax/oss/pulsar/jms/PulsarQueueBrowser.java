package com.datastax.oss.pulsar.jms;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import java.util.Collections;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
final class PulsarQueueBrowser implements QueueBrowser  {
    static final int SESSION_MODE_MARKER = Integer.MIN_VALUE;
    private final PulsarSession session;
    private final Queue queue;
    private final Consumer<byte[]> consumer;

    public PulsarQueueBrowser(PulsarSession session, Queue queue) throws JMSException {
        this.session = session;
        this.queue = queue;
        this.consumer = session.getFactory()
                .createConsumer((PulsarDestination) queue,
                "queue-browser-"+UUID.randomUUID(),
                        SESSION_MODE_MARKER,
                        SubscriptionMode.NonDurable,
                        SubscriptionType.Exclusive
                );
    }

    /**
     * Gets the queue associated with this queue browser.
     *
     * @return the queue
     * @throws JMSException if the JMS provider fails to get the queue associated with this browser due to some internal
     *                      error.
     */
    @Override
    public Queue getQueue() throws JMSException {
        return queue;
    }

    /**
     * Gets this queue browser's message selector expression.
     *
     * @return this queue browser's message selector, or null if no message selector exists for the message consumer (that
     * is, if the message selector was not set or was set to null or the empty string)
     * @throws JMSException if the JMS provider fails to get the message selector for this browser due to some internal
     *                      error.
     */
    @Override
    public String getMessageSelector() throws JMSException {
        return null;
    }

    /**
     * Gets an enumeration for browsing the current queue messages in the order they would be received.
     *
     * @return an enumeration for browsing the messages
     * @throws JMSException if the JMS provider fails to get the enumeration for this browser due to some internal error.
     */
    @Override
    public Enumeration getEnumeration() throws JMSException {
        MessageId last = Utils.invoke(() -> consumer.getLastMessageId());
        log.info("browser last {}", last);
        if (last.toString().contains("-1:-1")) { // this is bad
            return Collections.emptyEnumeration();
        }
        return new Enumeration() {
            PulsarMessage nextMessage;
            boolean finished;

            @Override
            public boolean hasMoreElements() {
                next();
                return nextMessage != null || !finished;

            }

            @Override
            public Object nextElement() {
                next();
                PulsarMessage next = nextMessage;
                if (next == null) {
                    throw new NoSuchElementException();
                }
                nextMessage = null;
                return next;
            }

            private void next() {
                try {
                    if (!finished && nextMessage == null) {
                        final Message<byte[]> message = consumer.receive();
                        if (message != null) {
                            log.info("browser received {} last {}", message.getMessageId(), last);
                            consumer.acknowledgeAsync(message);
                            nextMessage = PulsarMessage.decode(consumer, message);

                            if (message.getMessageId().compareTo(last) >= 0) {
                                finished = true;
                            }
                        } else {
                            finished = true;
                        }
                    }
                } catch (PulsarClientException | JMSException err) {
                    JMSRuntimeException error = new JMSRuntimeException("Internal error during scan");
                    error.initCause(err);
                    throw error;
                }
            }
        };
    }

    /**
     * Closes the {@code QueueBrowser}.
     *
     * <p>
     * Since a provider may allocate some resources on behalf of a QueueBrowser outside the Java virtual machine, clients
     * should close them when they are not needed. Relying on garbage collection to eventually reclaim these resources may
     * not be timely enough.
     *
     * @throws JMSException if the JMS provider fails to close this browser due to some internal error.
     */
    @Override
    public void close() throws JMSException {
        try {
            consumer.close();
        } catch (PulsarClientException err) {
        }
        session.getFactory().removeConsumer(consumer);
    }
}
