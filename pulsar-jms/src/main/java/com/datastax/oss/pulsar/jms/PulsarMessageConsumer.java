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

import com.datastax.oss.pulsar.jms.selectors.SelectorSupport;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSConsumer;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;

@Slf4j
public class PulsarMessageConsumer implements MessageConsumer, TopicSubscriber, QueueReceiver {

  final String subscriptionName;
  private final PulsarSession session;
  private final PulsarDestination destination;
  private SelectorSupport selectorSupport;
  private final boolean noLocal;
  private Consumer<byte[]> consumer;
  private MessageListener listener;
  private final SubscriptionMode subscriptionMode;
  private final SubscriptionType subscriptionType;
  final boolean unregisterSubscriptionOnClose;
  private boolean closed;
  private boolean requestClose;
  final AtomicLong receivedMessages = new AtomicLong();
  final AtomicLong skippedMessages = new AtomicLong();

  public PulsarMessageConsumer(
      String subscriptionName,
      PulsarDestination destination,
      PulsarSession session,
      SubscriptionMode subscriptionMode,
      SubscriptionType subscriptionType,
      String selector,
      boolean unregisterSubscriptionOnClose,
      boolean noLocal)
      throws JMSException {
    this.noLocal = noLocal;
    session.checkNotClosed();
    if (destination == null) {
      throw new InvalidDestinationException("Invalid destination");
    }
    if (destination instanceof PulsarTemporaryDestination) {
      PulsarTemporaryDestination dest = (PulsarTemporaryDestination) destination;
      if (dest.getSession() != session) {
        throw new JMSException(
            "Cannot subscribe to a temporary destination not created but this session");
      }
    }
    this.subscriptionName = subscriptionName;
    this.session = session;
    this.destination = destination;
    this.subscriptionMode = destination.isQueue() ? SubscriptionMode.Durable : subscriptionMode;
    this.subscriptionType = destination.isQueue() ? SubscriptionType.Shared : subscriptionType;

    // when we are in an Exclusive subscription
    // it is always safe to apply selectors, noLocal and timeToLive
    // when we are in a Shared subscription and we receive an invalid message
    // we have to bounce it?
    this.selectorSupport =
        SelectorSupport.build(
            selector,
            subscriptionType == SubscriptionType.Exclusive
                || session.getFactory().isEnableClientSideEmulation()
                || session.getFactory().isUseServerSideFiltering());
    this.unregisterSubscriptionOnClose = unregisterSubscriptionOnClose;
    if (noLocal
        && subscriptionType != SubscriptionType.Exclusive
        && !session.getFactory().isEnableClientSideEmulation()) {
      throw new IllegalStateException(
          "noLocal is not enabled by default with subscriptionType "
              + subscriptionType
              + ", please set jms.enableClientSideEmulation=true");
    }
  }

  public PulsarMessageConsumer subscribe() throws JMSException {
    if (destination.isQueue()) {
      // to not create eagerly the Consumer for Queues
      // but create the shared subscription
      session.getFactory().ensureQueueSubscription(destination);
    } else {
      getConsumer();
    }
    session.registerConsumer(this);
    return this;
  }

  // Visible for testing
  synchronized Consumer<byte[]> getConsumer() throws JMSException {
    if (closed) {
      throw new IllegalStateException("Consumer is closed");
    }
    if (consumer == null) {
      AtomicReference<String> selectorOnSubscriptionReceiver = new AtomicReference<>();
      String currentSelector = internalGetMessageSelector();
      consumer =
          session
              .getFactory()
              .createConsumer(
                  destination,
                  subscriptionName,
                  session.getAcknowledgeMode(),
                  subscriptionMode,
                  subscriptionType,
                  currentSelector,
                  noLocal,
                  session.getConnection().getConnectionId(),
                  selectorOnSubscriptionReceiver);
      String jmsSelectorOnSubscription = selectorOnSubscriptionReceiver.get();
      if (jmsSelectorOnSubscription != null && !jmsSelectorOnSubscription.isEmpty()) {
        if (currentSelector != null && !currentSelector.isEmpty()) {
          if (!currentSelector.equals(jmsSelectorOnSubscription)) {
            throw new javax.jms.InvalidSelectorException(
                "If you set locally a selector it must match"
                    + "  the selector set at subscription level, in this case it is "
                    + jmsSelectorOnSubscription);
          }
        }
        selectorSupport = SelectorSupport.build(jmsSelectorOnSubscription, true);
      }
    }
    return consumer;
  }

  /**
   * Gets this message consumer's message selector expression.
   *
   * @return this message consumer's message selector, or null if no message selector exists for the
   *     message consumer (that is, if the message selector was not set or was set to null or the
   *     empty string)
   * @throws JMSException if the JMS provider fails to get the message selector due to some internal
   *     error.
   */
  @Override
  public synchronized String getMessageSelector() throws JMSException {
    checkNotClosed();
    if (destination.isQueue() && session.getFactory().isUseServerSideFiltering()) {
      // ensure we download properly the selector from the server
      getConsumer();
    }
    return internalGetMessageSelector();
  }

  private synchronized String internalGetMessageSelector() {
    return selectorSupport != null ? selectorSupport.getSelector() : null;
  }

  /**
   * Gets the {@code MessageConsumer}'s {@code MessageListener}.
   *
   * <p>This method must not be used in a Java EE web or EJB application. Doing so may cause a
   * {@code JMSException} to be thrown though this is not guaranteed.
   *
   * @return the {@code MessageConsumer}'s {@code MessageListener}, or null if one was not set
   * @throws JMSException if the JMS provider fails to get the {@code MessageListener} for one of
   *     the following reasons:
   *     <ul>
   *       <li>an internal error has occurred or
   *       <li>this method has been called in a Java EE web or EJB application (though it is not
   *           guaranteed that an exception is thrown in this case)
   *     </ul>
   *
   * @see MessageConsumer#setMessageListener(MessageListener)
   */
  @Override
  public synchronized MessageListener getMessageListener() throws JMSException {
    checkNotClosed();
    return listener;
  }

  synchronized void checkNotClosed() throws JMSException {
    session.checkNotClosed();
    if (closed || requestClose) {
      throw new IllegalStateException("This consumer is closed");
    }
  }

  /**
   * Sets the {@code MessageConsumer}'s {@code MessageListener}.
   *
   * <p>Setting the the {@code MessageListener} to null is the equivalent of unsetting the {@code
   * MessageListener} for the {@code MessageConsumer}.
   *
   * <p>The effect of calling this method while messages are being consumed by an existing listener
   * or the {@code MessageConsumer} is being used to consume messages synchronously is undefined.
   *
   * <p>This method must not be used in a Java EE web or EJB application. Doing so may cause a
   * {@code JMSException} to be thrown though this is not guaranteed.
   *
   * @param listener the listener to which the messages are to be delivered
   * @throws JMSException if the JMS provider fails to set the {@code MessageConsumer}'s {@code
   *     MessageListener} for one of the following reasons:
   *     <ul>
   *       <li>an internal error has occurred or
   *       <li>this method has been called in a Java EE web or EJB application (though it is not
   *           guaranteed that an exception is thrown in this case)
   *     </ul>
   *
   * @see MessageConsumer#getMessageListener()
   */
  @Override
  public synchronized void setMessageListener(MessageListener listener) throws JMSException {
    checkNotClosed();
    this.listener = listener;
    session.ensureListenerThread();
  }

  /**
   * Receives the next message produced for this message consumer.
   *
   * <p>This call blocks indefinitely until a message is produced or until this message consumer is
   * closed.
   *
   * <p>If this {@code receive} is done within a transaction, the consumer retains the message until
   * the transaction commits.
   *
   * @return the next message produced for this message consumer, or null if this message consumer
   *     is concurrently closed
   * @throws JMSException if the JMS provider fails to receive the next message due to some internal
   *     error.
   */
  @Override
  public Message receive() throws JMSException {
    return receiveWithTimeoutAndValidateType(Long.MAX_VALUE, null);
  }

  /**
   * Receives the next message that arrives within the specified timeout interval.
   *
   * <p>This call blocks until a message arrives, the timeout expires, or this message consumer is
   * closed. A {@code timeout} of zero never expires, and the call blocks indefinitely.
   *
   * @param timeout the timeout value (in milliseconds)
   * @return the next message produced for this message consumer, or null if the timeout expires or
   *     this message consumer is concurrently closed
   * @throws JMSException if the JMS provider fails to receive the next message due to some internal
   *     error.
   */
  @Override
  public Message receive(long timeout) throws JMSException {
    return receiveWithTimeoutAndValidateType(timeout, null);
  }

  private synchronized Message receiveWithTimeoutAndValidateType(long timeout, Class expectedType)
      throws JMSException {
    checkNotClosed();
    if (listener != null) {
      throw new IllegalStateException("cannot receive if you have a messageListener");
    }
    // time to wait for the Connection to "start"
    final int acquireConnectionStartTime =
        timeout == Long.MAX_VALUE ? Integer.MAX_VALUE : (int) timeout;
    // time to wait for each cycle
    final int stepTimeout = timeout < 100 ? ((int) timeout) : 100;
    final long start = System.currentTimeMillis();
    return session.executeOperationIfConnectionStarted(
        () -> {
          do {
            Message result =
                session.executeCriticalOperation(
                    () -> {
                      try {
                        Consumer<byte[]> consumer = getConsumer();
                        org.apache.pulsar.client.api.Message<byte[]> message =
                            consumer.receive(stepTimeout, TimeUnit.MILLISECONDS);
                        if (message == null) {
                          return null;
                        }
                        return handleReceivedMessage(message, expectedType, null, noLocal);
                      } catch (Exception err) {
                        throw Utils.handleException(err);
                      }
                    });
            if (result != null) {
              return result;
            }
          } while (System.currentTimeMillis() - start < timeout && !session.isClosed());

          return null;
        },
        acquireConnectionStartTime);
  }

  /**
   * Receives the next message if one is immediately available.
   *
   * @return the next message produced for this message consumer, or null if one is not available
   * @throws JMSException if the JMS provider fails to receive the next message due to some internal
   *     error.
   */
  @Override
  public Message receiveNoWait() throws JMSException {
    // there is no receiveNoWait in Pulsar, so we are setting a very small timeout
    return receive(1);
  }

  private void skipMessage(org.apache.pulsar.client.api.Message<byte[]> message)
      throws JMSException {
    skippedMessages.incrementAndGet();
    if (subscriptionType == SubscriptionType.Exclusive
        || session.getFactory().isAcknowledgeRejectedMessages()) {
      // we are the only one that will ever receive this message
      // we can acknowledge it
      if (session.getTransaction() != null) {
        consumer.acknowledgeAsync(message.getMessageId(), session.getTransaction());
      } else {
        consumer.acknowledgeAsync(message.getMessageId());
      }
    } else {
      if (log.isDebugEnabled()) {
        log.debug("nAck filtered msg {}", message.getMessageId());
      }
      consumer.negativeAcknowledge(message);
    }
  }

  private PulsarMessage handleReceivedMessage(
      org.apache.pulsar.client.api.Message<byte[]> message,
      Class expectedType,
      java.util.function.Consumer<PulsarMessage> listenerCode,
      boolean noLocalFilter)
      throws JMSException, org.apache.pulsar.client.api.PulsarClientException {

    receivedMessages.incrementAndGet();

    PulsarMessage result = PulsarMessage.decode(this, message);
    Consumer<byte[]> consumer = getConsumer();
    if (expectedType != null && !result.isBodyAssignableTo(expectedType)) {
      if (log.isDebugEnabled()) {
        log.debug(
            "negativeAcknowledge for message {} that cannot be converted to {}",
            message,
            expectedType);
      }
      consumer.negativeAcknowledge(message);
      throw new MessageFormatException(
          "The message ("
              + result.messageType()
              + ","
              + result
              + ",) cannot be converted to a "
              + expectedType);
    }
    SelectorSupport selectorSupport = getSelectorSupport();
    if (selectorSupport != null
        && requiresClientSideFiltering(message)
        && !selectorSupport.matches(result)) {
      if (log.isDebugEnabled()) {
        log.debug("msg {} does not match selector {}", result, selectorSupport.getSelector());
      }
      skipMessage(message);
      return null;
    }
    if (noLocalFilter) {
      String senderConnectionID = result.getStringProperty("JMSConnectionID");
      if (senderConnectionID != null
          && senderConnectionID.equals(session.getConnection().getConnectionId())) {
        if (log.isDebugEnabled()) {
          log.debug("msg {} was generated from this connection {}", result, senderConnectionID);
        }
        skipMessage(message);
        return null;
      }
    }
    // in case of useServerSideFiltering this filter is also applied on the broker, is the Plugin is
    // present
    if (result.getJMSExpiration() > 0 && System.currentTimeMillis() >= result.getJMSExpiration()) {
      if (log.isDebugEnabled()) {
        log.debug("msg {} expired at {}", result, Instant.ofEpochMilli(result.getJMSExpiration()));
      }
      skipMessage(message);
      return null;
    }

    // this must happen before the execution of the listener
    // in order to support Session.recover
    session.registerUnacknowledgedMessage(result);

    if (listenerCode != null) {
      try {
        listenerCode.accept(result);
      } catch (Throwable t) {
        log.error("Listener thrown error, calling negativeAcknowledge", t);
        consumer.negativeAcknowledge(message);
        throw Utils.handleException(t);
      }
      if (result.isNegativeAcked()) {
        // this may happen if the listener calls "Session.recover"
        return null;
      }
    }

    if (session.getTransacted()) {
      // open transaction now
      session.getTransaction();
    } else if (session.getAcknowledgeMode() == Session.AUTO_ACKNOWLEDGE) {
      consumer.acknowledge(message);
    } else if (session.getAcknowledgeMode() == Session.DUPS_OK_ACKNOWLEDGE) {
      consumer
          .acknowledgeAsync(message)
          .whenComplete(
              (m, ex) -> {
                if (ex != null) {
                  log.error("Cannot acknowledge message {} {}", message, ex);
                }
              });
    }
    if (session.getAcknowledgeMode() != Session.CLIENT_ACKNOWLEDGE
        && session.getAcknowledgeMode() != Session.SESSION_TRANSACTED) {
      session.unregisterUnacknowledgedMessage(result);
    }
    if (requestClose) {
      closeInternal();
    }
    return result;
  }

  private boolean requiresClientSideFiltering(org.apache.pulsar.client.api.Message<?> message) {
    // for batch messages we have to verify the condition locally
    // because the broker can only ACCEPT or REJECT whole batches.
    // the broker will send the batch (Entry) if at least one message matches the selector
    boolean isBatch = (message.getMessageId() instanceof BatchMessageIdImpl);
    return isBatch || !session.getFactory().isUseServerSideFiltering();
  }

  /**
   * Closes the message consumer.
   *
   * <p>Since a provider may allocate some resources on behalf of a {@code MessageConsumer} outside
   * the Java virtual machine, clients should close them when they are not needed. Relying on
   * garbage collection to eventually reclaim these resources may not be timely enough.
   *
   * <p>This call will block until a {@code receive} call in progress on this consumer has
   * completed. A blocked {@code receive} call returns null when this message consumer is closed.
   *
   * <p>If this method is called whilst a message listener is in progress in another thread then it
   * will block until the message listener has completed.
   *
   * <p>This method may be called from a message listener's {@code onMessage} method on its own
   * consumer. After this method returns the {@code onMessage} method will be allowed to complete
   * normally.
   *
   * <p>This method is the only {@code MessageConsumer} method that can be called concurrently.
   *
   * @throws JMSException if the JMS provider fails to close the consumer due to some internal
   *     error.
   */
  @Override
  public synchronized void close() throws JMSException {
    if (Utils.isOnMessageListener(session, this)) {
      requestClose = true;
      return;
    }
    if (closed) {
      return;
    }
    closed = true;
    if (consumer == null) {
      return;
    }
    if (!session.isTransactionStarted()) {
      session.executeCriticalOperation(
          () -> {
            try {
              consumer.close();
              session.removeConsumer(this);
              return null;
            } catch (Exception err) {
              throw Utils.handleException(err);
            }
          });
    }
  }

  @Override
  public String toString() {
    return "PulsarConsumer{subscriptionName="
        + subscriptionName
        + ", destination="
        + destination
        + '}';
  }

  /**
   * Gets the {@code Topic} associated with this subscriber.
   *
   * @return this subscriber's {@code Topic}
   * @throws JMSException if the JMS provider fails to get the topic for this topic subscriber due
   *     to some internal error.
   */
  @Override
  public synchronized Topic getTopic() throws JMSException {
    checkNotClosed();
    if (destination.isTopic()) {
      return (Topic) destination;
    }
    throw new JMSException("This consumer has been created on a Queue");
  }

  @Override
  public synchronized Queue getQueue() throws JMSException {
    checkNotClosed();
    if (destination.isQueue()) {
      return (Queue) destination;
    }
    throw new JMSException("This consumer has been created on a Topic");
  }

  /**
   * Gets the {@code NoLocal} attribute for this subscriber. The default value for this attribute is
   * false.
   *
   * @return true if locally published messages are being inhibited
   * @throws JMSException if the JMS provider fails to get the {@code NoLocal} attribute for this
   *     topic subscriber due to some internal error.
   */
  @Override
  public synchronized boolean getNoLocal() throws JMSException {
    checkNotClosed();
    return noLocal;
  }

  public JMSConsumer asJMSConsumer() {
    return new PulsarJMSConsumer();
  }

  synchronized void acknowledge(
      org.apache.pulsar.client.api.Message<byte[]> receivedPulsarMessage, PulsarMessage message)
      throws JMSException {
    Consumer<byte[]> consumer = getConsumer();
    try {
      consumer.acknowledge(receivedPulsarMessage);
      session.unregisterUnacknowledgedMessage(message);
    } catch (PulsarClientException err) {
      throw Utils.handleException(err);
    }
  }

  synchronized void runListener(int timeout) {
    if (closed || listener == null) {
      return;
    }
    // activate checks about methods that cannot be called inside a listener
    // and block any concurrent "close()" operations
    Utils.executeMessageListenerInSessionContext(
        session,
        this,
        () -> {
          if (closed) {
            return;
          }
          try {
            Consumer<byte[]> consumer = getConsumer();
            org.apache.pulsar.client.api.Message<byte[]> message =
                consumer.receive(timeout, TimeUnit.MILLISECONDS);
            if (message == null) {
              return;
            }
            PulsarMessage pulsarMessage =
                handleReceivedMessage(
                    message,
                    null,
                    (pmessage) -> {
                      listener.onMessage(pmessage);
                    },
                    noLocal);
          } catch (PulsarClientException.AlreadyClosedException closed) {
            log.error("Error while receiving message con Closed consumer {}", this);
          } catch (JMSException | PulsarClientException err) {
            log.error("Error while receiving message con consumer {}", this, err);
            session.onError(err);
          }
        });
  }

  public void closeInternal() throws JMSException {
    if (closed) {
      return;
    }
    closed = true;
    requestClose = false;
    try {
      if (consumer != null) {
        consumer.close();
        consumer = null;
      }
    } catch (Exception err) {
      throw Utils.handleException(err);
    }
  }

  public void negativeAck(org.apache.pulsar.client.api.Message<byte[]> message) {
    if (consumer != null) {
      consumer.negativeAcknowledge(message);
    }
  }

  void redeliverUnacknowledgedMessages() {
    if (consumer != null) {
      consumer.redeliverUnacknowledgedMessages();
    }
  }

  PulsarSession getSession() {
    return session;
  }

  private synchronized SelectorSupport getSelectorSupport() {
    return selectorSupport;
  }

  Consumer<byte[]> getInternalConsumer() {
    return consumer;
  }

  Destination getDestination() {
    return destination;
  }

  public SubscriptionType getSubscriptionType() {
    return subscriptionType;
  }

  public long getReceivedMessages() {
    return receivedMessages.get();
  }

  public long getSkippedMessages() {
    return skippedMessages.get();
  }

  public class PulsarJMSConsumer implements JMSConsumer {

    public PulsarMessageConsumer asPulsarMessageConsumer() {
      return PulsarMessageConsumer.this;
    }

    @Override
    public String getMessageSelector() {
      return Utils.runtimeException(() -> PulsarMessageConsumer.this.getMessageSelector());
    }

    @Override
    public MessageListener getMessageListener() throws JMSRuntimeException {
      return Utils.runtimeException(() -> PulsarMessageConsumer.this.getMessageListener());
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSRuntimeException {
      Utils.runtimeException(() -> PulsarMessageConsumer.this.setMessageListener(listener));
    }

    @Override
    public Message receive() {
      return Utils.runtimeException(() -> PulsarMessageConsumer.this.receive());
    }

    @Override
    public Message receive(long timeout) {
      return Utils.runtimeException(() -> PulsarMessageConsumer.this.receive(timeout));
    }

    @Override
    public Message receiveNoWait() {
      return Utils.runtimeException(() -> PulsarMessageConsumer.this.receiveNoWait());
    }

    @Override
    public void close() {
      Utils.runtimeException(() -> PulsarMessageConsumer.this.close());
    }

    @Override
    public <T> T receiveBody(Class<T> c) {
      return Utils.runtimeException(
          () -> {
            Message msg =
                PulsarMessageConsumer.this.receiveWithTimeoutAndValidateType(Long.MAX_VALUE, c);
            return msg == null ? null : msg.getBody(c);
          });
    }

    @Override
    public <T> T receiveBody(Class<T> c, long timeout) {
      return Utils.runtimeException(
          () -> {
            Message msg = PulsarMessageConsumer.this.receiveWithTimeoutAndValidateType(timeout, c);
            return msg == null ? null : msg.getBody(c);
          });
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> c) {
      return Utils.runtimeException(
          () -> {
            Message msg = PulsarMessageConsumer.this.receiveWithTimeoutAndValidateType(1, c);
            return msg == null ? null : msg.getBody(c);
          });
    }
  }
}
