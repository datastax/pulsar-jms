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
package io.streamnative.oss.pulsar.jms;

import io.streamnative.oss.pulsar.jms.selectors.SelectorSupport;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSConsumer;
import javax.jms.JMSException;
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
import org.apache.pulsar.client.impl.ConsumerBase;

@Slf4j
public class PulsarMessageConsumer implements MessageConsumer, TopicSubscriber, QueueReceiver {

  final String subscriptionName;
  private final PulsarSession session;
  private final PulsarDestination destination;
  private SelectorSupport selectorSupport;
  private final Map<String, SelectorSupport> selectorSupportOnSubscriptions = new HashMap<>();
  private final boolean useServerSideFiltering;
  private final boolean noLocal;
  private ConsumerBase<?> consumer;
  private MessageListener listener;
  private final SubscriptionMode subscriptionMode;
  private final SubscriptionType subscriptionType;

  private final boolean dedicatedListenerThread;
  final boolean unregisterSubscriptionOnClose;
  private AtomicBoolean closed = new AtomicBoolean();
  private AtomicBoolean requestClose = new AtomicBoolean();
  private final AtomicBoolean closedWhileActiveTransaction = new AtomicBoolean(false);
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
    this.dedicatedListenerThread = session.isDedicatedListenerThread();
    this.useServerSideFiltering = session.getFactory().isUseServerSideFiltering();
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
  synchronized ConsumerBase<?> getConsumer() throws JMSException {
    if (closed.get()) {
      throw new IllegalStateException("Consumer is closed");
    }
    if (consumer == null) {
      String currentSelector = internalGetMessageSelector();
      consumer =
          session
              .getFactory()
              .createConsumer(
                  destination,
                  subscriptionName,
                  subscriptionMode,
                  subscriptionType,
                  currentSelector,
                  noLocal,
                  session);
    }
    return consumer;
  }

  boolean isClosed() {
    return closed.get();
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
  public String getMessageSelector() throws JMSException {
    checkNotClosed();
    String selector = internalGetMessageSelector();
    String selectorOnSubscription = internalGetMessageSelectorFromSubscription();
    if (selectorOnSubscription == null) {
      return selector;
    }
    if (selector == null) {
      return selectorOnSubscription;
    }
    return "(" + selectorOnSubscription + ") AND (" + selector + ")";
  }

  private synchronized String internalGetMessageSelector() {
    return selectorSupport != null ? selectorSupport.getSelector() : null;
  }

  private synchronized String internalGetMessageSelectorFromSubscription() {
    if (!useServerSideFiltering || selectorSupportOnSubscriptions.isEmpty()) {
      return null;
    }
    // pick one
    SelectorSupport next = selectorSupportOnSubscriptions.values().iterator().next();
    return next != null ? next.getSelector() : null;
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

  void checkNotClosed() throws JMSException {
    session.checkNotClosed();
    if (closed.get() || requestClose.get()) {
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
    session.ensureListenerThread(this);
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

  public List<Message> batchReceive(int maxMessages, long timeoutMs) throws JMSException {
    if (closed.get()) {
      return Collections.emptyList();
    }
    // ensure that the internal consumer has been created
    getConsumer();

    if (maxMessages < 0) {
      maxMessages = 1;
    }

    // block until we receive a message or timeout expires
    Message message = receive(timeoutMs);
    if (message == null) {
      // no message
      return Collections.emptyList();
    }

    List<Message> result = new ArrayList<>(maxMessages);
    result.add(message);

    // try to drain up to maxMessages without blocking
    while (hasSomePrefetchedMessages() && result.size() < maxMessages) {
      message = receiveNoWait();
      if (message == null) {
        break;
      }
      result.add(message);
    }
    return result;
  }

  public boolean hasSomePrefetchedMessages() {
    return consumer.getTotalIncomingMessages() > 0;
  }

  synchronized Message receiveWithTimeoutAndValidateType(long timeout, Class expectedType)
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
                        Consumer<?> consumer = getConsumer();
                        org.apache.pulsar.client.api.Message<?> message =
                            consumer.receive(stepTimeout, TimeUnit.MILLISECONDS);
                        if (message == null) {
                          return null;
                        }
                        return handleReceivedMessage(
                            message, consumer, expectedType, null, noLocal);
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

  private void skipMessage(org.apache.pulsar.client.api.Message<?> message) throws JMSException {
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
      org.apache.pulsar.client.api.Message<?> message,
      Consumer<?> consumer,
      Class expectedType,
      java.util.function.Consumer<PulsarMessage> listenerCode,
      boolean noLocalFilter)
      throws JMSException, org.apache.pulsar.client.api.PulsarClientException {
    receivedMessages.incrementAndGet();

    PulsarMessage result = PulsarMessage.decode(this, consumer, message);
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
    SelectorSupport selectorSupportOnSubscription =
        getSelectorSupportOnSubscription(message.getTopicName());
    if (selectorSupportOnSubscription != null
        && requiresClientSideFiltering(message)
        && !selectorSupportOnSubscription.matches(result)) {
      if (log.isDebugEnabled()) {
        log.debug(
            "msg {} does not match subscription selector {}",
            result,
            selectorSupportOnSubscription.getSelector());
      }
      // this message should have been filtered out on the server
      // because the selector is on the subscription
      // this case may happen with batch messages
      skippedMessages.incrementAndGet();
      consumer.acknowledgeAsync(message.getMessageId());
      return null;
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
    // in case of useServerSideFiltering this filter is also applied on the broker, is the Plugin
    // is
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
      // open transaction now, the message will be acknowledged on commit()
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
        && session.getAcknowledgeMode() != PulsarJMSConstants.INDIVIDUAL_ACKNOWLEDGE
        && session.getAcknowledgeMode() != Session.SESSION_TRANSACTED) {
      session.unregisterUnacknowledgedMessage(result);
    }
    if (requestClose.get()) {
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
  public void close() throws JMSException {
    if (Utils.isOnMessageListener(session, this)) {
      requestClose.set(true);
      return;
    }
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    Consumer<?> consumer = getInternalConsumer();
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
    } else if (session.getTransacted()) {
      closedWhileActiveTransaction.set(true);
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
    return new PulsarJMSConsumer(this);
  }

  void acknowledge(
      org.apache.pulsar.client.api.Message<?> receivedPulsarMessage,
      PulsarMessage message,
      Consumer<?> consumer)
      throws JMSException {
    try {
      consumer.acknowledge(receivedPulsarMessage);
      session.unregisterUnacknowledgedMessage(message);
    } catch (PulsarClientException err) {
      throw Utils.handleException(err);
    }
  }

  void runListenerNoWait() {
    runListener(0);
  }

  void runListener(int timeout) {
    if (closed.get()) {
      return;
    }
    boolean executeAgain = true;
    boolean someMessageFound = false;
    synchronized (this) {
      final MessageListener messageListener = this.listener;
      if (messageListener == null) {
        return;
      }

      while (executeAgain) {
        // activate checks about methods that cannot be called inside a listener
        // and block any concurrent "close()" operations
        boolean messageFound =
            Utils.executeMessageListenerInSessionContext(
                session,
                this,
                () -> {
                  try {
                    ConsumerBase<?> consumer = getConsumer();
                    if (timeout == 0) { // we don't want to wait
                      if (consumer.getTotalIncomingMessages() <= 0) {
                        return false;
                      } else {
                        // this is the same thing that happens in ConsumerBase
                        // in the native Pulsar client
                        // process all the messages in the internal buffer
                        // timeout is zero, but we know that there are messages
                        // in the internal queue
                      }
                    }
                    org.apache.pulsar.client.api.Message<?> message =
                        consumer.receive(timeout, TimeUnit.MILLISECONDS);
                    if (message == null) {
                      return false;
                    }
                    handleReceivedMessage(
                        message, consumer, null, messageListener::onMessage, noLocal);
                    return true;
                  } catch (PulsarClientException.AlreadyClosedException closed) {
                    log.error("Error while receiving message on Closed consumer {}", this);
                  } catch (JMSException | PulsarClientException err) {
                    log.error("Error while receiving message on consumer {}", this, err);
                    session.onError(err);
                  }
                  return false;
                });
        if (!dedicatedListenerThread && messageFound) {
          executeAgain = true;
          someMessageFound = true;
        } else {
          executeAgain = false;
        }
      }
    }
    if (!dedicatedListenerThread && !closed.get()) {
      session.scheduleConsumerListenerCycle(this, someMessageFound);
    }
  }

  void closeDuringRollback() throws JMSException {
    try {
      consumer.close();
      session.removeConsumer(this);
    } catch (Exception err) {
      throw Utils.handleException(err);
    }
  }

  public void closeInternal() throws JMSException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    requestClose.set(false);
    try {
      if (consumer != null) {
        consumer.close();
        consumer = null;
      }
    } catch (Exception err) {
      throw Utils.handleException(err);
    }
  }

  public boolean isClosedWhileActiveTransaction() {
    return closedWhileActiveTransaction.get();
  }

  public void negativeAck(org.apache.pulsar.client.api.Message<?> message) {
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

  public synchronized SelectorSupport getSelectorSupportOnSubscription(String topicName)
      throws JMSException {
    if (!useServerSideFiltering) {
      return null;
    }
    if (!selectorSupportOnSubscriptions.containsKey(topicName)) {
      String subscriptionName =
          destination.isQueue()
              ? session.getFactory().getQueueSubscriptionName(destination)
              : this.subscriptionName;
      String selector =
          session
              .getFactory()
              .downloadServerSideFilter(topicName, subscriptionName, subscriptionMode);
      selectorSupportOnSubscriptions.put(topicName, SelectorSupport.build(selector, true));
    }
    return selectorSupportOnSubscriptions.get(topicName);
  }

  synchronized Consumer<?> getInternalConsumer() {
    return consumer;
  }

  PulsarDestination getDestination() {
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

  synchronized void refreshServerSideSelectors() {
    int size = selectorSupportOnSubscriptions.size();
    if (size > 0) {
      selectorSupportOnSubscriptions.clear();
      log.info("Refreshing {} server-side filters on {}", size, destination);
    }
  }
}
