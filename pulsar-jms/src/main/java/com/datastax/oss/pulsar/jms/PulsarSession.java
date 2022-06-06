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

import com.datastax.oss.pulsar.jms.messages.PulsarBytesMessage;
import com.datastax.oss.pulsar.jms.messages.PulsarMapMessage;
import com.datastax.oss.pulsar.jms.messages.PulsarObjectMessage;
import com.datastax.oss.pulsar.jms.messages.PulsarSimpleMessage;
import com.datastax.oss.pulsar.jms.messages.PulsarStreamMessage;
import com.datastax.oss.pulsar.jms.messages.PulsarTextMessage;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.jms.TransactionRolledBackException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;

@Slf4j
public class PulsarSession implements Session, QueueSession, TopicSession {

  private final PulsarConnection connection;
  private boolean jms20;
  private final int sessionMode;
  private final boolean transacted;
  // this is to emulate QueueSession/TopicSession
  private boolean allowQueueOperations = true;
  private boolean allowTopicOperations = true;
  Transaction transaction;
  private MessageListener messageListener;
  private final Map<PulsarDestination, Producer<byte[]>> producers = new HashMap<>();
  private final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
  private final List<PulsarMessage> unackedMessages = new ArrayList<>();
  private final Map<String, PulsarDestination> destinationBySubscription = new HashMap<>();
  private volatile boolean closed;
  private volatile ListenerThread listenerThread;
  // this collection is accessed by the Listener thread
  private final List<PulsarMessageConsumer> consumers = new CopyOnWriteArrayList<>();
  private final List<PulsarQueueBrowser> browsers = new CopyOnWriteArrayList<>();

  public PulsarSession(int sessionMode, PulsarConnection connection) throws JMSException {
    this.jms20 = false;
    this.connection = connection;
    this.sessionMode = sessionMode;
    this.transacted = sessionMode == Session.SESSION_TRANSACTED;
    validateSessionMode(sessionMode);
    if (sessionMode == SESSION_TRANSACTED) {
      if (!connection.getFactory().isEnableTransaction()) {
        throw new JMSException(
            "Please enable transactions on PulsarConnectionFactory with enableTransaction=true");
      }
    }
  }

  Transaction getTransaction() throws JMSException {
    if (transaction == null && sessionMode == SESSION_TRANSACTED) {
      this.transaction = startTransaction(connection);
    }
    return this.transaction;
  }

  private Transaction startTransaction(PulsarConnection connection) throws JMSException {
    Transaction transaction = null;
    int createTransactionTrials = 10;
    while (createTransactionTrials-- > 0) {
      try {
        try {
          transaction = connection.getFactory().getPulsarClient().newTransaction().build().get();
          break;
        } catch (ExecutionException err) {
          if (err.getCause()
              instanceof TransactionCoordinatorClientException.CoordinatorNotFoundException) {
            log.info("Transaction service not available {}", err.getCause().getMessage());
            Thread.sleep(1000);
          } else {
            throw Utils.handleException(err.getCause());
          }
        }
      } catch (Exception err) {
        throw Utils.handleException(err);
      }
    }
    if (transaction == null) {
      throw new JMSException("Cannot create a Transaction in time");
    }
    return transaction;
  }

  private static void validateSessionMode(int sessionMode) throws JMSException {
    switch (sessionMode) {
      case Session.SESSION_TRANSACTED:
      case Session.AUTO_ACKNOWLEDGE:
      case Session.CLIENT_ACKNOWLEDGE:
      case PulsarJMSConstants.INDIVIDUAL_ACKNOWLEDGE:
      case Session.DUPS_OK_ACKNOWLEDGE:
        break;
      default:
        throw new JMSException("Invalid sessionMode " + sessionMode);
    }
  }

  PulsarConnectionFactory getFactory() {
    return connection.getFactory();
  }

  /**
   * Creates a {@code BytesMessage} object. A {@code BytesMessage} object is used to send a message
   * containing a stream of uninterpreted bytes.
   *
   * <p>The message object returned may be sent using any {@code Session} or {@code JMSContext}. It
   * is not restricted to being sent using the {@code JMSContext} used to create it.
   *
   * <p>The message object returned may be optimised for use with the JMS provider used to create
   * it. However it can be sent using any JMS provider, not just the JMS provider used to create it.
   *
   * @return A {@code BytesMessage} object.
   * @throws JMSException if the JMS provider fails to create this message due to some internal
   *     error.
   */
  @Override
  public PulsarBytesMessage createBytesMessage() throws JMSException {
    checkNotClosed();
    return new PulsarBytesMessage();
  }

  /**
   * Creates a {@code MapMessage} object. A {@code MapMessage} object is used to send a
   * self-defining set of name-value pairs, where names are {@code String} objects and values are
   * primitive values in the Java programming language.
   *
   * <p>The message object returned may be sent using any {@code Session} or {@code JMSContext}. It
   * is not restricted to being sent using the {@code JMSContext} used to create it.
   *
   * <p>The message object returned may be optimised for use with the JMS provider used to create
   * it. However it can be sent using any JMS provider, not just the JMS provider used to create it.
   *
   * @return A {@code MapMessage} object.
   * @throws JMSException if the JMS provider fails to create this message due to some internal
   *     error.
   */
  @Override
  public MapMessage createMapMessage() throws JMSException {
    checkNotClosed();
    return new PulsarMapMessage();
  }

  public MapMessage createMapMessage(Map<String, Object> body) throws JMSException {
    return new PulsarMapMessage(body);
  }

  /**
   * Creates a {@code Message} object. The {@code Message} interface is the root interface of all
   * JMS messages. A {@code Message} object holds all the standard message header information. It
   * can be sent when a message containing only header information is sufficient.
   *
   * <p>The message object returned may be sent using any {@code Session} or {@code JMSContext}. It
   * is not restricted to being sent using the {@code JMSContext} used to create it.
   *
   * <p>The message object returned may be optimised for use with the JMS provider used to create
   * it. However it can be sent using any JMS provider, not just the JMS provider used to create it.
   *
   * @return A {@code Message} object.
   * @throws JMSException if the JMS provider fails to create this message due to some internal
   *     error.
   */
  @Override
  public Message createMessage() throws JMSException {
    checkNotClosed();
    return new PulsarSimpleMessage();
  }

  /**
   * Creates an {@code ObjectMessage} object. An {@code ObjectMessage} object is used to send a
   * message that contains a serializable Java object.
   *
   * <p>The message object returned may be sent using any {@code Session} or {@code JMSContext}. It
   * is not restricted to being sent using the {@code JMSContext} used to create it.
   *
   * <p>The message object returned may be optimised for use with the JMS provider used to create
   * it. However it can be sent using any JMS provider, not just the JMS provider used to create it.
   *
   * @return A {@code ObjectMessage} object.
   * @throws JMSException if the JMS provider fails to create this message due to some internal
   *     error.
   */
  @Override
  public ObjectMessage createObjectMessage() throws JMSException {
    checkNotClosed();
    return new PulsarObjectMessage();
  }

  /**
   * Creates an initialized {@code ObjectMessage} object. An {@code ObjectMessage} object is used to
   * send a message that contains a serializable Java object.
   *
   * <p>The message object returned may be sent using any {@code Session} or {@code JMSContext}. It
   * is not restricted to being sent using the {@code JMSContext} used to create it.
   *
   * <p>The message object returned may be optimised for use with the JMS provider used to create
   * it. However it can be sent using any JMS provider, not just the JMS provider used to create it.
   *
   * @param object the object to use to initialize this message
   * @return A {@code ObjectMessage} object.
   * @throws JMSException if the JMS provider fails to create this message due to some internal
   *     error.
   */
  @Override
  public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
    checkNotClosed();
    PulsarObjectMessage res = new PulsarObjectMessage();
    res.setObject(object);
    return res;
  }

  /**
   * Creates a {@code StreamMessage} object. A {@code StreamMessage} object is used to send a
   * self-defining stream of primitive values in the Java programming language.
   *
   * <p>The message object returned may be sent using any {@code Session} or {@code JMSContext}. It
   * is not restricted to being sent using the {@code JMSContext} used to create it.
   *
   * <p>The message object returned may be optimised for use with the JMS provider used to create
   * it. However it can be sent using any JMS provider, not just the JMS provider used to create it.
   *
   * @return A {@code StreamMessage} object.
   * @throws JMSException if the JMS provider fails to create this message due to some internal
   *     error.
   */
  @Override
  public StreamMessage createStreamMessage() throws JMSException {
    checkNotClosed();
    return new PulsarStreamMessage();
  }

  /**
   * Creates a {@code TextMessage} object. A {@code TextMessage} object is used to send a message
   * containing a {@code String} object.
   *
   * <p>The message object returned may be sent using any {@code Session} or {@code JMSContext}. It
   * is not restricted to being sent using the {@code JMSContext} used to create it.
   *
   * <p>The message object returned may be optimised for use with the JMS provider used to create
   * it. However it can be sent using any JMS provider, not just the JMS provider used to create it.
   *
   * @return A {@code TextMessage} object.
   * @throws JMSException if the JMS provider fails to create this message due to some internal
   *     error.
   */
  @Override
  public TextMessage createTextMessage() throws JMSException {
    checkNotClosed();
    return new PulsarTextMessage((String) null);
  }

  /**
   * Creates an initialized {@code TextMessage} object. A {@code TextMessage} object is used to send
   * a message containing a {@code String}.
   *
   * <p>The message object returned may be sent using any {@code Session} or {@code JMSContext}. It
   * is not restricted to being sent using the {@code JMSContext} used to create it.
   *
   * <p>The message object returned may be optimised for use with the JMS provider used to create
   * it. However it can be sent using any JMS provider, not just the JMS provider used to create it.
   *
   * @param text the string used to initialize this message
   * @return A {@code TextMessage} object.
   * @throws JMSException if the JMS provider fails to create this message due to some internal
   *     error.
   */
  @Override
  public TextMessage createTextMessage(String text) throws JMSException {
    checkNotClosed();
    return new PulsarTextMessage(text);
  }

  /**
   * Indicates whether the session is in transacted mode.
   *
   * @return true if the session is in transacted mode
   * @throws JMSException if the JMS provider fails to return the transaction mode due to some
   *     internal error.
   */
  @Override
  public boolean getTransacted() throws JMSException {
    checkNotClosed();
    return sessionMode == SESSION_TRANSACTED;
  }

  /**
   * Returns the acknowledgement mode of the session. The acknowledgement mode is set at the time
   * that the session is created. If the session is transacted, the acknowledgement mode is ignored.
   *
   * @return If the session is not transacted, returns the current acknowledgement mode for the
   *     session. If the session is transacted, returns SESSION_TRANSACTED.
   * @throws JMSException if the JMS provider fails to return the acknowledgment mode due to some
   *     internal error.
   * @see Connection#createSession
   * @since JMS 1.1
   */
  @Override
  public int getAcknowledgeMode() throws JMSException {
    checkNotClosed();
    return sessionMode;
  }

  /**
   * Commits all messages done in this transaction and releases any locks currently held.
   *
   * <p>This method must not return until any incomplete asynchronous send operations for this
   * <tt>Session</tt> have been completed and any <tt>CompletionListener</tt> callbacks have
   * returned. Incomplete sends should be allowed to complete normally unless an error occurs.
   *
   * <p>A <tt>CompletionListener</tt> callback method must not call <tt>commit</tt> on its own
   * <tt>Session</tt>. Doing so will cause an <tt>IllegalStateException</tt> to be thrown.
   *
   * @throws IllegalStateException
   *     <ul>
   *       <li>the session is not using a local transaction
   *       <li>this method has been called by a <tt>CompletionListener</tt> callback method on its
   *           own <tt>Session</tt>
   *     </ul>
   *
   * @throws JMSException if the JMS provider fails to commit the transaction due to some internal
   *     error.
   * @throws TransactionRolledBackException if the transaction is rolled back due to some internal
   *     error during commit.
   */
  @Override
  public void commit() throws JMSException {
    checkNotClosed();
    Utils.checkNotOnMessageListener(this);
    Utils.checkNotOnMessageProducer(this, null);
    if (!transacted) {
      throw new IllegalStateException("session is not transacted");
    }
    closeLock.readLock().lock();
    try {
      if (transaction != null) {
        // we are postponing to this moment the acknowledgment
        List<CompletableFuture<?>> handles = new ArrayList<>();
        for (PulsarMessage msg : unackedMessages) {
          handles.add(msg.acknowledgeInternalInTransaction(transaction));
        }
        handles.add(transaction.commit());
        Utils.get(CompletableFuture.allOf(handles.toArray(new CompletableFuture<?>[0])));
        unackedMessages.clear();
        transaction = null;
      }
    } finally {
      closeLock.readLock().unlock();
    }
  }

  /**
   * Rolls back any messages done in this transaction and releases any locks currently held.
   *
   * <p>This method must not return until any incomplete asynchronous send operations for this
   * <tt>Session</tt> have been completed and any <tt>CompletionListener</tt> callbacks have
   * returned. Incomplete sends should be allowed to complete normally unless an error occurs.
   *
   * <p>A <tt>CompletionListener</tt> callback method must not call <tt>commit</tt> on its own
   * <tt>Session</tt>. Doing so will cause an <tt>IllegalStateException</tt> to be thrown.
   *
   * @throws IllegalStateException
   *     <ul>
   *       <li>the session is not using a local transaction
   *       <li>this method has been called by a <tt>CompletionListener</tt> callback method on its
   *           own <tt>Session</tt>
   *     </ul>
   *
   * @throws JMSException if the JMS provider fails to roll back the transaction due to some
   *     internal error.
   */
  @Override
  public void rollback() throws JMSException {
    checkNotClosed();
    Utils.checkNotOnMessageListener(this);
    Utils.checkNotOnMessageProducer(this, null);
    closeLock.readLock().lock();
    try {
      if (!transacted) {
        throw new IllegalStateException("session is not transacted");
      }
      rollbackInternal();
    } finally {
      closeLock.readLock().unlock();
    }
  }

  private void rollbackInternal() throws JMSException {
    for (PulsarMessageConsumer consumer : consumers) {
      consumer.redeliverUnacknowledgedMessages();
      if (consumer.isClosedWhileActiveTransaction()) {
        // consumer closed before calling "rollback"
        consumer.closeDuringRollback();
      }
    }
    unackedMessages.clear();
    if (transaction != null) {
      Utils.get(transaction.abort());
    }
    transaction = null;
  }

  /**
   * Closes the session.
   *
   * <p>Since a provider may allocate some resources on behalf of a session outside the JVM, clients
   * should close the resources when they are not needed. Relying on garbage collection to
   * eventually reclaim these resources may not be timely enough.
   *
   * <p>There is no need to close the producers and consumers of a closed session.
   *
   * <p>This call will block until a {@code receive} call or message listener in progress has
   * completed. A blocked message consumer {@code receive} call returns {@code null} when this
   * session is closed.
   *
   * <p>However if the close method is called from a message listener on its own {@code Session},
   * then it will either fail and throw a {@code javax.jms.IllegalStateException}, or it will
   * succeed and close the {@code Session}, blocking until any pending receive call in progress has
   * completed. If close succeeds and the acknowledge mode of the {@code Session} is set to {@code
   * AUTO_ACKNOWLEDGE}, the current message will still be acknowledged automatically when the {@code
   * onMessage} call completes.
   *
   * <p>Since two alternative behaviors are permitted in this case, applications should avoid
   * calling close from a message listener on its own {@code Session} because this is not portable.
   *
   * <p>This method must not return until any incomplete asynchronous send operations for this
   * <tt>Session</tt> have been completed and any <tt>CompletionListener</tt> callbacks have
   * returned. Incomplete sends should be allowed to complete normally unless an error occurs.
   *
   * <p>For the avoidance of doubt, if an exception listener for this session's connection is
   * running when {@code close} is invoked, there is no requirement for the {@code close} call to
   * wait until the exception listener has returned before it may return.
   *
   * <p>Closing a transacted session must roll back the transaction in progress.
   *
   * <p>This method is the only {@code Session} method that can be called concurrently.
   *
   * <p>A <tt>CompletionListener</tt> callback method must not call <tt>close</tt> on its own
   * <tt>Session</tt>. Doing so will cause an <tt>IllegalStateException</tt> to be thrown.
   *
   * <p>Invoking any other {@code Session} method on a closed session must throw a {@code
   * IllegalStateException}. Closing a closed session must <I>not</I> throw an exception.
   *
   * @throws IllegalStateException
   *     <ul>
   *       <li>this method has been called by a <tt>MessageListener </tt> on its own
   *           <tt>Session</tt>
   *       <li>this method has been called by a <tt>CompletionListener</tt> callback method on its
   *           own <tt>Session</tt>
   *     </ul>
   *
   * @throws JMSException if the JMS provider fails to close the session due to some internal error.
   */
  @Override
  public void close() throws JMSException {
    Utils.checkNotOnSessionCallback(this);
    closeLock.writeLock().lock();
    try {
      if (closed) {
        return;
      }
      closed = true;
      if (transacted && transaction != null) {
        rollbackInternal();
      }
      unackedMessages.clear();
      for (PulsarMessageConsumer consumer : consumers) {
        consumer.closeInternal();
      }
      for (PulsarQueueBrowser browser : browsers) {
        browser.close();
      }
      consumers.clear();
      browsers.clear();
    } finally {
      closeLock.writeLock().unlock();
      connection.unregisterSession(this);
    }
    // wait for the thread to complete
    if (listenerThread != null) {
      try {
        listenerThread.join();
      } catch (InterruptedException err) {
        // ignore
      } finally {
        listenerThread = null;
      }
    }
  }

  /**
   * Stops message delivery in this session, and restarts message delivery with the oldest
   * unacknowledged message.
   *
   * <p>All consumers deliver messages in a serial order. Acknowledging a received message
   * automatically acknowledges all messages that have been delivered to the client.
   *
   * <p>Restarting a session causes it to take the following actions:
   *
   * <ul>
   *   <li>Stop message delivery
   *   <li>Mark all messages that might have been delivered but not acknowledged as "redelivered"
   *   <li>Restart the delivery sequence including all unacknowledged messages that had been
   *       previously delivered. Redelivered messages do not have to be delivered in exactly their
   *       original delivery order.
   * </ul>
   *
   * @throws JMSException if the JMS provider fails to stop and restart message delivery due to some
   *     internal error.
   * @throws IllegalStateException if the method is called by a transacted session.
   */
  @Override
  public void recover() throws JMSException {
    checkNotClosed();
    if (transacted) {
      throw new IllegalStateException("cannot call this method inside a transacted session");
    }
    log.info("recover, unacked messages {}", unackedMessages);
    for (PulsarMessage msg : unackedMessages) {
      log.info("recovering message {}", msg);
      msg.negativeAck();
    }
    unackedMessages.clear();
  }

  /**
   * Returns the session's distinguished message listener (optional).
   *
   * <p>This method must not be used in a Java EE web or EJB application. Doing so may cause a
   * {@code JMSException} to be thrown though this is not guaranteed.
   *
   * @return the distinguished message listener associated with this session
   * @throws JMSException if the JMS provider fails to get the session's distinguished message
   *     listener for one of the following reasons:
   *     <ul>
   *       <li>an internal error has occurred
   *       <li>this method has been called in a Java EE web or EJB application (though it is not
   *           guaranteed that an exception is thrown in this case)
   *     </ul>
   *
   * @see Session#setMessageListener
   * @see ServerSessionPool
   * @see ServerSession
   */
  @Override
  public MessageListener getMessageListener() throws JMSException {
    return messageListener;
  }

  /**
   * Sets the session's distinguished message listener (optional).
   *
   * <p>When the distinguished message listener is set, no other form of message receipt in the
   * session can be used; however, all forms of sending messages are still supported.
   *
   * <p>This is an expert facility not used by ordinary JMS clients.
   *
   * <p>This method must not be used in a Java EE web or EJB application. Doing so may cause a
   * {@code JMSException} to be thrown though this is not guaranteed.
   *
   * @param listener the message listener to associate with this session
   * @throws JMSException if the JMS provider fails to set the session's distinguished message
   *     listener for one of the following reasons:
   *     <ul>
   *       <li>an internal error has occurred
   *       <li>this method has been called in a Java EE web or EJB application (though it is not
   *           guaranteed that an exception is thrown in this case)
   *     </ul>
   *
   * @see Session#getMessageListener
   * @see ServerSessionPool
   * @see ServerSession
   */
  @Override
  public void setMessageListener(MessageListener listener) throws JMSException {
    Objects.requireNonNull(listener);
    this.messageListener = listener;
  }

  /**
   * Optional operation, intended to be used only by Application Servers, not by ordinary JMS
   * clients.
   *
   * <p>This method must not be used in a Java EE web or EJB application. Doing so may cause a
   * {@code JMSRuntimeException} to be thrown though this is not guaranteed.
   *
   * @throws JMSRuntimeException if this method has been called in a Java EE web or EJB application
   *     (though it is not guaranteed that an exception is thrown in this case)
   * @see ServerSession
   */
  @Override
  public void run() {
    if (consumers.isEmpty() || !connection.isStarted()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException err) {
        // early exit
      }
      return;
    }
    // we can run this in a tight loop
    // because as far as there consumers we are going to
    // block on some Consumer#receive call
    for (PulsarMessageConsumer consumer : consumers) {
      try {
        connection.executeInConnectionPausedLock(
            () -> {
              consumer.runListener(100);
              return null;
            },
            0);
      } catch (Throwable err) {
        log.error("Error in Session Thread {}", this, err);
      }
      if (!connection.isStarted()) {
        return;
      }
    }
  }

  /**
   * Creates a {@code MessageProducer} to send messages to the specified destination.
   *
   * <p>A client uses a {@code MessageProducer} object to send messages to a destination. Since
   * {@code Queue} and {@code Topic} both inherit from {@code Destination}, they can be used in the
   * destination parameter to create a {@code MessageProducer} object.
   *
   * @param destination the {@code Destination} to send to, or null if this is a producer which does
   *     not have a specified destination.
   * @return A {@code MessageProducer} to send messages.
   * @throws JMSException if the session fails to create a MessageProducer due to some internal
   *     error.
   * @throws InvalidDestinationException if an invalid destination is specified.
   * @since JMS 1.1
   */
  @Override
  public PulsarMessageProducer createProducer(Destination destination) throws JMSException {
    connection.setAllowSetClientId(false);
    return new PulsarMessageProducer(this, destination);
  }

  /**
   * Creates a {@code MessageConsumer} for the specified destination. Since {@code Queue} and {@code
   * Topic} both inherit from {@code Destination}, they can be used in the destination parameter to
   * create a {@code MessageConsumer}.
   *
   * @param destination the {@code Destination} to access.
   * @return A {@code MessageConsumer} for the specified destination.
   * @throws JMSException if the session fails to create a consumer due to some internal error.
   * @throws InvalidDestinationException if an invalid destination is specified.
   * @since JMS 1.1
   */
  @Override
  public PulsarMessageConsumer createConsumer(Destination destination) throws JMSException {
    return createConsumer(destination, null);
  }

  /**
   * Creates a {@code MessageConsumer} for the specified destination, using a message selector.
   * Since {@code Queue} and {@code Topic} both inherit from {@code Destination}, they can be used
   * in the destination parameter to create a {@code MessageConsumer}.
   *
   * <p>A client uses a {@code MessageConsumer} object to receive messages that have been sent to a
   * destination.
   *
   * @param destination the {@code Destination} to access
   * @param messageSelector only messages with properties matching the message selector expression
   *     are delivered. A value of null or an empty string indicates that there is no message
   *     selector for the message consumer.
   * @return A {@code MessageConsumer} for the specified destination.
   * @throws JMSException if the session fails to create a MessageConsumer due to some internal
   *     error.
   * @throws InvalidDestinationException if an invalid destination is specified.
   * @throws InvalidSelectorException if the message selector is invalid.
   * @since JMS 1.1
   */
  @Override
  public PulsarMessageConsumer createConsumer(Destination destination, String messageSelector)
      throws JMSException {
    if (destination == null) {
      throw new InvalidDestinationException("null destination");
    }
    return createConsumer(destination, messageSelector, false);
  }

  /**
   * Creates a {@code MessageConsumer} for the specified destination, specifying a message selector
   * and the {@code noLocal} parameter.
   *
   * <p>Since {@code Queue} and {@code Topic} both inherit from {@code Destination}, they can be
   * used in the destination parameter to create a {@code MessageConsumer}.
   *
   * <p>A client uses a {@code MessageConsumer} object to receive messages that have been published
   * to a destination.
   *
   * <p>The {@code noLocal} argument is for use when the destination is a topic and the session's
   * connection is also being used to publish messages to that topic. If {@code noLocal} is set to
   * true then the {@code MessageConsumer} will not receive messages published to the topic by its
   * own connection. The default value of this argument is false. If the destination is a queue then
   * the effect of setting {@code noLocal} to true is not specified.
   *
   * @param destination the {@code Destination} to access
   * @param messageSelector only messages with properties matching the message selector expression
   *     are delivered. A value of null or an empty string indicates that there is no message
   *     selector for the message consumer.
   * @param noLocal - if true, and the destination is a topic, then the {@code MessageConsumer} will
   *     not receive messages published to the topic by its own connection.
   * @return A {@code MessageConsumer} for the specified destination.
   * @throws JMSException if the session fails to create a MessageConsumer due to some internal
   *     error.
   * @throws InvalidDestinationException if an invalid destination is specified.
   * @throws InvalidSelectorException if the message selector is invalid.
   * @since JMS 1.1
   */
  @Override
  public PulsarMessageConsumer createConsumer(
      Destination destination, String messageSelector, boolean noLocal) throws JMSException {
    if (destination == null) {
      throw new InvalidDestinationException("null destination");
    }
    PulsarDestination pulsarDestination = PulsarConnectionFactory.toPulsarDestination(destination);
    return new PulsarMessageConsumer(
            UUID.randomUUID().toString(),
            pulsarDestination,
            this,
            SubscriptionMode.NonDurable,
            getFactory().getExclusiveSubscriptionTypeForSimpleConsumers(destination),
            messageSelector,
            false,
            noLocal)
        .subscribe();
  }

  /**
   * Creates a shared non-durable subscription with the specified name on the specified topic (if
   * one does not already exist) and creates a consumer on that subscription. This method creates
   * the non-durable subscription without a message selector.
   *
   * <p>If a shared non-durable subscription already exists with the same name and client identifier
   * (if set), and the same topic and message selector value has been specified, then this method
   * creates a {@code MessageConsumer} on the existing subscription.
   *
   * <p>A non-durable shared subscription is used by a client which needs to be able to share the
   * work of receiving messages from a topic subscription amongst multiple consumers. A non-durable
   * shared subscription may therefore have more than one consumer. Each message from the
   * subscription will be delivered to only one of the consumers on that subscription. Such a
   * subscription is not persisted and will be deleted (together with any undelivered messages
   * associated with it) when there are no consumers on it. The term "consumer" here means a {@code
   * MessageConsumer} or {@code JMSConsumer} object in any client.
   *
   * <p>A shared non-durable subscription is identified by a name specified by the client and by the
   * client identifier (which may be unset). An application which subsequently wishes to create a
   * consumer on that shared non-durable subscription must use the same client identifier.
   *
   * <p>If a shared non-durable subscription already exists with the same name and client identifier
   * (if set) but a different topic or message selector has been specified, and there is a consumer
   * already active (i.e. not closed) on the subscription, then a {@code JMSException} will be
   * thrown.
   *
   * <p>There is no restriction on durable subscriptions and shared non-durable subscriptions having
   * the same name and clientId (which may be unset). Such subscriptions would be completely
   * separate.
   *
   * @param topic the {@code Topic} to subscribe to
   * @param sharedSubscriptionName the name used to identify the shared non-durable subscription
   * @return A shared non-durable subscription with the specified name on the specified topic.
   * @throws JMSException if the session fails to create the shared non-durable subscription and
   *     {@code MessageConsumer} due to some internal error.
   * @throws InvalidDestinationException if an invalid topic is specified.
   * @throws InvalidSelectorException if the message selector is invalid.
   * @since JMS 2.0
   */
  @Override
  public PulsarMessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName)
      throws JMSException {
    return createSharedConsumer(topic, sharedSubscriptionName, null);
  }

  /**
   * Creates a shared non-durable subscription with the specified name on the specified topic (if
   * one does not already exist) specifying a message selector, and creates a consumer on that
   * subscription.
   *
   * <p>If a shared non-durable subscription already exists with the same name and client identifier
   * (if set), and the same topic and message selector has been specified, then this method creates
   * a {@code MessageConsumer} on the existing subscription.
   *
   * <p>A non-durable shared subscription is used by a client which needs to be able to share the
   * work of receiving messages from a topic subscription amongst multiple consumers. A non-durable
   * shared subscription may therefore have more than one consumer. Each message from the
   * subscription will be delivered to only one of the consumers on that subscription. Such a
   * subscription is not persisted and will be deleted (together with any undelivered messages
   * associated with it) when there are no consumers on it. The term "consumer" here means a {@code
   * MessageConsumer} or {@code JMSConsumer} object in any client.
   *
   * <p>A shared non-durable subscription is identified by a name specified by the client and by the
   * client identifier (which may be unset). An application which subsequently wishes to create a
   * consumer on that shared non-durable subscription must use the same client identifier.
   *
   * <p>If a shared non-durable subscription already exists with the same name and client identifier
   * (if set) but a different topic or message selector has been specified, and there is a consumer
   * already active (i.e. not closed) on the subscription, then a {@code JMSException} will be
   * thrown.
   *
   * <p>There is no restriction on durable subscriptions and shared non-durable subscriptions having
   * the same name and clientId (which may be unset). Such subscriptions would be completely
   * separate.
   *
   * @param topic the {@code Topic} to subscribe to
   * @param sharedSubscriptionName the name used to identify the shared non-durable subscription
   * @param messageSelector only messages with properties matching the message selector expression
   *     are added to the shared non-durable subscription. A value of null or an empty string
   *     indicates that there is no message selector for the shared non-durable subscription.
   * @return A shared non-durable subscription with the specified name on the specified topic.
   * @throws JMSException if the session fails to create the shared non-durable subscription and
   *     {@code MessageConsumer} due to some internal error.
   * @throws InvalidDestinationException if an invalid topic is specified.
   * @throws InvalidSelectorException if the message selector is invalid.
   * @since JMS 2.0
   */
  @Override
  public PulsarMessageConsumer createSharedConsumer(
      Topic topic, String sharedSubscriptionName, String messageSelector) throws JMSException {
    if (topic == null) {
      throw new InvalidDestinationException("null destination");
    }
    checkTopicOperationEnabled();
    sharedSubscriptionName = connection.prependClientId(sharedSubscriptionName, true);
    PulsarDestination pulsarDestination = PulsarConnectionFactory.toPulsarDestination(topic);
    registerSubscriptionName(pulsarDestination, sharedSubscriptionName, true);
    return new PulsarMessageConsumer(
            sharedSubscriptionName,
            pulsarDestination,
            this,
            SubscriptionMode.NonDurable,
            getFactory().getTopicSharedSubscriptionType(),
            messageSelector,
            true,
            false)
        .subscribe();
  }

  /**
   * Creates a {@code Queue} object which encapsulates a specified provider-specific queue name.
   *
   * <p>The use of provider-specific queue names in an application may render the application
   * non-portable. Portable applications are recommended to not use this method but instead look up
   * an administratively-defined {@code Queue} object using JNDI.
   *
   * <p>Note that this method simply creates an object that encapsulates the name of a queue. It
   * does not create the physical queue in the JMS provider. JMS does not provide a method to create
   * the physical queue, since this would be specific to a given JMS provider. Creating a physical
   * queue is provider-specific and is typically an administrative task performed by an
   * administrator, though some providers may create them automatically when needed. The one
   * exception to this is the creation of a temporary queue, which is done using the {@code
   * createTemporaryQueue} method.
   *
   * @param queueName A provider-specific queue name
   * @return a Queue object which encapsulates the specified name
   * @throws JMSException if a Queue object cannot be created due to some internal error
   */
  @Override
  public PulsarQueue createQueue(String queueName) throws JMSException {
    checkNotClosed();
    checkQueueOperationEnabled();
    return new PulsarQueue(queueName);
  }

  /**
   * Creates a {@code Topic} object which encapsulates a specified provider-specific topic name.
   *
   * <p>The use of provider-specific topic names in an application may render the application
   * non-portable. Portable applications are recommended to not use this method but instead look up
   * an administratively-defined {@code Topic} object using JNDI.
   *
   * <p>Note that this method simply creates an object that encapsulates the name of a topic. It
   * does not create the physical topic in the JMS provider. JMS does not provide a method to create
   * the physical topic, since this would be specific to a given JMS provider. Creating a physical
   * topic is provider-specific and is typically an administrative task performed by an
   * administrator, though some providers may create them automatically when needed. The one
   * exception to this is the creation of a temporary topic, which is done using the {@code
   * createTemporaryTopic} method.
   *
   * @param topicName A provider-specific topic name
   * @return a Topic object which encapsulates the specified name
   * @throws JMSException if a Topic object cannot be created due to some internal error
   */
  @Override
  public PulsarTopic createTopic(String topicName) throws JMSException {
    checkNotClosed();
    checkTopicOperationEnabled();
    return new PulsarTopic(topicName);
  }

  /**
   * Creates an unshared durable subscription on the specified topic (if one does not already exist)
   * and creates a consumer on that durable subscription. This method creates the durable
   * subscription without a message selector and with a {@code noLocal} value of {@code false}.
   *
   * <p>A durable subscription is used by an application which needs to receive all the messages
   * published on a topic, including the ones published when there is no active consumer associated
   * with it. The JMS provider retains a record of this durable subscription and ensures that all
   * messages from the topic's publishers are retained until they are delivered to, and acknowledged
   * by, a consumer on this durable subscription or until they have expired.
   *
   * <p>A durable subscription will continue to accumulate messages until it is deleted using the
   * {@code unsubscribe} method.
   *
   * <p>This method may only be used with unshared durable subscriptions. Any durable subscription
   * created using this method will be unshared. This means that only one active (i.e. not closed)
   * consumer on the subscription may exist at a time. The term "consumer" here means a {@code
   * TopicSubscriber}, {@code MessageConsumer} or {@code JMSConsumer} object in any client.
   *
   * <p>An unshared durable subscription is identified by a name specified by the client and by the
   * client identifier, which must be set. An application which subsequently wishes to create a
   * consumer on that unshared durable subscription must use the same client identifier.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier,
   * and the same topic, message selector and {@code noLocal} value has been specified, and there is
   * no consumer already active (i.e. not closed) on the durable subscription then this method
   * creates a {@code TopicSubscriber} on the existing durable subscription.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier,
   * and there is a consumer already active (i.e. not closed) on the durable subscription, then a
   * {@code JMSException} will be thrown.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier
   * but a different topic, message selector or {@code noLocal} value has been specified, and there
   * is no consumer already active (i.e. not closed) on the durable subscription then this is
   * equivalent to unsubscribing (deleting) the old one and creating a new one.
   *
   * <p>A shared durable subscription and an unshared durable subscription may not have the same
   * name and client identifier. If a shared durable subscription already exists with the same name
   * and client identifier then a {@code JMSException} is thrown.
   *
   * <p>There is no restriction on durable subscriptions and shared non-durable subscriptions having
   * the same name and clientId. Such subscriptions would be completely separate.
   *
   * <p>This method is identical to the corresponding {@code createDurableConsumer} method except
   * that it returns a {@code TopicSubscriber} rather than a {@code MessageConsumer} to represent
   * the consumer.
   *
   * @param topic the non-temporary {@code Topic} to subscribe to
   * @param name the name used to identify this subscription
   * @return An unshared durable subscription on the specified topic.
   * @throws InvalidDestinationException if an invalid topic is specified.
   * @throws IllegalStateException if the client identifier is unset
   * @throws JMSException
   *     <ul>
   *       <li>if the session fails to create the unshared durable subscription and {@code
   *           TopicSubscriber} due to some internal error
   *       <li>if an unshared durable subscription already exists with the same name and client
   *           identifier, and there is a consumer already active
   *       <li>if a shared durable subscription already exists with the same name and client
   *           identifier
   *     </ul>
   *
   * @since JMS 1.1
   */
  @Override
  public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
    return createDurableSubscriber(topic, name, null, false);
  }

  /**
   * Creates an unshared durable subscription on the specified topic (if one does not already
   * exist), specifying a message selector and the {@code noLocal} parameter, and creates a consumer
   * on that durable subscription.
   *
   * <p>A durable subscription is used by an application which needs to receive all the messages
   * published on a topic, including the ones published when there is no active consumer associated
   * with it. The JMS provider retains a record of this durable subscription and ensures that all
   * messages from the topic's publishers are retained until they are delivered to, and acknowledged
   * by, a consumer on this durable subscription or until they have expired.
   *
   * <p>A durable subscription will continue to accumulate messages until it is deleted using the
   * {@code unsubscribe} method.
   *
   * <p>This method may only be used with unshared durable subscriptions. Any durable subscription
   * created using this method will be unshared. This means that only one active (i.e. not closed)
   * consumer on the subscription may exist at a time. The term "consumer" here means a {@code
   * TopicSubscriber}, {@code MessageConsumer} or {@code JMSConsumer} object in any client.
   *
   * <p>An unshared durable subscription is identified by a name specified by the client and by the
   * client identifier, which must be set. An application which subsequently wishes to create a
   * consumer on that unshared durable subscription must use the same client identifier.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier,
   * and the same topic, message selector and {@code noLocal} value has been specified, and there is
   * no consumer already active (i.e. not closed) on the durable subscription then this method
   * creates a {@code TopicSubscriber} on the existing durable subscription.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier,
   * and there is a consumer already active (i.e. not closed) on the durable subscription, then a
   * {@code JMSException} will be thrown.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier
   * but a different topic, message selector or {@code noLocal} value has been specified, and there
   * is no consumer already active (i.e. not closed) on the durable subscription then this is
   * equivalent to unsubscribing (deleting) the old one and creating a new one.
   *
   * <p>If {@code noLocal} is set to true then any messages published to the topic using this
   * session's connection, or any other connection with the same client identifier, will not be
   * added to the durable subscription.
   *
   * <p>A shared durable subscription and an unshared durable subscription may not have the same
   * name and client identifier. If a shared durable subscription already exists with the same name
   * and client identifier then a {@code JMSException} is thrown.
   *
   * <p>There is no restriction on durable subscriptions and shared non-durable subscriptions having
   * the same name and clientId. Such subscriptions would be completely separate.
   *
   * <p>This method is identical to the corresponding {@code createDurableConsumer} method except
   * that it returns a {@code TopicSubscriber} rather than a {@code MessageConsumer} to represent
   * the consumer.
   *
   * @param topic the non-temporary {@code Topic} to subscribe to
   * @param name the name used to identify this subscription
   * @param messageSelector only messages with properties matching the message selector expression
   *     are added to the durable subscription. A value of null or an empty string indicates that
   *     there is no message selector for the durable subscription.
   * @param noLocal if true then any messages published to the topic using this session's
   *     connection, or any other connection with the same client identifier, will not be added to
   *     the durable subscription.
   * @return An unshared durable subscription on the specified topic.
   * @throws InvalidDestinationException if an invalid topic is specified.
   * @throws InvalidSelectorException if the message selector is invalid.
   * @throws IllegalStateException if the client identifier is unset
   * @throws JMSException
   *     <ul>
   *       <li>if the session fails to create the unshared durable subscription and {@code
   *           TopicSubscriber} due to some internal error
   *       <li>if an unshared durable subscription already exists with the same name and client
   *           identifier, and there is a consumer already active
   *       <li>if a shared durable subscription already exists with the same name and client
   *           identifier
   *     </ul>
   *
   * @since JMS 1.1
   */
  @Override
  public PulsarMessageConsumer createDurableSubscriber(
      Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
    return createDurableSubscriber(topic, name, messageSelector, noLocal, false);
  }

  private PulsarMessageConsumer createDurableSubscriber(
      Topic topic, String name, String messageSelector, boolean noLocal, boolean allowUnsetClientId)
      throws JMSException {
    checkTopicOperationEnabled();
    if (topic == null) {
      throw new InvalidDestinationException("null destination");
    }
    PulsarTopic pulsarTopic = (PulsarTopic) PulsarConnectionFactory.toPulsarDestination(topic);
    name = connection.prependClientId(name, allowUnsetClientId);
    registerSubscriptionName(pulsarTopic, name, false);

    return new PulsarMessageConsumer(
            name,
            pulsarTopic,
            this,
            SubscriptionMode.Durable,
            SubscriptionType.Exclusive,
            messageSelector,
            true,
            noLocal)
        .subscribe();
  }

  private void registerSubscriptionName(PulsarDestination topic, String name, boolean shared)
      throws JMSException {
    PulsarDestination alreadyExists = destinationBySubscription.put(name, topic);
    if (alreadyExists != null && alreadyExists.equals(topic) && !shared) {
      // we cannot perform a cluster wide check
      throw new IllegalStateException(
          "a subscription with name " + name + " is already registered on this session");
    }
  }

  private void unregisterSubscriptionName(String name, Topic topic) {
    PulsarDestination existing = destinationBySubscription.get(name);
    if (existing != null && existing.equals(topic)) {
      destinationBySubscription.remove(name);
    }
  }

  /**
   * Creates an unshared durable subscription on the specified topic (if one does not already exist)
   * and creates a consumer on that durable subscription. This method creates the durable
   * subscription without a message selector and with a {@code noLocal} value of {@code false}.
   *
   * <p>A durable subscription is used by an application which needs to receive all the messages
   * published on a topic, including the ones published when there is no active consumer associated
   * with it. The JMS provider retains a record of this durable subscription and ensures that all
   * messages from the topic's publishers are retained until they are delivered to, and acknowledged
   * by, a consumer on this durable subscription or until they have expired.
   *
   * <p>A durable subscription will continue to accumulate messages until it is deleted using the
   * {@code unsubscribe} method.
   *
   * <p>This method may only be used with unshared durable subscriptions. Any durable subscription
   * created using this method will be unshared. This means that only one active (i.e. not closed)
   * consumer on the subscription may exist at a time. The term "consumer" here means a {@code
   * TopicSubscriber}, {@code MessageConsumer} or {@code JMSConsumer} object in any client.
   *
   * <p>An unshared durable subscription is identified by a name specified by the client and by the
   * client identifier, which must be set. An application which subsequently wishes to create a
   * consumer on that unshared durable subscription must use the same client identifier.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier,
   * and the same topic, message selector and {@code noLocal} value has been specified, and there is
   * no consumer already active (i.e. not closed) on the durable subscription then this method
   * creates a {@code MessageConsumer} on the existing durable subscription.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier,
   * and there is a consumer already active (i.e. not closed) on the durable subscription, then a
   * {@code JMSException} will be thrown.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier
   * but a different topic, message selector or {@code noLocal} value has been specified, and there
   * is no consumer already active (i.e. not closed) on the durable subscription then this is
   * equivalent to unsubscribing (deleting) the old one and creating a new one.
   *
   * <p>A shared durable subscription and an unshared durable subscription may not have the same
   * name and client identifier. If a shared durable subscription already exists with the same name
   * and client identifier then a {@code JMSException} is thrown.
   *
   * <p>There is no restriction on durable subscriptions and shared non-durable subscriptions having
   * the same name and clientId. Such subscriptions would be completely separate.
   *
   * <p>This method is identical to the corresponding {@code createDurableSubscriber} method except
   * that it returns a {@code MessageConsumer} rather than a {@code TopicSubscriber} to represent
   * the consumer.
   *
   * @param topic the non-temporary {@code Topic} to subscribe to
   * @param name the name used to identify this subscription
   * @return An unshared durable subscription on the specified topic.
   * @throws InvalidDestinationException if an invalid topic is specified.
   * @throws IllegalStateException if the client identifier is unset
   * @throws JMSException
   *     <ul>
   *       <li>if the session fails to create the unshared durable subscription and {@code
   *           MessageConsumer} due to some internal error
   *       <li>if an unshared durable subscription already exists with the same name and client
   *           identifier, and there is a consumer already active
   *       <li>if a shared durable subscription already exists with the same name and client
   *           identifier
   *     </ul>
   *
   * @since JMS 2.0
   */
  @Override
  public PulsarMessageConsumer createDurableConsumer(Topic topic, String name) throws JMSException {
    return createDurableConsumer(topic, name, null, false);
  }

  /**
   * Creates an unshared durable subscription on the specified topic (if one does not already
   * exist), specifying a message selector and the {@code noLocal} parameter, and creates a consumer
   * on that durable subscription.
   *
   * <p>A durable subscription is used by an application which needs to receive all the messages
   * published on a topic, including the ones published when there is no active consumer associated
   * with it. The JMS provider retains a record of this durable subscription and ensures that all
   * messages from the topic's publishers are retained until they are delivered to, and acknowledged
   * by, a consumer on this durable subscription or until they have expired.
   *
   * <p>A durable subscription will continue to accumulate messages until it is deleted using the
   * {@code unsubscribe} method.
   *
   * <p>This method may only be used with unshared durable subscriptions. Any durable subscription
   * created using this method will be unshared. This means that only one active (i.e. not closed)
   * consumer on the subscription may exist at a time. The term "consumer" here means a {@code
   * TopicSubscriber}, {@code MessageConsumer} or {@code JMSConsumer} object in any client.
   *
   * <p>An unshared durable subscription is identified by a name specified by the client and by the
   * client identifier, which must be set. An application which subsequently wishes to create a
   * consumer on that unshared durable subscription must use the same client identifier.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier,
   * and the same topic, message selector and {@code noLocal} value has been specified, and there is
   * no consumer already active (i.e. not closed) on the durable subscription then this method
   * creates a {@code MessageConsumer} on the existing durable subscription.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier,
   * and there is a consumer already active (i.e. not closed) on the durable subscription, then a
   * {@code JMSException} will be thrown.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier
   * but a different topic, message selector or {@code noLocal} value has been specified, and there
   * is no consumer already active (i.e. not closed) on the durable subscription then this is
   * equivalent to unsubscribing (deleting) the old one and creating a new one.
   *
   * <p>If {@code noLocal} is set to true then any messages published to the topic using this
   * session's connection, or any other connection with the same client identifier, will not be
   * added to the durable subscription.
   *
   * <p>A shared durable subscription and an unshared durable subscription may not have the same
   * name and client identifier. If a shared durable subscription already exists with the same name
   * and client identifier then a {@code JMSException} is thrown.
   *
   * <p>There is no restriction on durable subscriptions and shared non-durable subscriptions having
   * the same name and clientId. Such subscriptions would be completely separate.
   *
   * <p>This method is identical to the corresponding {@code createDurableSubscriber} method except
   * that it returns a {@code MessageConsumer} rather than a {@code TopicSubscriber} to represent
   * the consumer.
   *
   * @param topic the non-temporary {@code Topic} to subscribe to
   * @param name the name used to identify this subscription
   * @param messageSelector only messages with properties matching the message selector expression
   *     are added to the durable subscription. A value of null or an empty string indicates that
   *     there is no message selector for the durable subscription.
   * @param noLocal if true then any messages published to the topic using this session's
   *     connection, or any other connection with the same client identifier, will not be added to
   *     the durable subscription.
   * @return An unshared durable subscription on the specified topic.
   * @throws InvalidDestinationException if an invalid topic is specified.
   * @throws InvalidSelectorException if the message selector is invalid.
   * @throws IllegalStateException if the client identifier is unset
   * @throws JMSException
   *     <ul>
   *       <li>if the session fails to create the unshared durable subscription and {@code
   *           MessageConsumer} due to some internal error
   *       <li>if an unshared durable subscription already exists with the same name and client
   *           identifier, and there is a consumer already active
   *       <li>if a shared durable subscription already exists with the same name and client
   *           identifier
   *     </ul>
   *
   * @since JMS 2.0
   */
  @Override
  public PulsarMessageConsumer createDurableConsumer(
      Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
    return createDurableSubscriber(topic, name, messageSelector, noLocal, false);
  }

  /**
   * Creates a shared durable subscription on the specified topic (if one does not already exist),
   * specifying a message selector and the {@code noLocal} parameter, and creates a consumer on that
   * durable subscription. This method creates the durable subscription without a message selector.
   *
   * <p>A durable subscription is used by an application which needs to receive all the messages
   * published on a topic, including the ones published when there is no active consumer associated
   * with it. The JMS provider retains a record of this durable subscription and ensures that all
   * messages from the topic's publishers are retained until they are delivered to, and acknowledged
   * by, a consumer on this durable subscription or until they have expired.
   *
   * <p>A durable subscription will continue to accumulate messages until it is deleted using the
   * {@code unsubscribe} method.
   *
   * <p>This method may only be used with shared durable subscriptions. Any durable subscription
   * created using this method will be shared. This means that multiple active (i.e. not closed)
   * consumers on the subscription may exist at the same time. The term "consumer" here means a
   * {@code MessageConsumer} or {@code JMSConsumer} object in any client.
   *
   * <p>A shared durable subscription is identified by a name specified by the client and by the
   * client identifier (which may be unset). An application which subsequently wishes to create a
   * consumer on that shared durable subscription must use the same client identifier.
   *
   * <p>If a shared durable subscription already exists with the same name and client identifier (if
   * set), and the same topic and message selector has been specified, then this method creates a
   * {@code MessageConsumer} on the existing shared durable subscription.
   *
   * <p>If a shared durable subscription already exists with the same name and client identifier (if
   * set) but a different topic or message selector has been specified, and there is no consumer
   * already active (i.e. not closed) on the durable subscription then this is equivalent to
   * unsubscribing (deleting) the old one and creating a new one.
   *
   * <p>If a shared durable subscription already exists with the same name and client identifier (if
   * set) but a different topic or message selector has been specified, and there is a consumer
   * already active (i.e. not closed) on the durable subscription, then a {@code JMSException} will
   * be thrown.
   *
   * <p>A shared durable subscription and an unshared durable subscription may not have the same
   * name and client identifier (if set). If an unshared durable subscription already exists with
   * the same name and client identifier (if set) then a {@code JMSException} is thrown.
   *
   * <p>There is no restriction on durable subscriptions and shared non-durable subscriptions having
   * the same name and clientId (which may be unset). Such subscriptions would be completely
   * separate.
   *
   * @param topic the non-temporary {@code Topic} to subscribe to
   * @param name the name used to identify this subscription
   * @return A shared durable subscription on the specified topic.
   * @throws JMSException
   *     <ul>
   *       <li>if the session fails to create the shared durable subscription and {@code
   *           MessageConsumer} due to some internal error
   *       <li>if a shared durable subscription already exists with the same name and client
   *           identifier, but a different topic or message selector, and there is a consumer
   *           already active
   *       <li>if an unshared durable subscription already exists with the same name and client
   *           identifier
   *     </ul>
   *
   * @throws InvalidDestinationException if an invalid topic is specified.
   * @since JMS 2.0
   */
  @Override
  public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
    return createSharedDurableConsumer(topic, name, null);
  }

  /**
   * Creates a shared durable subscription on the specified topic (if one does not already exist),
   * specifying a message selector, and creates a consumer on that durable subscription.
   *
   * <p>A durable subscription is used by an application which needs to receive all the messages
   * published on a topic, including the ones published when there is no active consumer associated
   * with it. The JMS provider retains a record of this durable subscription and ensures that all
   * messages from the topic's publishers are retained until they are delivered to, and acknowledged
   * by, a consumer on this durable subscription or until they have expired.
   *
   * <p>A durable subscription will continue to accumulate messages until it is deleted using the
   * {@code unsubscribe} method.
   *
   * <p>This method may only be used with shared durable subscriptions. Any durable subscription
   * created using this method will be shared. This means that multiple active (i.e. not closed)
   * consumers on the subscription may exist at the same time. The term "consumer" here means a
   * {@code MessageConsumer} or {@code JMSConsumer} object in any client.
   *
   * <p>A shared durable subscription is identified by a name specified by the client and by the
   * client identifier (which may be unset). An application which subsequently wishes to create a
   * consumer on that shared durable subscription must use the same client identifier.
   *
   * <p>If a shared durable subscription already exists with the same name and client identifier (if
   * set), and the same topic and message selector has been specified, then this method creates a
   * {@code MessageConsumer} on the existing shared durable subscription.
   *
   * <p>If a shared durable subscription already exists with the same name and client identifier (if
   * set) but a different topic or message selector has been specified, and there is no consumer
   * already active (i.e. not closed) on the durable subscription then this is equivalent to
   * unsubscribing (deleting) the old one and creating a new one.
   *
   * <p>If a shared durable subscription already exists with the same name and client identifier (if
   * set) but a different topic or message selector has been specified, and there is a consumer
   * already active (i.e. not closed) on the durable subscription, then a {@code JMSException} will
   * be thrown.
   *
   * <p>A shared durable subscription and an unshared durable subscription may not have the same
   * name and client identifier (if set). If an unshared durable subscription already exists with
   * the same name and client identifier (if set) then a {@code JMSException} is thrown.
   *
   * <p>There is no restriction on durable subscriptions and shared non-durable subscriptions having
   * the same name and clientId (which may be unset). Such subscriptions would be completely
   * separate.
   *
   * @param topic the non-temporary {@code Topic} to subscribe to
   * @param name the name used to identify this subscription
   * @param messageSelector only messages with properties matching the message selector expression
   *     are added to the durable subscription. A value of null or an empty string indicates that
   *     there is no message selector for the durable subscription.
   * @return A shared durable subscription on the specified topic.
   * @throws JMSException
   *     <ul>
   *       <li>if the session fails to create the shared durable subscription and {@code
   *           MessageConsumer} due to some internal error
   *       <li>if a shared durable subscription already exists with the same name and client
   *           identifier, but a different topic or message selector, and there is a consumer
   *           already active
   *       <li>if an unshared durable subscription already exists with the same name and client
   *           identifier
   *     </ul>
   *
   * @throws InvalidDestinationException if an invalid topic is specified.
   * @throws InvalidSelectorException if the message selector is invalid.
   * @since JMS 2.0
   */
  @Override
  public PulsarMessageConsumer createSharedDurableConsumer(
      Topic topic, String name, String messageSelector) throws JMSException {
    if (topic == null) {
      throw new InvalidDestinationException("null destination");
    }
    checkTopicOperationEnabled();
    PulsarTopic pulsarTopic = (PulsarTopic) PulsarConnectionFactory.toPulsarDestination(topic);
    name = connection.prependClientId(name, true);
    registerSubscriptionName(pulsarTopic, name, true);
    return new PulsarMessageConsumer(
            name,
            pulsarTopic,
            this,
            SubscriptionMode.Durable,
            getFactory().getTopicSharedSubscriptionType(),
            messageSelector,
            true,
            false)
        .subscribe();
  }

  /**
   * Creates a {@code QueueBrowser} object to peek at the messages on the specified queue.
   *
   * @param queue the {@code queue} to access
   * @return A {@code QueueBrowser} object to peek at the messages on the specified queue.
   * @throws JMSException if the session fails to create a browser due to some internal error.
   * @throws InvalidDestinationException if an invalid destination is specified
   * @since JMS 1.1
   */
  @Override
  public QueueBrowser createBrowser(Queue queue) throws JMSException {
    return createBrowser(queue, null);
  }

  /**
   * Creates a {@code QueueBrowser} object to peek at the messages on the specified queue using a
   * message selector.
   *
   * @param queue the {@code queue} to access
   * @param messageSelector only messages with properties matching the message selector expression
   *     are delivered. A value of null or an empty string indicates that there is no message
   *     selector for the message consumer.
   * @return A {@code QueueBrowser} object to peek at the messages on the specified queue.
   * @throws JMSException if the session fails to create a browser due to some internal error.
   * @throws InvalidDestinationException if an invalid destination is specified
   * @throws InvalidSelectorException if the message selector is invalid.
   * @since JMS 1.1
   */
  @Override
  public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
    if (queue == null) {
      throw new InvalidDestinationException("invalid null queue");
    }
    checkQueueOperationEnabled();
    PulsarQueueBrowser res = new PulsarQueueBrowser(this, queue, messageSelector);
    browsers.add(res);
    return res;
  }

  /**
   * Creates a {@code TemporaryQueue} object. Its lifetime will be that of the {@code Connection}
   * unless it is deleted earlier.
   *
   * @return a temporary queue identity
   * @throws JMSException if the session fails to create a temporary queue due to some internal
   *     error.
   * @since JMS 1.1
   */
  @Override
  public TemporaryQueue createTemporaryQueue() throws JMSException {
    checkNotClosed();
    checkQueueOperationEnabled();
    return connection.createTemporaryQueue(this);
  }

  /**
   * Creates a {@code TemporaryTopic} object. Its lifetime will be that of the {@code Connection}
   * unless it is deleted earlier.
   *
   * @return a temporary topic identity
   * @throws JMSException if the session fails to create a temporary topic due to some internal
   *     error.
   * @since JMS 1.1
   */
  @Override
  public TemporaryTopic createTemporaryTopic() throws JMSException {
    checkNotClosed();
    checkTopicOperationEnabled();
    return connection.createTemporaryTopic(this);
  }

  /**
   * Unsubscribes a durable subscription that has been created by a client.
   *
   * <p>This method deletes the state being maintained on behalf of the subscriber by its provider.
   *
   * <p>A durable subscription is identified by a name specified by the client and by the client
   * identifier if set. If the client identifier was set when the durable subscription was created
   * then a client which subsequently wishes to use this method to delete a durable subscription
   * must use the same client identifier.
   *
   * <p>It is erroneous for a client to delete a durable subscription while there is an active (not
   * closed) consumer for the subscription, or while a consumed message is part of a pending
   * transaction or has not been acknowledged in the session.
   *
   * @param name the name used to identify this subscription
   * @throws JMSException if the session fails to unsubscribe to the durable subscription due to
   *     some internal error.
   * @throws InvalidDestinationException if an invalid subscription name is specified.
   * @since JMS 1.1
   */
  @Override
  public void unsubscribe(String name) throws JMSException {
    checkNotClosed();
    checkTopicOperationEnabled();
    name = connection.prependClientId(name, true);
    PulsarDestination destination = destinationBySubscription.remove(name);
    if (destination == null) {
      log.error(
          "Cannot unsubscribe {}, please open and close the subscription within this session before unsubscribing, because in Pulsar you need to known the Destination for the subscription. Known subscription names {}",
          name,
          destinationBySubscription);
    }

    boolean someThingDone = getFactory().deleteSubscription(destination, name);
    if (!someThingDone) {
      throw new InvalidDestinationException("Subscription " + name + " not found");
    }
  }

  /**
   * Used by JMSContext.
   *
   * @throws JMSException
   */
  void acknowledgeAllMessages() throws JMSException {
    checkNotClosed();
    for (PulsarMessage unackedMessage : new ArrayList<>(unackedMessages)) {
      unackedMessage.acknowledgeInternal();
    }
    unackedMessages.clear();
  }

  public void registerUnacknowledgedMessage(PulsarMessage result) {
    unackedMessages.add(result);
  }

  public void unregisterUnacknowledgedMessage(PulsarMessage result) {
    unackedMessages.remove(result);
  }

  public void removeConsumer(PulsarMessageConsumer consumer) {
    Consumer<?> pulsarConsumer = consumer.getInternalConsumer();
    if (pulsarConsumer != null) {
      consumers.remove(consumer);
      getFactory().removeConsumer(pulsarConsumer);
      for (Iterator<PulsarMessage> it = unackedMessages.iterator(); it.hasNext(); ) {
        PulsarMessage message = it.next();
        if (message != null && message.isReceivedFromConsumer(consumer)) {
          it.remove();
        }
      }
    }
    if (consumer.unregisterSubscriptionOnClose) {
      unregisterSubscriptionName(consumer.subscriptionName, (Topic) consumer.getDestination());
    }
  }

  public void onError(Throwable err) {
    log.error("Internal error ", err);
  }

  public void registerConsumer(PulsarMessageConsumer consumer) {
    consumers.add(consumer);
    connection.setAllowSetClientId(false);
  }

  public boolean isJms20() {
    return jms20;
  }

  public void setJms20(boolean jms20) {
    this.jms20 = jms20;
  }

  public PulsarConnection getConnection() {
    return connection;
  }

  void removeBrowser(PulsarQueueBrowser pulsarQueueBrowser) {
    browsers.remove(pulsarQueueBrowser);
  }

  public boolean isTransactionStarted() {
    return transaction != null;
  }

  interface BlockCLoseOperation<T> {
    T execute() throws JMSException;
  }

  /**
   * This mechanisms allows the close() method to follow the specs and wait for any operation to
   * complete before closing the session. Also it implements the Connection#stop behaviour
   *
   * @param operation any code
   * @param <T>
   * @return the result of the execution of the code
   * @throws JMSException
   */
  <T> T executeOperationIfConnectionStarted(BlockCLoseOperation<T> operation, int timeoutMillis)
      throws JMSException {
    // if the connection is "paused" we are not executing the operation
    // and we return null
    // executeInConnectionPausedLock also prevents any ongoing "stop()" operation
    // to complete if we entered the execution of the given operation
    checkNotClosed();
    return connection.executeInConnectionPausedLock(operation::execute, timeoutMillis);
  }

  <T> T executeCriticalOperation(BlockCLoseOperation<T> operation) throws JMSException {
    checkNotClosed();
    // if the connection is "paused" we are not executing the operation
    // and we return null
    // executeInConnectionPausedLock also prevents any ongoing "stop()" operation
    // to complete if we entered the execution of the given operation

    // the execution of the operation will block any ongoing "close()" operation
    closeLock.readLock().lock();
    try {
      // check again inside the lock
      checkNotClosed();
      return operation.execute();
    } finally {
      closeLock.readLock().unlock();
    }
  }

  @Override
  public QueueReceiver createReceiver(Queue queue) throws JMSException {
    return createConsumer(queue);
  }

  @Override
  public QueueReceiver createReceiver(Queue queue, String s) throws JMSException {
    return createConsumer(queue, s);
  }

  @Override
  public QueueSender createSender(Queue queue) throws JMSException {
    return createProducer(queue);
  }

  @Override
  public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
    return createConsumer(topic);
  }

  @Override
  public TopicSubscriber createSubscriber(Topic topic, String s, boolean b) throws JMSException {
    return createConsumer(topic, s, b);
  }

  @Override
  public TopicPublisher createPublisher(Topic topic) throws JMSException {
    return createProducer(topic);
  }

  public void checkNotClosed() throws JMSException {
    if (closed) {
      throw new IllegalStateException("Session is closed");
    }
    connection.checkNotClosed();
  }

  public boolean isClosed() {
    closeLock.readLock().lock();
    try {
      return closed;
    } finally {
      closeLock.readLock().unlock();
    }
  }

  private class ListenerThread extends Thread {
    private ListenerThread() {
      super("jms-session-thread");
      setDaemon(true);
    }

    @Override
    public void run() {
      while (!closed) {
        PulsarSession.this.run();
      }
    }
  }

  void ensureListenerThread() {
    if (listenerThread == null) {
      listenerThread = new ListenerThread();
      listenerThread.start();
    }
  }

  void checkQueueOperationEnabled() throws JMSException {
    if (!allowQueueOperations) {
      throw new IllegalStateException("This is not a QueueSession");
    }
  }

  void checkTopicOperationEnabled() throws JMSException {
    if (!allowTopicOperations) {
      throw new IllegalStateException("This is not a TopicSession");
    }
  }

  PulsarSession emulateLegacySession(boolean queue, boolean topic) {
    this.allowQueueOperations = queue;
    this.allowTopicOperations = topic;
    return this;
  }
}
