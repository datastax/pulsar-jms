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
import jakarta.jms.BytesMessage;
import jakarta.jms.CompletionListener;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageEOFException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.QueueSender;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TopicPublisher;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
class PulsarMessageProducer implements MessageProducer, TopicPublisher, QueueSender {
  protected final PulsarSession session;
  protected final PulsarDestination defaultDestination;
  private final boolean jms20;
  private final String id;
  // only for "emulated transactions"
  private List<PreparedMessage> uncommittedMessages;

  public PulsarMessageProducer(PulsarSession session, Destination defaultDestination)
      throws JMSException {
    this.jms20 = session.isJms20();
    session.checkNotClosed();
    this.session = session;
    this.id = UUID.randomUUID().toString();
    try {
      this.defaultDestination = (PulsarDestination) defaultDestination;
    } catch (ClassCastException err) {
      throw new InvalidDestinationException(
          "Invalid destination type " + defaultDestination.getClass());
    }
  }

  protected boolean closed;
  private boolean disableMessageId;
  private boolean disableMessageTimestamp;
  private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
  private int priority = Message.DEFAULT_PRIORITY;
  private long defaultTimeToLive = Message.DEFAULT_TIME_TO_LIVE;
  private long defaultDeliveryDelay = Message.DEFAULT_DELIVERY_DELAY;

  /**
   * Specify whether message IDs may be disabled.
   *
   * <p>Since message IDs take some effort to create and increase a message's size, some JMS
   * providers may be able to optimise message overhead if they are given a hint that the message ID
   * is not used by an application. By calling this method, a JMS application enables this potential
   * optimisation for all messages sent using this {@code MessageProducer}. If the JMS provider
   * accepts this hint, these messages must have the message ID set to null; if the provider ignores
   * the hint, the message ID must be set to its normal unique value.
   *
   * <p>Message IDs are enabled by default.
   *
   * @param value indicates if message IDs may be disabled
   * @throws JMSException if the JMS provider fails to set message ID to disabled due to some
   *     internal error.
   */
  @Override
  public void setDisableMessageID(boolean value) throws JMSException {
    checkNotClosed();
    this.disableMessageId = value;
  }

  /**
   * Gets an indication of whether message IDs are disabled.
   *
   * @return an indication of whether message IDs are disabled
   * @throws JMSException if the JMS provider fails to determine if message IDs are disabled due to
   *     some internal error.
   */
  @Override
  public boolean getDisableMessageID() throws JMSException {
    checkNotClosed();
    return disableMessageId;
  }

  /**
   * Specify whether message timestamps may be disabled.
   *
   * <p>Since timestamps take some effort to create and increase a message's size, some JMS
   * providers may be able to optimise message overhead if they are given a hint that the timestamp
   * is not used by an application. By calling this method, a JMS application enables this potential
   * optimisation for all messages sent using this {@code MessageProducer}. If the JMS provider
   * accepts this hint, these messages must have the timestamp set to zero; if the provider ignores
   * the hint, the timestamp must be set to its normal value.
   *
   * <p>Message timestamps are enabled by default.
   *
   * @param value indicates whether message timestamps may be disabled
   * @throws JMSException if the JMS provider fails to set timestamps to disabled due to some
   *     internal error.
   */
  @Override
  public void setDisableMessageTimestamp(boolean value) throws JMSException {
    checkNotClosed();
    this.disableMessageTimestamp = value;
  }

  /**
   * Gets an indication of whether message timestamps are disabled.
   *
   * @return an indication of whether message timestamps are disabled
   * @throws JMSException if the JMS provider fails to determine if timestamps are disabled due to
   *     some internal error.
   */
  @Override
  public boolean getDisableMessageTimestamp() throws JMSException {
    checkNotClosed();
    return disableMessageTimestamp;
  }

  /**
   * Sets the producer's default delivery mode.
   *
   * <p>Delivery mode is set to {@code PERSISTENT} by default.
   *
   * @param deliveryMode the message delivery mode for this message producer; legal values are
   *     {@code DeliveryMode.NON_PERSISTENT} and {@code DeliveryMode.PERSISTENT}
   * @throws JMSException if the JMS provider fails to set the delivery mode due to some internal
   *     error.
   * @see MessageProducer#getDeliveryMode
   * @see DeliveryMode#NON_PERSISTENT
   * @see DeliveryMode#PERSISTENT
   * @see Message#DEFAULT_DELIVERY_MODE
   */
  @Override
  public void setDeliveryMode(int deliveryMode) throws JMSException {
    checkNotClosed();
    validateDeliveryMode(deliveryMode);
    this.deliveryMode = deliveryMode;
  }

  private static void validateDeliveryMode(int deliveryMode) throws JMSException {
    switch (deliveryMode) {
      case DeliveryMode.NON_PERSISTENT:
      case DeliveryMode.PERSISTENT:
        break;
      default:
        throw new JMSException("Invalid deliveryMode " + deliveryMode);
    }
  }

  /**
   * Gets the producer's default delivery mode.
   *
   * @return the message delivery mode for this message producer
   * @throws JMSException if the JMS provider fails to get the delivery mode due to some internal
   *     error.
   * @see MessageProducer#setDeliveryMode
   */
  @Override
  public int getDeliveryMode() throws JMSException {
    checkNotClosed();
    return deliveryMode;
  }

  private void checkNotClosed() throws JMSException {
    session.checkNotClosed();
    if (closed) {
      throw new IllegalStateException("this producer is closed");
    }
  }

  String getId() {
    return id;
  }

  /**
   * Sets the producer's default priority.
   *
   * <p>The JMS API defines ten levels of priority value, with 0 as the lowest priority and 9 as the
   * highest. Clients should consider priorities 0-4 as gradations of normal priority and priorities
   * 5-9 as gradations of expedited priority. Priority is set to 4 by default.
   *
   * @param defaultPriority the message priority for this message producer; must be a value between
   *     0 and 9
   * @throws JMSException if the JMS provider fails to set the priority due to some internal error.
   * @see MessageProducer#getPriority
   * @see Message#DEFAULT_PRIORITY
   */
  @Override
  public void setPriority(int defaultPriority) throws JMSException {
    checkNotClosed();
    validatePriority(defaultPriority);
    this.priority = defaultPriority;
  }

  private void validatePriority(int defaultPriority) throws JMSException {
    if (defaultPriority < 0 || defaultPriority > 10) {
      throw new JMSException("invalid priority " + defaultPriority);
    }
  }

  /**
   * Gets the producer's default priority.
   *
   * @return the message priority for this message producer
   * @throws JMSException if the JMS provider fails to get the priority due to some internal error.
   * @see MessageProducer#setPriority
   */
  @Override
  public int getPriority() throws JMSException {
    checkNotClosed();
    return priority;
  }

  /**
   * Sets the default length of time in milliseconds from its dispatch time that a produced message
   * should be retained by the message system.
   *
   * <p>Time to live is set to zero by default.
   *
   * @param timeToLive the message time to live in milliseconds; zero is unlimited
   * @throws JMSException if the JMS provider fails to set the time to live due to some internal
   *     error.
   * @see MessageProducer#getTimeToLive
   * @see Message#DEFAULT_TIME_TO_LIVE
   */
  @Override
  public void setTimeToLive(long timeToLive) throws JMSException {
    checkNotClosed();
    this.defaultTimeToLive = timeToLive;
  }

  /**
   * Gets the default length of time in milliseconds from its dispatch time that a produced message
   * should be retained by the message system.
   *
   * @return the message time to live in milliseconds; zero is unlimited
   * @throws JMSException if the JMS provider fails to get the time to live due to some internal
   *     error.
   * @see MessageProducer#setTimeToLive
   */
  @Override
  public long getTimeToLive() throws JMSException {
    checkNotClosed();
    return defaultTimeToLive;
  }

  /**
   * Sets the minimum length of time in milliseconds that must elapse after a message is sent before
   * the JMS provider may deliver the message to a consumer.
   *
   * <p>For transacted sends, this time starts when the client sends the message, not when the
   * transaction is committed.
   *
   * <p>deliveryDelay is set to zero by default.
   *
   * @param deliveryDelay the delivery delay in milliseconds.
   * @throws JMSException if the JMS provider fails to set the delivery delay due to some internal
   *     error.
   * @see MessageProducer#getDeliveryDelay
   * @see Message#DEFAULT_DELIVERY_DELAY
   * @since JMS 2.0
   */
  @Override
  public void setDeliveryDelay(long deliveryDelay) throws JMSException {
    checkNotClosed();
    this.defaultDeliveryDelay = deliveryDelay;
  }

  /**
   * Gets the minimum length of time in milliseconds that must elapse after a message is sent before
   * the JMS provider may deliver the message to a consumer.
   *
   * @return the delivery delay in milliseconds.
   * @throws JMSException if the JMS provider fails to get the delivery delay due to some internal
   *     error.
   * @see MessageProducer#setDeliveryDelay
   * @since JMS 2.0
   */
  @Override
  public long getDeliveryDelay() throws JMSException {
    checkNotClosed();
    return defaultDeliveryDelay;
  }

  /**
   * Gets the destination associated with this {@code MessageProducer}.
   *
   * @return this producer's {@code Destination}
   * @throws JMSException if the JMS provider fails to get the destination for this {@code
   *     MessageProducer} due to some internal error.
   * @since JMS 1.1
   */
  @Override
  public Destination getDestination() throws JMSException {
    checkNotClosed();
    return defaultDestination;
  }

  /**
   * Closes the message producer.
   *
   * <p>Since a provider may allocate some resources on behalf of a {@code MessageProducer} outside
   * the Java virtual machine, clients should close them when they are not needed. Relying on
   * garbage collection to eventually reclaim these resources may not be timely enough.
   *
   * <p>This method must not return until any incomplete asynchronous send operations for this
   * <code>MessageProducer</code> have been completed and any <code>CompletionListener</code>
   * callbacks have returned. Incomplete sends should be allowed to complete normally unless an
   * error occurs.
   *
   * <p>A <code>CompletionListener</code> callback method must not call <code>close</code> on its
   * own <code>MessageProducer</code>. Doing so will cause an <code>IllegalStateException</code> to
   * be thrown.
   *
   * @throws IllegalStateException this method has been called by a <code>CompletionListener</code>
   *     callback method on its own <code>MessageProducer</code>
   * @throws JMSException if the JMS provider fails to close the producer due to some internal
   *     error.
   */
  @Override
  public void close() throws JMSException {
    Utils.checkNotOnMessageProducer(session, this);
    closed = true;
  }

  /**
   * Sends a message using the {@code MessageProducer}'s default delivery mode, priority, and time
   * to live.
   *
   * @param message the message to send
   * @throws JMSException if the JMS provider fails to send the message due to some internal error.
   * @throws MessageFormatException if an invalid message is specified.
   * @throws InvalidDestinationException if a client uses this method with a {@code MessageProducer}
   *     with an invalid destination.
   * @throws UnsupportedOperationException if a client uses this method with a {@code
   *     MessageProducer} that did not specify a destination at creation time.
   * @see Session#createProducer
   * @since JMS 1.1
   */
  @Override
  public void send(Message message) throws JMSException {
    message.setJMSDeliveryMode(deliveryMode);
    message.setJMSPriority(priority);
    validateMessageSend(
        message, defaultDestination, true, Message.DEFAULT_TIME_TO_LIVE, deliveryMode, priority);
    sendMessage(defaultDestination, message);
  }

  /**
   * Sends a message, specifying delivery mode, priority, and time to live.
   *
   * @param message the message to send
   * @param deliveryMode the delivery mode to use
   * @param priority the priority for this message
   * @param timeToLive the message's lifetime (in milliseconds)
   * @throws JMSException if the JMS provider fails to send the message due to some internal error.
   * @throws MessageFormatException if an invalid message is specified.
   * @throws InvalidDestinationException if a client uses this method with a {@code MessageProducer}
   *     with an invalid destination.
   * @throws UnsupportedOperationException if a client uses this method with a {@code
   *     MessageProducer} that did not specify a destination at creation time.
   * @see Session#createProducer
   * @since JMS 1.1
   */
  @Override
  public void send(Message message, int deliveryMode, int priority, long timeToLive)
      throws JMSException {
    validateMessageSend(message, defaultDestination, true, timeToLive, deliveryMode, priority);
    message.setJMSDeliveryMode(deliveryMode);
    message.setJMSPriority(priority);
    applyTimeToLive(message, timeToLive);
    sendMessage(defaultDestination, message);
  }

  /**
   * Sends a message to a destination for an unidentified message producer using the {@code
   * MessageProducer}'s default delivery mode, priority, and time to live.
   *
   * <p>Typically, a message producer is assigned a destination at creation time; however, the JMS
   * API also supports unidentified message producers, which require that the destination be
   * supplied every time a message is sent.
   *
   * @param destination the destination to send this message to
   * @param message the message to send
   * @throws JMSException if the JMS provider fails to send the message due to some internal error.
   * @throws MessageFormatException if an invalid message is specified.
   * @throws InvalidDestinationException if a client uses this method with an invalid destination.
   * @throws UnsupportedOperationException if a client uses this method with a {@code
   *     MessageProducer} that specified a destination at creation time.
   * @see Session#createProducer
   * @since JMS 1.1
   */
  @Override
  public void send(Destination destination, Message message) throws JMSException {
    checkNoDefaultDestinationSet();
    validateMessageSend(
        message, destination, false, Message.DEFAULT_TIME_TO_LIVE, deliveryMode, priority);
    message.setJMSDeliveryMode(deliveryMode);
    message.setJMSPriority(priority);
    sendMessage(destination, message);
  }

  /**
   * Sends a message to a destination for an unidentified message producer, specifying delivery
   * mode, priority and time to live.
   *
   * <p>Typically, a message producer is assigned a destination at creation time; however, the JMS
   * API also supports unidentified message producers, which require that the destination be
   * supplied every time a message is sent.
   *
   * @param destination the destination to send this message to
   * @param message the message to send
   * @param deliveryMode the delivery mode to use
   * @param priority the priority for this message
   * @param timeToLive the message's lifetime (in milliseconds)
   * @throws JMSException if the JMS provider fails to send the message due to some internal error.
   * @throws MessageFormatException if an invalid message is specified.
   * @throws InvalidDestinationException if a client uses this method with an invalid destination.
   * @throws UnsupportedOperationException if a client uses this method with a {@code
   *     MessageProducer} that specified a destination at creation time.
   * @see Session#createProducer
   * @since JMS 1.1
   */
  @Override
  public void send(
      Destination destination, Message message, int deliveryMode, int priority, long timeToLive)
      throws JMSException {
    checkNoDefaultDestinationSet();
    validateMessageSend(message, destination, false, timeToLive, deliveryMode, priority);
    message.setJMSDeliveryMode(deliveryMode);
    message.setJMSPriority(priority);
    applyTimeToLive(message, timeToLive);
    sendMessage(destination, message);
  }

  private void checkNoDefaultDestinationSet() {
    if (defaultDestination != null) {
      throw new UnsupportedOperationException(
          "you cannot use this producer with another destination");
    }
  }

  private void validateMessageSend(
      Message message,
      Destination destination,
      boolean isDefaultDestination,
      long timeToLive,
      int deliveryMode,
      int priority)
      throws JMSException {
    checkNotClosed();
    if (message == null) {
      throw new MessageFormatException("Invalid null message");
    }
    if (deliveryMode != DeliveryMode.PERSISTENT && deliveryMode != DeliveryMode.NON_PERSISTENT) {
      throw new JMSException("Invalid deliveryMode " + deliveryMode);
    }
    validatePriority(priority);
    if (destination == null) {
      if (isDefaultDestination) {
        throw new UnsupportedOperationException("please set a destination");
      } else {
        throw new InvalidDestinationException("destination is null");
      }
    }
  }

  /**
   * Sends a message using the {@code MessageProducer}'s default delivery mode, priority, and time
   * to live, performing part of the work involved in sending the message in a separate thread and
   * notifying the specified <code>CompletionListener</code> when the operation has completed. JMS
   * refers to this as an "asynchronous send".
   *
   * <p>When the message has been successfully sent the JMS provider invokes the callback method
   * <code>onCompletion</code> on an application-specified <code>CompletionListener</code> object.
   * Only when that callback has been invoked can the application be sure that the message has been
   * successfully sent with the same degree of confidence as if a normal synchronous send had been
   * performed. An application which requires this degree of confidence must therefore wait for the
   * callback to be invoked before continuing.
   *
   * <p>The following information is intended to give an indication of how an asynchronous send
   * would typically be implemented.
   *
   * <p>In some JMS providers, a normal synchronous send involves sending the message to a remote
   * JMS server and then waiting for an acknowledgement to be received before returning. It is
   * expected that such a provider would implement an asynchronous send by sending the message to
   * the remote JMS server and then returning without waiting for an acknowledgement. When the
   * acknowledgement is received, the JMS provider would notify the application by invoking the
   * <code>onCompletion</code> method on the application-specified <code>CompletionListener</code>
   * object. If for some reason the acknowledgement is not received the JMS provider would notify
   * the application by invoking the <code>CompletionListener</code>'s <code>onException</code>
   * method.
   *
   * <p>In those cases where the JMS specification permits a lower level of reliability, a normal
   * synchronous send might not wait for an acknowledgement. In that case it is expected that an
   * asynchronous send would be similar to a synchronous send: the JMS provider would send the
   * message to the remote JMS server and then return without waiting for an acknowledgement.
   * However the JMS provider would still notify the application that the send had completed by
   * invoking the <code>onCompletion</code> method on the application-specified <code>
   * CompletionListener</code> object.
   *
   * <p>It is up to the JMS provider to decide exactly what is performed in the calling thread and
   * what, if anything, is performed asynchronously, so long as it satisfies the requirements given
   * below:
   *
   * <p><b>Quality of service</b>: After the send operation has completed successfully, which means
   * that the message has been successfully sent with the same degree of confidence as if a normal
   * synchronous send had been performed, the JMS provider must invoke the <code>CompletionListener
   * </code>'s <code>onCompletion</code> method. The <code>CompletionListener</code> must not be
   * invoked earlier than this.
   *
   * <p><b>Exceptions</b>: If an exception is encountered during the call to the <code>send</code>
   * method then an appropriate exception should be thrown in the thread that is calling the <code>
   * send</code> method. In this case the JMS provider must not invoke the <code>CompletionListener
   * </code>'s <code>onCompletion</code> or <code>onException</code> method. If an exception is
   * encountered which cannot be thrown in the thread that is calling the <code>send</code> method
   * then the JMS provider must call the <code>CompletionListener</code>'s <code>onException</code>
   * method. In both cases if an exception occurs it is undefined whether or not the message was
   * successfully sent.
   *
   * <p><b>Message order</b>: If the same <code>MessageProducer</code> is used to send multiple
   * messages then JMS message ordering requirements must be satisfied. This applies even if a
   * combination of synchronous and asynchronous sends has been performed. The application is not
   * required to wait for an asynchronous send to complete before sending the next message.
   *
   * <p><b>Close, commit or rollback</b>: If the <code>close</code> method is called on the <code>
   * MessageProducer</code> or its <code>Session</code> or <code>Connection</code> then the JMS
   * provider must block until any incomplete send operations have been completed and all {@code
   * CompletionListener} callbacks have returned before closing the object and returning. If the
   * session is transacted (uses a local transaction) then when the <code>Session</code>'s <code>
   * commit</code> or <code>rollback</code> method is called the JMS provider must block until any
   * incomplete send operations have been completed and all {@code CompletionListener} callbacks
   * have returned before performing the commit or rollback. Incomplete sends should be allowed to
   * complete normally unless an error occurs.
   *
   * <p>A <code>CompletionListener</code> callback method must not call <code>close</code> on its
   * own <code>Connection</code>, <code>Session</code> or <code>MessageProducer</code> or call
   * <code>commit</code> or <code>rollback</code> on its own <code>Session</code>. Doing so will
   * cause the <code>close</code>, <code>commit</code> or <code>rollback</code> to throw an <code>
   * IllegalStateException</code>.
   *
   * <p><b>Restrictions on usage in Java EE</b> This method must not be used in a Java EE EJB or web
   * container. Doing so may cause a {@code JMSException} to be thrown though this is not
   * guaranteed.
   *
   * <p><b>Message headers</b> JMS defines a number of message header fields and message properties
   * which must be set by the "JMS provider on send". If the send is asynchronous these fields and
   * properties may be accessed on the sending client only after the <code>CompletionListener</code>
   * has been invoked. If the <code>CompletionListener</code>'s <code>onException</code> method is
   * called then the state of these message header fields and properties is undefined.
   *
   * <p><b>Restrictions on threading</b>: Applications that perform an asynchronous send must
   * confirm to the threading restrictions defined in JMS. This means that the session may be used
   * by only one thread at a time.
   *
   * <p>Setting a <code>CompletionListener</code> does not cause the session to be dedicated to the
   * thread of control which calls the <code>CompletionListener</code>. The application thread may
   * therefore continue to use the session after performing an asynchronous send. However the <code>
   * CompletionListener</code>'s callback methods must not use the session if an application thread
   * might be using the session at the same time.
   *
   * <p><b>Use of the <code>CompletionListener</code> by the JMS provider</b>: A session will only
   * invoke one <code>CompletionListener</code> callback method at a time. For a given <code>
   * MessageProducer</code>, callbacks (both {@code onCompletion} and {@code onException}) will be
   * performed in the same order as the corresponding calls to the asynchronous send method. A JMS
   * provider must not invoke the <code>CompletionListener</code> from the thread that is calling
   * the asynchronous <code>send</code> method.
   *
   * <p><b>Restrictions on the use of the Message object</b>: Applications which perform an
   * asynchronous send must take account of the restriction that a <code>Message</code> object is
   * designed to be accessed by one logical thread of control at a time and does not support
   * concurrent use.
   *
   * <p>After the <code>send</code> method has returned, the application must not attempt to read
   * the headers, properties or body of the <code>Message</code> object until the <code>
   * CompletionListener</code>'s <code>onCompletion</code> or <code>onException</code> method has
   * been called. This is because the JMS provider may be modifying the <code>Message</code> object
   * in another thread during this time. The JMS provider may throw an <code>JMSException</code> if
   * the application attempts to access or modify the <code>Message</code> object after the <code>
   * send</code> method has returned and before the <code>CompletionListener</code> has been
   * invoked. If the JMS provider does not throw an exception then the behaviour is undefined.
   *
   * @param message the message to send
   * @param completionListener a {@code CompletionListener} to be notified when the send has
   *     completed
   * @throws JMSException if an internal error occurs
   * @throws MessageFormatException if an invalid message is specified.
   * @throws InvalidDestinationException if a client uses this method with a {@code MessageProducer}
   *     with an invalid destination.
   * @throws IllegalArgumentException if the specified {@code CompletionListener} is null
   * @throws UnsupportedOperationException if a client uses this method with a {@code
   *     MessageProducer} that did not specify a destination at creation time.
   * @see Session#createProducer
   * @see CompletionListener
   * @since JMS 2.0
   */
  @Override
  public void send(Message message, CompletionListener completionListener) throws JMSException {
    message.setJMSDeliveryMode(deliveryMode);
    message.setJMSPriority(priority);
    validateCompletionListener(completionListener);

    if (!jms20) {
      // here the error is to be reported not in the CompletionListener
      validateDeliveryMode(deliveryMode);
    }

    try {
      validateMessageSend(message, defaultDestination, true, 0, deliveryMode, priority);
    } catch (JMSException err) {
      completionListener.onException(message, err);
      return;
    }
    message.setJMSDeliveryMode(deliveryMode);
    message.setJMSPriority(priority);
    sendMessage(defaultDestination, message, completionListener);
  }

  /**
   * Sends a message, specifying delivery mode, priority and time to live, performing part of the
   * work involved in sending the message in a separate thread and notifying the specified <code>
   * CompletionListener</code> when the operation has completed. JMS refers to this as an
   * "asynchronous send".
   *
   * <p>When the message has been successfully sent the JMS provider invokes the callback method
   * <code>onCompletion</code> on an application-specified <code>CompletionListener</code> object.
   * Only when that callback has been invoked can the application be sure that the message has been
   * successfully sent with the same degree of confidence as if a normal synchronous send had been
   * performed. An application which requires this degree of confidence must therefore wait for the
   * callback to be invoked before continuing.
   *
   * <p>The following information is intended to give an indication of how an asynchronous send
   * would typically be implemented.
   *
   * <p>In some JMS providers, a normal synchronous send involves sending the message to a remote
   * JMS server and then waiting for an acknowledgement to be received before returning. It is
   * expected that such a provider would implement an asynchronous send by sending the message to
   * the remote JMS server and then returning without waiting for an acknowledgement. When the
   * acknowledgement is received, the JMS provider would notify the application by invoking the
   * <code>onCompletion</code> method on the application-specified <code>CompletionListener</code>
   * object. If for some reason the acknowledgement is not received the JMS provider would notify
   * the application by invoking the <code>CompletionListener</code>'s <code>onException</code>
   * method.
   *
   * <p>In those cases where the JMS specification permits a lower level of reliability, a normal
   * synchronous send might not wait for an acknowledgement. In that case it is expected that an
   * asynchronous send would be similar to a synchronous send: the JMS provider would send the
   * message to the remote JMS server and then return without waiting for an acknowledgement.
   * However the JMS provider would still notify the application that the send had completed by
   * invoking the <code>onCompletion</code> method on the application-specified <code>
   * CompletionListener</code> object.
   *
   * <p>It is up to the JMS provider to decide exactly what is performed in the calling thread and
   * what, if anything, is performed asynchronously, so long as it satisfies the requirements given
   * below:
   *
   * <p><b>Quality of service</b>: After the send operation has completed successfully, which means
   * that the message has been successfully sent with the same degree of confidence as if a normal
   * synchronous send had been performed, the JMS provider must invoke the <code>CompletionListener
   * </code>'s <code>onCompletion</code> method. The <code>CompletionListener</code> must not be
   * invoked earlier than this.
   *
   * <p><b>Exceptions</b>: If an exception is encountered during the call to the <code>send</code>
   * method then an appropriate exception should be thrown in the thread that is calling the <code>
   * send</code> method. In this case the JMS provider must not invoke the <code>CompletionListener
   * </code>'s <code>onCompletion</code> or <code>onException</code> method. If an exception is
   * encountered which cannot be thrown in the thread that is calling the <code>send</code> method
   * then the JMS provider must call the <code>CompletionListener</code>'s <code>onException</code>
   * method. In both cases if an exception occurs it is undefined whether or not the message was
   * successfully sent.
   *
   * <p><b>Message order</b>: If the same <code>MessageProducer</code> is used to send multiple
   * messages then JMS message ordering requirements must be satisfied. This applies even if a
   * combination of synchronous and asynchronous sends has been performed. The application is not
   * required to wait for an asynchronous send to complete before sending the next message.
   *
   * <p><b>Close, commit or rollback</b>: If the <code>close</code> method is called on the <code>
   * MessageProducer</code> or its <code>Session</code> or <code>Connection</code> then the JMS
   * provider must block until any incomplete send operations have been completed and all {@code
   * CompletionListener} callbacks have returned before closing the object and returning. If the
   * session is transacted (uses a local transaction) then when the <code>Session</code>'s <code>
   * commit</code> or <code>rollback</code> method is called the JMS provider must block until any
   * incomplete send operations have been completed and all {@code CompletionListener} callbacks
   * have returned before performing the commit or rollback. Incomplete sends should be allowed to
   * complete normally unless an error occurs.
   *
   * <p>A <code>CompletionListener</code> callback method must not call <code>close</code> on its
   * own <code>Connection</code>, <code>Session</code> or <code>MessageProducer</code> or call
   * <code>commit</code> or <code>rollback</code> on its own <code>Session</code>. Doing so will
   * cause the <code>close</code>, <code>commit</code> or <code>rollback</code> to throw an <code>
   * IllegalStateException</code>.
   *
   * <p><b>Restrictions on usage in Java EE</b> This method must not be used in a Java EE EJB or web
   * container. Doing so may cause a {@code JMSException} to be thrown though this is not
   * guaranteed.
   *
   * <p><b>Message headers</b> JMS defines a number of message header fields and message properties
   * which must be set by the "JMS provider on send". If the send is asynchronous these fields and
   * properties may be accessed on the sending client only after the <code>CompletionListener</code>
   * has been invoked. If the <code>CompletionListener</code>'s <code>onException</code> method is
   * called then the state of these message header fields and properties is undefined.
   *
   * <p><b>Restrictions on threading</b>: Applications that perform an asynchronous send must
   * confirm to the threading restrictions defined in JMS. This means that the session may be used
   * by only one thread at a time.
   *
   * <p>Setting a <code>CompletionListener</code> does not cause the session to be dedicated to the
   * thread of control which calls the <code>CompletionListener</code>. The application thread may
   * therefore continue to use the session after performing an asynchronous send. However the <code>
   * CompletionListener</code>'s callback methods must not use the session if an application thread
   * might be using the session at the same time.
   *
   * <p><b>Use of the <code>CompletionListener</code> by the JMS provider</b>: A session will only
   * invoke one <code>CompletionListener</code> callback method at a time. For a given <code>
   * MessageProducer</code>, callbacks (both {@code onCompletion} and {@code onException}) will be
   * performed in the same order as the corresponding calls to the asynchronous send method. A JMS
   * provider must not invoke the <code>CompletionListener</code> from the thread that is calling
   * the asynchronous <code>send</code> method.
   *
   * <p><b>Restrictions on the use of the Message object</b>: Applications which perform an
   * asynchronous send must take account of the restriction that a <code>Message</code> object is
   * designed to be accessed by one logical thread of control at a time and does not support
   * concurrent use.
   *
   * <p>After the <code>send</code> method has returned, the application must not attempt to read
   * the headers, properties or body of the <code>Message</code> object until the <code>
   * CompletionListener</code>'s <code>onCompletion</code> or <code>onException</code> method has
   * been called. This is because the JMS provider may be modifying the <code>Message</code> object
   * in another thread during this time. The JMS provider may throw an <code>JMSException</code> if
   * the application attempts to access or modify the <code>Message</code> object after the <code>
   * send</code> method has returned and before the <code>CompletionListener</code> has been
   * invoked. If the JMS provider does not throw an exception then the behaviour is undefined.
   *
   * @param message the message to send
   * @param deliveryMode the delivery mode to use
   * @param priority the priority for this message
   * @param timeToLive the message's lifetime (in milliseconds)
   * @param completionListener a {@code CompletionListener} to be notified when the send has
   *     completed
   * @throws JMSException if an internal error occurs
   * @throws MessageFormatException if an invalid message is specified.
   * @throws InvalidDestinationException if a client uses this method with a {@code MessageProducer}
   *     with an invalid destination.
   * @throws IllegalArgumentException if the specified {@code CompletionListener} is null
   * @throws UnsupportedOperationException if a client uses this method with a {@code
   *     MessageProducer} that did not specify a destination at creation time.
   * @see Session#createProducer
   * @see CompletionListener
   * @since JMS 2.0
   */
  @Override
  public void send(
      Message message,
      int deliveryMode,
      int priority,
      long timeToLive,
      CompletionListener completionListener)
      throws JMSException {
    validateCompletionListener(completionListener);
    if (!jms20) {
      // here the error is to be reported not in the CompletionListener
      validateDeliveryMode(deliveryMode);
      validatePriority(priority);
    }

    try {
      validateMessageSend(message, defaultDestination, true, timeToLive, deliveryMode, priority);
    } catch (JMSException err) {
      completionListener.onException(message, err);
      return;
    }
    message.setJMSDestination(defaultDestination);
    message.setJMSDeliveryMode(deliveryMode);
    message.setJMSPriority(priority);
    applyTimeToLive(message, timeToLive);
    PulsarMessage pulsarMessage = prepareMessageForSend(message);
    sendMessage(defaultDestination, pulsarMessage, completionListener);
  }

  private PulsarMessage prepareMessageForSend(Message message) throws JMSException {
    if (message == null) {
      throw new IllegalArgumentException("Cannot send a null message");
    }
    PulsarMessage res;
    if (!(message instanceof PulsarMessage)) {
      if (message instanceof TextMessage) {
        res = new PulsarTextMessage(((TextMessage) message).getText());
      } else if (message instanceof BytesMessage) {
        BytesMessage sm = (BytesMessage) message;
        sm.reset();
        byte[] buffer = new byte[(int) sm.getBodyLength()];
        sm.readBytes(buffer);
        PulsarBytesMessage dest = new PulsarBytesMessage(buffer);
        res = dest;
      } else if (message instanceof MapMessage) {
        MapMessage sm = (MapMessage) message;
        PulsarMapMessage dest = new PulsarMapMessage();
        for (Enumeration en = sm.getMapNames(); en.hasMoreElements(); ) {
          String name = (String) en.nextElement();
          dest.setObject(name, sm.getObject(name));
        }
        res = dest;
      } else if (message instanceof ObjectMessage) {
        res = new PulsarObjectMessage(((ObjectMessage) message).getObject());
      } else if (message instanceof StreamMessage) {
        StreamMessage sm = (StreamMessage) message;
        sm.reset();
        PulsarStreamMessage dest = new PulsarStreamMessage();
        while (true) {
          try {
            Object object = sm.readObject();
            dest.writeObject(object);
          } catch (MessageEOFException end) {
            break;
          }
        }
        res = dest;
      } else {
        res = new PulsarSimpleMessage();
      }
      res.setWritable(true);
      for (Enumeration en = message.getPropertyNames(); en.hasMoreElements(); ) {
        String name = (String) en.nextElement();
        res.setObjectProperty(name, message.getObjectProperty(name));
      }
      res.setJMSCorrelationIDAsBytes(message.getJMSCorrelationIDAsBytes());
      res.setJMSDeliveryMode(message.getJMSDeliveryMode());
      res.setJMSPriority(message.getJMSPriority());
      res.setJMSDestination(message.getJMSDestination());
      res.setJMSDeliveryTime(message.getJMSDeliveryTime());

      // DO NOT COPY THESE VALUES
      // res.setJMSMessageID(message.getJMSMessageID());
      // res.setJMSTimestamp(message.getJMSTimestamp());
      // res.setJMSExpiration(message.getJMSExpiration());
    } else {
      res = (PulsarMessage) message;
    }
    res.setWritable(true);
    res.setStringProperty("JMSConnectionID", session.getConnection().getConnectionId());
    return res;
  }

  /**
   * Sends a message to a destination for an unidentified message producer, using the {@code
   * MessageProducer}'s default delivery mode, priority, and time to live, performing part of the
   * work involved in sending the message in a separate thread and notifying the specified <code>
   * CompletionListener</code> when the operation has completed. JMS refers to this as an
   * "asynchronous send".
   *
   * <p>Typically, a message producer is assigned a destination at creation time; however, the JMS
   * API also supports unidentified message producers, which require that the destination be
   * supplied every time a message is sent.
   *
   * <p>When the message has been successfully sent the JMS provider invokes the callback method
   * <code>onCompletion</code> on an application-specified <code>CompletionListener</code> object.
   * Only when that callback has been invoked can the application be sure that the message has been
   * successfully sent with the same degree of confidence as if a normal synchronous send had been
   * performed. An application which requires this degree of confidence must therefore wait for the
   * callback to be invoked before continuing.
   *
   * <p>The following information is intended to give an indication of how an asynchronous send
   * would typically be implemented.
   *
   * <p>In some JMS providers, a normal synchronous send involves sending the message to a remote
   * JMS server and then waiting for an acknowledgement to be received before returning. It is
   * expected that such a provider would implement an asynchronous send by sending the message to
   * the remote JMS server and then returning without waiting for an acknowledgement. When the
   * acknowledgement is received, the JMS provider would notify the application by invoking the
   * <code>onCompletion</code> method on the application-specified <code>CompletionListener</code>
   * object. If for some reason the acknowledgement is not received the JMS provider would notify
   * the application by invoking the <code>CompletionListener</code>'s <code>onException</code>
   * method.
   *
   * <p>In those cases where the JMS specification permits a lower level of reliability, a normal
   * synchronous send might not wait for an acknowledgement. In that case it is expected that an
   * asynchronous send would be similar to a synchronous send: the JMS provider would send the
   * message to the remote JMS server and then return without waiting for an acknowledgement.
   * However the JMS provider would still notify the application that the send had completed by
   * invoking the <code>onCompletion</code> method on the application-specified <code>
   * CompletionListener</code> object.
   *
   * <p>It is up to the JMS provider to decide exactly what is performed in the calling thread and
   * what, if anything, is performed asynchronously, so long as it satisfies the requirements given
   * below:
   *
   * <p><b>Quality of service</b>: After the send operation has completed successfully, which means
   * that the message has been successfully sent with the same degree of confidence as if a normal
   * synchronous send had been performed, the JMS provider must invoke the <code>CompletionListener
   * </code>'s <code>onCompletion</code> method. The <code>CompletionListener</code> must not be
   * invoked earlier than this.
   *
   * <p><b>Exceptions</b>: If an exception is encountered during the call to the <code>send</code>
   * method then an appropriate exception should be thrown in the thread that is calling the <code>
   * send</code> method. In this case the JMS provider must not invoke the <code>CompletionListener
   * </code>'s <code>onCompletion</code> or <code>onException</code> method. If an exception is
   * encountered which cannot be thrown in the thread that is calling the <code>send</code> method
   * then the JMS provider must call the <code>CompletionListener</code>'s <code>onException</code>
   * method. In both cases if an exception occurs it is undefined whether or not the message was
   * successfully sent.
   *
   * <p><b>Message order</b>: If the same <code>MessageProducer</code> is used to send multiple
   * messages then JMS message ordering requirements must be satisfied. This applies even if a
   * combination of synchronous and asynchronous sends has been performed. The application is not
   * required to wait for an asynchronous send to complete before sending the next message.
   *
   * <p><b>Close, commit or rollback</b>: If the <code>close</code> method is called on the <code>
   * MessageProducer</code> or its <code>Session</code> or <code>Connection</code> then the JMS
   * provider must block until any incomplete send operations have been completed and all {@code
   * CompletionListener} callbacks have returned before closing the object and returning. If the
   * session is transacted (uses a local transaction) then when the <code>Session</code>'s <code>
   * commit</code> or <code>rollback</code> method is called the JMS provider must block until any
   * incomplete send operations have been completed and all {@code CompletionListener} callbacks
   * have returned before performing the commit or rollback. Incomplete sends should be allowed to
   * complete normally unless an error occurs.
   *
   * <p>A <code>CompletionListener</code> callback method must not call <code>close</code> on its
   * own <code>Connection</code>, <code>Session</code> or <code>MessageProducer</code> or call
   * <code>commit</code> or <code>rollback</code> on its own <code>Session</code>. Doing so will
   * cause the <code>close</code>, <code>commit</code> or <code>rollback</code> to throw an <code>
   * IllegalStateException</code>.
   *
   * <p><b>Restrictions on usage in Java EE</b> This method must not be used in a Java EE EJB or web
   * container. Doing so may cause a {@code JMSException} to be thrown though this is not
   * guaranteed.
   *
   * <p><b>Message headers</b> JMS defines a number of message header fields and message properties
   * which must be set by the "JMS provider on send". If the send is asynchronous these fields and
   * properties may be accessed on the sending client only after the <code>CompletionListener</code>
   * has been invoked. If the <code>CompletionListener</code>'s <code>onException</code> method is
   * called then the state of these message header fields and properties is undefined.
   *
   * <p><b>Restrictions on threading</b>: Applications that perform an asynchronous send must
   * confirm to the threading restrictions defined in JMS. This means that the session may be used
   * by only one thread at a time.
   *
   * <p>Setting a <code>CompletionListener</code> does not cause the session to be dedicated to the
   * thread of control which calls the <code>CompletionListener</code>. The application thread may
   * therefore continue to use the session after performing an asynchronous send. However the <code>
   * CompletionListener</code>'s callback methods must not use the session if an application thread
   * might be using the session at the same time.
   *
   * <p><b>Use of the <code>CompletionListener</code> by the JMS provider</b>: A session will only
   * invoke one <code>CompletionListener</code> callback method at a time. For a given <code>
   * MessageProducer</code>, callbacks (both {@code onCompletion} and {@code onException}) will be
   * performed in the same order as the corresponding calls to the asynchronous send method. A JMS
   * provider must not invoke the <code>CompletionListener</code> from the thread that is calling
   * the asynchronous <code>send</code> method.
   *
   * <p><b>Restrictions on the use of the Message object</b>: Applications which perform an
   * asynchronous send must take account of the restriction that a <code>Message</code> object is
   * designed to be accessed by one logical thread of control at a time and does not support
   * concurrent use.
   *
   * <p>After the <code>send</code> method has returned, the application must not attempt to read
   * the headers, properties or body of the <code>Message</code> object until the <code>
   * CompletionListener</code>'s <code>onCompletion</code> or <code>onException</code> method has
   * been called. This is because the JMS provider may be modifying the <code>Message</code> object
   * in another thread during this time. The JMS provider may throw an <code>JMSException</code> if
   * the application attempts to access or modify the <code>Message</code> object after the <code>
   * send</code> method has returned and before the <code>CompletionListener</code> has been
   * invoked. If the JMS provider does not throw an exception then the behaviour is undefined.
   *
   * @param destination the destination to send this message to
   * @param message the message to send
   * @param completionListener a {@code CompletionListener} to be notified when the send has
   *     completed
   * @throws JMSException if an internal error occurs
   * @throws MessageFormatException if an invalid message is specified.
   * @throws InvalidDestinationException if a client uses this method with an invalid destination
   * @throws IllegalArgumentException if the specified {@code CompletionListener} is null
   * @throws UnsupportedOperationException if a client uses this method with a {@code
   *     MessageProducer} that specified a destination at creation time.
   * @see Session#createProducer
   * @see CompletionListener
   * @since JMS 2.0
   */
  @Override
  public void send(Destination destination, Message message, CompletionListener completionListener)
      throws JMSException {
    validateCompletionListener(completionListener);
    checkNoDefaultDestinationSet();
    if (!jms20) {
      // here the error is to be reported not in the CompletionListener
      validateDeliveryMode(deliveryMode);
    }
    try {
      validateMessageSend(
          message, destination, false, Message.DEFAULT_TIME_TO_LIVE, deliveryMode, priority);
    } catch (JMSException err) {
      completionListener.onException(message, err);
      return;
    }
    message.setJMSDeliveryMode(deliveryMode);
    message.setJMSPriority(priority);
    sendMessage(destination, message, completionListener);
  }

  /**
   * Sends a message to a destination for an unidentified message producer, specifying delivery
   * mode, priority and time to live, performing part of the work involved in sending the message in
   * a separate thread and notifying the specified <code>CompletionListener</code> when the
   * operation has completed. JMS refers to this as an "asynchronous send".
   *
   * <p>Typically, a message producer is assigned a destination at creation time; however, the JMS
   * API also supports unidentified message producers, which require that the destination be
   * supplied every time a message is sent.
   *
   * <p>When the message has been successfully sent the JMS provider invokes the callback method
   * <code>onCompletion</code> on an application-specified <code>CompletionListener</code> object.
   * Only when that callback has been invoked can the application be sure that the message has been
   * successfully sent with the same degree of confidence as if a normal synchronous send had been
   * performed. An application which requires this degree of confidence must therefore wait for the
   * callback to be invoked before continuing.
   *
   * <p>The following information is intended to give an indication of how an asynchronous send
   * would typically be implemented.
   *
   * <p>In some JMS providers, a normal synchronous send involves sending the message to a remote
   * JMS server and then waiting for an acknowledgement to be received before returning. It is
   * expected that such a provider would implement an asynchronous send by sending the message to
   * the remote JMS server and then returning without waiting for an acknowledgement. When the
   * acknowledgement is received, the JMS provider would notify the application by invoking the
   * <code>onCompletion</code> method on the application-specified <code>CompletionListener</code>
   * object. If for some reason the acknowledgement is not received the JMS provider would notify
   * the application by invoking the <code>CompletionListener</code>'s <code>onException</code>
   * method.
   *
   * <p>In those cases where the JMS specification permits a lower level of reliability, a normal
   * synchronous send might not wait for an acknowledgement. In that case it is expected that an
   * asynchronous send would be similar to a synchronous send: the JMS provider would send the
   * message to the remote JMS server and then return without waiting for an acknowledgement.
   * However the JMS provider would still notify the application that the send had completed by
   * invoking the <code>onCompletion</code> method on the application-specified <code>
   * CompletionListener</code> object.
   *
   * <p>It is up to the JMS provider to decide exactly what is performed in the calling thread and
   * what, if anything, is performed asynchronously, so long as it satisfies the requirements given
   * below:
   *
   * <p><b>Quality of service</b>: After the send operation has completed successfully, which means
   * that the message has been successfully sent with the same degree of confidence as if a normal
   * synchronous send had been performed, the JMS provider must invoke the <code>CompletionListener
   * </code>'s <code>onCompletion</code> method. The <code>CompletionListener</code> must not be
   * invoked earlier than this.
   *
   * <p><b>Exceptions</b>: If an exception is encountered during the call to the <code>send</code>
   * method then an appropriate exception should be thrown in the thread that is calling the <code>
   * send</code> method. In this case the JMS provider must not invoke the <code>CompletionListener
   * </code>'s <code>onCompletion</code> or <code>onException</code> method. If an exception is
   * encountered which cannot be thrown in the thread that is calling the <code>send</code> method
   * then the JMS provider must call the <code>CompletionListener</code>'s <code>onException</code>
   * method. In both cases if an exception occurs it is undefined whether or not the message was
   * successfully sent.
   *
   * <p><b>Message order</b>: If the same <code>MessageProducer</code> is used to send multiple
   * messages then JMS message ordering requirements must be satisfied. This applies even if a
   * combination of synchronous and asynchronous sends has been performed. The application is not
   * required to wait for an asynchronous send to complete before sending the next message.
   *
   * <p><b>Close, commit or rollback</b>: If the <code>close</code> method is called on the <code>
   * MessageProducer</code> or its <code>Session</code> or <code>Connection</code> then the JMS
   * provider must block until any incomplete send operations have been completed and all {@code
   * CompletionListener} callbacks have returned before closing the object and returning. If the
   * session is transacted (uses a local transaction) then when the <code>Session</code>'s <code>
   * commit</code> or <code>rollback</code> method is called the JMS provider must block until any
   * incomplete send operations have been completed and all {@code CompletionListener} callbacks
   * have returned before performing the commit or rollback. Incomplete sends should be allowed to
   * complete normally unless an error occurs.
   *
   * <p>A <code>CompletionListener</code> callback method must not call <code>close</code> on its
   * own <code>Connection</code>, <code>Session</code> or <code>MessageProducer</code> or call
   * <code>commit</code> or <code>rollback</code> on its own <code>Session</code>. Doing so will
   * cause the <code>close</code>, <code>commit</code> or <code>rollback</code> to throw an <code>
   * IllegalStateException</code>.
   *
   * <p><b>Restrictions on usage in Java EE</b> This method must not be used in a Java EE EJB or web
   * container. Doing so may cause a {@code JMSException} to be thrown though this is not
   * guaranteed.
   *
   * <p><b>Message headers</b> JMS defines a number of message header fields and message properties
   * which must be set by the "JMS provider on send". If the send is asynchronous these fields and
   * properties may be accessed on the sending client only after the <code>CompletionListener</code>
   * has been invoked. If the <code>CompletionListener</code>'s <code>onException</code> method is
   * called then the state of these message header fields and properties is undefined.
   *
   * <p><b>Restrictions on threading</b>: Applications that perform an asynchronous send must
   * confirm to the threading restrictions defined in JMS. This means that the session may be used
   * by only one thread at a time.
   *
   * <p>Setting a <code>CompletionListener</code> does not cause the session to be dedicated to the
   * thread of control which calls the <code>CompletionListener</code>. The application thread may
   * therefore continue to use the session after performing an asynchronous send. However the <code>
   * CompletionListener</code>'s callback methods must not use the session if an application thread
   * might be using the session at the same time.
   *
   * <p><b>Use of the <code>CompletionListener</code> by the JMS provider</b>: A session will only
   * invoke one <code>CompletionListener</code> callback method at a time. For a given <code>
   * MessageProducer</code>, callbacks (both {@code onCompletion} and {@code onException}) will be
   * performed in the same order as the corresponding calls to the asynchronous send method. A JMS
   * provider must not invoke the <code>CompletionListener</code> from the thread that is calling
   * the asynchronous <code>send</code> method.
   *
   * <p><b>Restrictions on the use of the Message object</b>: Applications which perform an
   * asynchronous send must take account of the restriction that a <code>Message</code> object is
   * designed to be accessed by one logical thread of control at a time and does not support
   * concurrent use.
   *
   * <p>After the <code>send</code> method has returned, the application must not attempt to read
   * the headers, properties or body of the <code>Message</code> object until the <code>
   * CompletionListener</code>'s <code>onCompletion</code> or <code>onException</code> method has
   * been called. This is because the JMS provider may be modifying the <code>Message</code> object
   * in another thread during this time. The JMS provider may throw an <code>JMSException</code> if
   * the application attempts to access or modify the <code>Message</code> object after the <code>
   * send</code> method has returned and before the <code>CompletionListener</code> has been
   * invoked. If the JMS provider does not throw an exception then the behaviour is undefined.
   *
   * @param destination the destination to send this message to
   * @param message the message to send
   * @param deliveryMode the delivery mode to use
   * @param priority the priority for this message
   * @param timeToLive the message's lifetime (in milliseconds)
   * @param completionListener a {@code CompletionListener} to be notified when the send has
   *     completed
   * @throws JMSException if an internal error occurs
   * @throws MessageFormatException if an invalid message is specified.
   * @throws InvalidDestinationException if a client uses this method with an invalid destination.
   * @throws IllegalArgumentException if the specified {@code CompletionListener} is null
   * @throws UnsupportedOperationException if a client uses this method with a {@code
   *     MessageProducer} that specified a destination at creation time.
   * @see Session#createProducer
   * @see CompletionListener
   * @since JMS 2.0
   */
  @Override
  public void send(
      Destination destination,
      Message message,
      int deliveryMode,
      int priority,
      long timeToLive,
      CompletionListener completionListener)
      throws JMSException {
    validateCompletionListener(completionListener);
    checkNoDefaultDestinationSet();
    if (!jms20) {
      // here the error is to be reported not in the CompletionListener
      validateDeliveryMode(deliveryMode);
      validatePriority(priority);
    }
    try {
      validateMessageSend(message, destination, false, timeToLive, deliveryMode, priority);
    } catch (JMSException err) {
      completionListener.onException(message, err);
      return;
    }
    message.setJMSDeliveryMode(deliveryMode);
    message.setJMSPriority(priority);
    applyTimeToLive(message, timeToLive);
    sendMessage(destination, message, completionListener);
  }

  private void applyTimeToLive(Message message, long timeToLive) throws JMSException {
    if (timeToLive > 0) {
      long time = System.currentTimeMillis() + timeToLive;
      message.setLongProperty("JMSExpiration", System.currentTimeMillis() + timeToLive);
      message.setJMSExpiration(time);
    }
  }

  private void validateCompletionListener(CompletionListener completionListener) {
    if (completionListener == null) {
      throw new IllegalArgumentException("IllegalArgumentException is null");
    }
  }

  private void sendMessage(Destination defaultDestination, Message message) throws JMSException {
    if (message == null) {
      throw new MessageFormatException("null message");
    }
    session.executeCriticalOperation(
        () -> {
          Producer<byte[]> producer = session.getProducerForDestination(defaultDestination, this);
          message.setJMSDestination(defaultDestination);
          PulsarMessage pulsarMessage = prepareMessageForSend(message);
          final TypedMessageBuilder<byte[]> typedMessageBuilder;
          session.blockTransactionOperations();
          try {
            if (session.getTransacted()) {
              Transaction transaction = session.getTransaction();
              if (transaction != null) {
                typedMessageBuilder = producer.newMessage(transaction);
              } else {
                // emulated transactions
                typedMessageBuilder = producer.newMessage();
                if (uncommittedMessages == null) {
                  uncommittedMessages = new ArrayList<>();
                }
                uncommittedMessages.add(new PreparedMessage(typedMessageBuilder, pulsarMessage));
                session.registerProducerWithTransaction(this);
                if (defaultDeliveryDelay > 0) {
                  typedMessageBuilder.deliverAfter(defaultDeliveryDelay, TimeUnit.MILLISECONDS);
                }
                return null;
              }
            } else {
              typedMessageBuilder = producer.newMessage();
            }
            if (defaultDeliveryDelay > 0) {
              typedMessageBuilder.deliverAfter(defaultDeliveryDelay, TimeUnit.MILLISECONDS);
            }
            pulsarMessage.send(typedMessageBuilder, disableMessageTimestamp, session);
          } finally {
            session.unblockTransactionOperations();
          }
          if (message != pulsarMessage) {
            applyBackMessageProperties(message, pulsarMessage);
          }
          return null;
        });
  }

  private static class PreparedMessage {
    final TypedMessageBuilder<byte[]> typedMessageBuilder;
    final PulsarMessage pulsarMessage;

    PreparedMessage(TypedMessageBuilder<byte[]> typedMessageBuilder, PulsarMessage pulsarMessage) {
      this.typedMessageBuilder = typedMessageBuilder;
      this.pulsarMessage = pulsarMessage;
    }
  }

  private void sendMessage(
      Destination defaultDestination, Message message, CompletionListener completionListener)
      throws JMSException {
    if (message == null) {
      throw new MessageFormatException("null message");
    }
    session.executeCriticalOperation(
        () -> {
          Producer<byte[]> producer = session.getProducerForDestination(defaultDestination, this);
          message.setJMSDestination(defaultDestination);
          PulsarMessage pulsarMessage = prepareMessageForSend(message);
          CompletionListener endActivityCompletionListener =
              new CompletionListener() {
                @Override
                public void onCompletion(Message message) {
                  try {
                    completionListener.onCompletion(message);
                  } finally {
                    session.unblockTransactionOperations();
                  }
                }

                @Override
                public void onException(Message message, Exception exception) {
                  try {
                    completionListener.onException(message, exception);
                  } finally {
                    session.unblockTransactionOperations();
                  }
                }
              };
          CompletionListener finalCompletionListener = endActivityCompletionListener;
          if (pulsarMessage != message) {
            finalCompletionListener =
                new CompletionListener() {
                  @Override
                  public void onCompletion(Message completedMessage) {
                    // we have to pass the original message to the called
                    applyBackMessageProperties(message, pulsarMessage);
                    endActivityCompletionListener.onCompletion(message);
                  }

                  @Override
                  public void onException(Message completedMessage, Exception e) {
                    // we have to pass the original message to the called
                    applyBackMessageProperties(message, pulsarMessage);
                    endActivityCompletionListener.onException(message, e);
                  }
                };
          }

          session.blockTransactionOperations();
          TypedMessageBuilder<byte[]> typedMessageBuilder;
          if (session.getTransacted()) {
            Transaction transaction = session.getTransaction();
            if (transaction != null) {
              typedMessageBuilder = producer.newMessage(transaction);
            } else {
              // emulated transactions
              typedMessageBuilder = producer.newMessage();
              if (defaultDeliveryDelay > 0) {
                typedMessageBuilder.deliverAfter(defaultDeliveryDelay, TimeUnit.MILLISECONDS);
              }
              if (uncommittedMessages == null) {
                uncommittedMessages = new ArrayList<>();
              }
              uncommittedMessages.add(new PreparedMessage(typedMessageBuilder, pulsarMessage));
              session.registerProducerWithTransaction(this);
              finalCompletionListener.onCompletion(pulsarMessage);
              return null;
            }
          } else {
            typedMessageBuilder = producer.newMessage();
          }
          if (defaultDeliveryDelay > 0) {
            typedMessageBuilder.deliverAfter(defaultDeliveryDelay, TimeUnit.MILLISECONDS);
          }
          pulsarMessage.sendAsync(
              typedMessageBuilder, finalCompletionListener, session, this, disableMessageTimestamp);
          return null;
        });
  }

  protected void rollbackEmulatedTransaction() {
    uncommittedMessages = null;
  }

  protected void commitEmulatedTransaction() throws JMSException {
    if (uncommittedMessages != null) {
      List<CompletableFuture<?>> callbacks = new ArrayList<>();

      // we are sending all the messages in async mode
      // users use emulated transactions in order to have a better performance
      // even with sync mode
      for (PreparedMessage message : uncommittedMessages) {
        CompletableFuture<?> result = new CompletableFuture<>();
        callbacks.add(result);
        message.pulsarMessage.sendAsync(
            message.typedMessageBuilder,
            new CompletionListener() {
              @Override
              public void onCompletion(Message message) {
                result.complete(null);
              }

              @Override
              public void onException(Message message, Exception e) {
                log.error("Error while sending message {} during emulated transaction", message, e);
                result.completeExceptionally(e);
              }
            },
            session,
            this,
            disableMessageTimestamp);
      }
      uncommittedMessages = null;
      try {
        FutureUtil.waitForAll(callbacks).join();
      } catch (RuntimeException error) {
        throw new JMSException(
            "Some errors occurred while sending messages during emulated transaction");
      }
    }
  }

  private void applyBackMessageProperties(Message message, PulsarMessage pulsarMessage) {
    Utils.runtimeException(
        () -> {
          message.setJMSTimestamp(pulsarMessage.getJMSTimestamp());
          message.setJMSExpiration(pulsarMessage.getJMSExpiration());
          message.setJMSMessageID(pulsarMessage.getJMSMessageID());
        });
  }

  @Override
  public Queue getQueue() throws JMSException {
    checkNotClosed();
    if (defaultDestination.isQueue()) {
      return (Queue) defaultDestination;
    }
    throw new JMSException("Created on a topic");
  }

  @Override
  public void send(Queue queue, Message message) throws JMSException {
    this.send((Destination) queue, message);
  }

  @Override
  public void send(Queue queue, Message message, int i, int i1, long l) throws JMSException {
    this.send((Destination) queue, message, i, i1, l);
  }

  @Override
  public Topic getTopic() throws JMSException {
    checkNotClosed();
    if (defaultDestination.isTopic()) {
      return (Topic) defaultDestination;
    }
    throw new JMSException("Created on a queue");
  }

  @Override
  public void publish(Message message) throws JMSException {
    send(message);
  }

  @Override
  public void publish(Message message, int i, int i1, long l) throws JMSException {
    send(message, i, i1, l);
  }

  @Override
  public void publish(Topic topic, Message message) throws JMSException {
    send((Destination) topic, message);
  }

  @Override
  public void publish(Topic topic, Message message, int i, int i1, long l) throws JMSException {
    send((Destination) topic, message, i, i1, l);
  }
}
