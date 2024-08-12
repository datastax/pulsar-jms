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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidDestinationRuntimeException;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.MessageNotWriteableRuntimeException;

public class PulsarJMSProducer implements JMSProducer {
  private final PulsarJMSContext parent;
  private boolean disableMessageId;
  private boolean disableMessageTimestamp;
  private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
  private int priority = Message.DEFAULT_PRIORITY;
  private long deliveryDelay = Message.DEFAULT_DELIVERY_DELAY;
  private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
  private final Map<String, Object> properties = new HashMap<>();
  private CompletionListener completionListener;
  private byte[] correlationID;
  private String jmsType;
  private Destination jmsReplyTo;

  public PulsarJMSProducer(PulsarJMSContext parent) {
    this.parent = parent;
  }

  /**
   * Sends a message to the specified destination, using any send options, message properties and
   * message headers that have been defined on this {@code JMSProducer}.
   *
   * @param destination the destination to send this message to
   * @param message the message to send
   * @return this {@code JMSProducer}
   * @throws MessageFormatRuntimeException if an invalid message is specified.
   * @throws InvalidDestinationRuntimeException if a client uses this method with an invalid
   *     destination.
   * @throws MessageNotWriteableRuntimeException if this {@code JMSProducer} has been configured to
   *     set a message property, but the message's properties are read-only
   * @throws JMSRuntimeException if the JMS provider fails to send the message due to some internal
   *     error.
   */
  @Override
  public JMSProducer send(Destination destination, Message message) {
    Utils.runtimeException(() -> getProducerAndSend(destination, message));
    return this;
  }

  /**
   * Send a {@code TextMessage} with the specified body to the specified destination, using any send
   * options, message properties and message headers that have been defined on this {@code
   * JMSProducer}.
   *
   * @param destination the destination to send this message to
   * @param body the body of the {@code TextMessage} that will be sent. If a null value is specified
   *     then a {@code TextMessage} with no body will be sent.
   * @return this {@code JMSProducer}
   * @throws MessageFormatRuntimeException if an invalid message is specified.
   * @throws InvalidDestinationRuntimeException if a client uses this method with an invalid
   *     destination.
   * @throws JMSRuntimeException if the JMS provider fails to send the message due to some internal
   *     error.
   */
  @Override
  public JMSProducer send(Destination destination, String body) {
    Utils.runtimeException(
        () -> getProducerAndSend(destination, parent.session.createTextMessage(body)));
    return this;
  }

  /**
   * Send a {@code MapMessage} with the specified body to the specified destination, using any send
   * options, message properties and message headers that have been defined on this {@code
   * JMSProducer}.
   *
   * @param destination the destination to send this message to
   * @param body the body of the {@code MapMessage} that will be sent. If a null value is specified
   *     then a {@code MapMessage} with no map entries will be sent.
   * @return this {@code JMSProducer}
   * @throws MessageFormatRuntimeException if an invalid message is specified.
   * @throws InvalidDestinationRuntimeException if a client uses this method with an invalid
   *     destination.
   * @throws JMSRuntimeException if the JMS provider fails to send the message due to some internal
   *     error.
   */
  @Override
  public JMSProducer send(Destination destination, Map<String, Object> body) {
    Utils.runtimeException(
        () -> getProducerAndSend(destination, parent.session.createMapMessage(body)));
    return this;
  }

  /**
   * Send a {@code BytesMessage} with the specified body to the specified destination, using any
   * send options, message properties and message headers that have been defined on this {@code
   * JMSProducer}.
   *
   * @param destination the destination to send this message to
   * @param body the body of the {@code BytesMessage} that will be sent. If a null value is
   *     specified then a {@code BytesMessage} with no body will be sent.
   * @return this {@code JMSProducer}
   * @throws MessageFormatRuntimeException if an invalid message is specified.
   * @throws InvalidDestinationRuntimeException if a client uses this method with an invalid
   *     destination.
   * @throws JMSRuntimeException if the JMS provider fails to send the message due to some internal
   *     error.
   */
  @Override
  public JMSProducer send(Destination destination, byte[] body) {
    Utils.runtimeException(
        () -> getProducerAndSend(destination, parent.session.createBytesMessage().fill(body)));
    return this;
  }

  /**
   * Send an {@code ObjectMessage} with the specified body to the specified destination, using any
   * send options, message properties and message headers that have been defined on this {@code
   * JMSProducer}.
   *
   * @param destination the destination to send this message to
   * @param body the body of the ObjectMessage that will be sent. If a null value is specified then
   *     an {@code ObjectMessage} with no body will be sent.
   * @return this {@code JMSProducer}
   * @throws MessageFormatRuntimeException if an invalid message is specified.
   * @throws InvalidDestinationRuntimeException if a client uses this method with an invalid
   *     destination.
   * @throws JMSRuntimeException if JMS provider fails to send the message due to some internal
   *     error.
   */
  @Override
  public JMSProducer send(Destination destination, Serializable body) {
    Utils.runtimeException(
        () -> getProducerAndSend(destination, parent.session.createObjectMessage(body)));
    return this;
  }

  private void getProducerAndSend(Destination destination, Message message) throws JMSException {
    if (message == null) {
      if (completionListener != null) {
        completionListener.onException(null, new MessageFormatRuntimeException("message is null"));
        return;
      } else {
        throw new MessageFormatException("message is null");
      }
    }
    if (destination == null) {
      if (completionListener != null) {
        completionListener.onException(
            message, new InvalidDestinationRuntimeException("message is null"));
        return;
      } else {
        throw new InvalidDestinationException("destination is null");
      }
    }
    PulsarMessageProducer producer = parent.session.createProducer(null);
    producer.setDisableMessageID(disableMessageId);

    producer.setDisableMessageTimestamp(disableMessageTimestamp);
    producer.setDeliveryMode(deliveryMode);
    producer.setPriority(priority);
    producer.setDeliveryDelay(deliveryDelay);
    producer.setTimeToLive(timeToLive);
    for (Map.Entry<String, Object> prop : properties.entrySet()) {
      message.setObjectProperty(prop.getKey(), prop.getValue());
    }
    if (message.getJMSPriority() != Message.DEFAULT_PRIORITY) {
      message.setJMSPriority(priority);
    }
    if (message.getJMSCorrelationIDAsBytes() != null) {
      message.setJMSCorrelationIDAsBytes(message.getJMSCorrelationIDAsBytes());
    }
    if (message.getJMSType() != null) {
      message.setJMSType(message.getJMSType());
    }
    if (message.getJMSReplyTo() != null) {
      message.setJMSReplyTo(message.getJMSReplyTo());
    }

    if (completionListener != null) {
      producer.send(destination, message, deliveryMode, priority, timeToLive, completionListener);
    } else {
      producer.send(destination, message, deliveryMode, priority, timeToLive);
    }
  }

  /**
   * Specifies whether message IDs may be disabled for messages that are sent using this {@code
   * JMSProducer}
   *
   * <p>Since message IDs take some effort to create and increase a message's size, some JMS
   * providers may be able to optimise message overhead if they are given a hint that the message ID
   * is not used by an application. By calling this method, a JMS application enables this potential
   * optimisation for all messages sent using this {@code JMSProducer}. If the JMS provider accepts
   * this hint, these messages must have the message ID set to null; if the provider ignores the
   * hint, the message ID must be set to its normal unique value.
   *
   * <p>Message IDs are enabled by default.
   *
   * @param value indicates whether message IDs may be disabled
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set message ID to disabled due to some
   *     internal error.
   * @see JMSProducer#getDisableMessageID
   */
  @Override
  public JMSProducer setDisableMessageID(boolean value) {
    this.disableMessageId = value;
    return this;
  }

  /**
   * Gets an indication of whether message IDs are disabled.
   *
   * @return an indication of whether message IDs are disabled
   * @throws JMSRuntimeException if the JMS provider fails to determine if message IDs are disabled
   *     due to some internal error.
   * @see JMSProducer#setDisableMessageID
   */
  @Override
  public boolean getDisableMessageID() {
    return disableMessageId;
  }

  /**
   * Specifies whether message timestamps may be disabled for messages that are sent using this
   * {@code JMSProducer}.
   *
   * <p>Since timestamps take some effort to create and increase a message's size, some JMS
   * providers may be able to optimise message overhead if they are given a hint that the timestamp
   * is not used by an application. By calling this method, a JMS application enables this potential
   * optimisation for all messages sent using this {@code JMSProducer}. If the JMS provider accepts
   * this hint, these messages must have the timestamp set to zero; if the provider ignores the
   * hint, the timestamp must be set to its normal value.
   *
   * <p>Message timestamps are enabled by default.
   *
   * @param value indicates whether message timestamps may be disabled
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set timestamps to disabled due to some
   *     internal error.
   * @see JMSProducer#getDisableMessageTimestamp
   */
  @Override
  public JMSProducer setDisableMessageTimestamp(boolean value) {
    this.disableMessageTimestamp = value;
    return this;
  }

  /**
   * Gets an indication of whether message timestamps are disabled.
   *
   * @return an indication of whether message timestamps are disabled
   * @throws JMSRuntimeException if the JMS provider fails to determine if timestamps are disabled
   *     due to some internal error.
   * @see JMSProducer#setDisableMessageTimestamp
   */
  @Override
  public boolean getDisableMessageTimestamp() {
    return disableMessageTimestamp;
  }

  /**
   * Specifies the delivery mode of messages that are sent using this {@code JMSProducer}
   *
   * <p>Delivery mode is set to {@code PERSISTENT} by default.
   *
   * @param deliveryMode the message delivery mode to be used; legal values are {@code
   *     DeliveryMode.NON_PERSISTENT} and {@code DeliveryMode.PERSISTENT}
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the delivery mode due to some
   *     internal error.
   * @see JMSProducer#getDeliveryMode
   * @see DeliveryMode#NON_PERSISTENT
   * @see DeliveryMode#PERSISTENT
   * @see Message#DEFAULT_DELIVERY_MODE
   */
  @Override
  public JMSProducer setDeliveryMode(int deliveryMode) {
    switch (deliveryMode) {
      case DeliveryMode.NON_PERSISTENT:
      case DeliveryMode.PERSISTENT:
        break;
      default:
        throw new JMSRuntimeException("Invalid deliveryMode " + deliveryMode);
    }
    this.deliveryMode = deliveryMode;
    return this;
  }

  /**
   * Returns the delivery mode of messages that are sent using this {@code JMSProducer}
   *
   * @return the message delivery mode
   * @throws JMSRuntimeException if the JMS provider fails to get the delivery mode due to some
   *     internal error.
   * @see JMSProducer#setDeliveryMode
   */
  @Override
  public int getDeliveryMode() {
    return deliveryMode;
  }

  /**
   * Specifies the priority of messages that are sent using this {@code JMSProducer}
   *
   * <p>The JMS API defines ten levels of priority value, with 0 as the lowest priority and 9 as the
   * highest. Clients should consider priorities 0-4 as gradations of normal priority and priorities
   * 5-9 as gradations of expedited priority. Priority is set to 4 by default.
   *
   * @param priority the message priority to be used; must be a value between 0 and 9
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the priority due to some internal
   *     error.
   * @see JMSProducer#getPriority
   * @see Message#DEFAULT_PRIORITY
   */
  @Override
  public JMSProducer setPriority(int priority) {
    if (priority < 0 || priority > 10) {
      throw new JMSRuntimeException("Invalid priority " + priority);
    }
    this.priority = priority;
    return this;
  }

  /**
   * Return the priority of messages that are sent using this {@code JMSProducer}
   *
   * @return the message priority
   * @throws JMSRuntimeException if the JMS provider fails to get the priority due to some internal
   *     error.
   * @see JMSProducer#setPriority
   */
  @Override
  public int getPriority() {
    return priority;
  }

  /**
   * Specifies the time to live of messages that are sent using this {@code JMSProducer}. This is
   * used to determine the expiration time of a message.
   *
   * <p>The expiration time of a message is the sum of the message's time to live and the time it is
   * sent. For transacted sends, this is the time the client sends the message, not the time the
   * transaction is committed.
   *
   * <p>Clients should not receive messages that have expired; however, JMS does not guarantee that
   * this will not happen.
   *
   * <p>A JMS provider should do its best to accurately expire messages; however, JMS does not
   * define the accuracy provided. It is not acceptable to simply ignore time-to-live.
   *
   * <p>Time to live is set to zero by default, which means a message never expires.
   *
   * @param timeToLive the message time to live to be used, in milliseconds; a value of zero means
   *     that a message never expires.
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the time to live due to some
   *     internal error.
   * @see JMSProducer#getTimeToLive
   * @see Message#DEFAULT_TIME_TO_LIVE
   */
  @Override
  public JMSProducer setTimeToLive(long timeToLive) {
    this.timeToLive = timeToLive;
    return this;
  }

  /**
   * Returns the time to live of messages that are sent using this {@code JMSProducer}.
   *
   * @return the message time to live in milliseconds; a value of zero means that a message never
   *     expires.
   * @throws JMSRuntimeException if the JMS provider fails to get the time to live due to some
   *     internal error.
   * @see JMSProducer#setTimeToLive
   */
  @Override
  public long getTimeToLive() {
    return timeToLive;
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
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the delivery delay due to some
   *     internal error.
   * @see JMSProducer#getDeliveryDelay
   * @see Message#DEFAULT_DELIVERY_DELAY
   */
  @Override
  public JMSProducer setDeliveryDelay(long deliveryDelay) {
    this.deliveryDelay = deliveryDelay;
    return this;
  }

  /**
   * Gets the minimum length of time in milliseconds that must elapse after a message is sent before
   * the JMS provider may deliver the message to a consumer.
   *
   * @return the delivery delay in milliseconds.
   * @throws JMSRuntimeException if the JMS provider fails to get the delivery delay due to some
   *     internal error.
   * @see JMSProducer#setDeliveryDelay
   */
  @Override
  public long getDeliveryDelay() {
    return deliveryDelay;
  }

  /**
   * Specifies whether subsequent calls to {@code send} on this {@code JMSProducer} object should be
   * synchronous or asynchronous. If the specified {@code CompletionListener} is not null then
   * subsequent calls to {@code send} will be asynchronous. If the specified {@code
   * CompletionListener} is null then subsequent calls to {@code send} will be synchronous. Calls to
   * {@code send} are synchronous by default.
   *
   * <p>If a call to {@code send} is asynchronous then part of the work involved in sending the
   * message will be performed in a separate thread and the specified <code>CompletionListener
   * </code> will be notified when the operation has completed.
   *
   * <p>When the message has been successfully sent the JMS provider invokes the callback method
   * <code>onCompletion</code> on the <code>CompletionListener</code> object. Only when that
   * callback has been invoked can the application be sure that the message has been successfully
   * sent with the same degree of confidence as if the send had been synchronous. An application
   * which requires this degree of confidence must therefore wait for the callback to be invoked
   * before continuing.
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
   * <p><b>Message order</b>: If the same <code>JMSContext</code> is used to send multiple messages
   * then JMS message ordering requirements must be satisfied. This applies even if a combination of
   * synchronous and asynchronous sends has been performed. The application is not required to wait
   * for an asynchronous send to complete before sending the next message.
   *
   * <p><b>Close, commit or rollback</b>: If the <code>close</code> method is called on the <code>
   * JMSContext</code> then the JMS provider must block until any incomplete send operations have
   * been completed and all {@code CompletionListener} callbacks have returned before closing the
   * object and returning. If the session is transacted (uses a local transaction) then when the
   * <code>JMSContext</code>'s <code>commit</code> or <code>rollback</code> method is called the JMS
   * provider must block until any incomplete send operations have been completed and all {@code
   * CompletionListener} callbacks have returned before performing the commit or rollback.
   * Incomplete sends should be allowed to complete normally unless an error occurs.
   *
   * <p>A <code>CompletionListener</code> callback method must not call <code>close</code>, <code>
   * commit</code> or <code>rollback</code> on its own <code>JMSContext</code>. Doing so will cause
   * the <code>close</code>, <code>commit</code> or <code>rollback</code> to throw an <code>
   * IllegalStateRuntimeException</code>.
   *
   * <p><b>Restrictions on usage in Java EE</b> This method must not be used in a Java EE EJB or web
   * container. Doing so may cause a {@code JMSRuntimeException} to be thrown though this is not
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
   * JMSContext</code>, callbacks (both {@code onCompletion} and {@code onException}) will be
   * performed in the same order as the corresponding calls to the <code>send</code> method. A JMS
   * provider must not invoke the <code>CompletionListener</code> from the thread that is calling
   * the <code>send</code> method.
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
   * @param completionListener If asynchronous send behaviour is required, this should be set to a
   *     {@code CompletionListener} to be notified when the send has completed. If synchronous send
   *     behaviour is required, this should be set to {@code null}.
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if an internal error occurs
   * @see JMSProducer#getAsync
   * @see CompletionListener
   */
  @Override
  public JMSProducer setAsync(CompletionListener completionListener) {
    this.completionListener = completionListener;
    return this;
  }

  /**
   * If subsequent calls to {@code send} on this {@code JMSProducer} object have been configured to
   * be asynchronous then this method returns the {@code CompletionListener} that has previously
   * been configured. If subsequent calls to {@code send} have been configured to be synchronous
   * then this method returns {@code null}.
   *
   * @return the {@code CompletionListener} or {@code null}
   * @throws JMSRuntimeException if the JMS provider fails to get the required information due to
   *     some internal error.
   * @see JMSProducer#setAsync
   */
  @Override
  public CompletionListener getAsync() {
    return completionListener;
  }

  /**
   * Specifies that messages sent using this {@code JMSProducer} will have the specified property
   * set to the specified {@code boolean} value.
   *
   * <p>This will replace any property of the same name that is already set on the message being
   * sent.
   *
   * @param name the name of the property
   * @param value the {@code boolean} value to set
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the property due to some internal
   *     error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @see JMSProducer#getBooleanProperty
   */
  @Override
  public JMSProducer setProperty(String name, boolean value) {
    setPropertyInternal(name, value);
    return this;
  }

  /**
   * Specifies that messages sent using this {@code JMSProducer} will have the specified property
   * set to the specified {@code byte} value.
   *
   * <p>This will replace any property of the same name that is already set on the message being
   * sent.
   *
   * @param name the name of the property
   * @param value the {@code byte} value to set
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the property due to some internal
   *     error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @see JMSProducer#getByteProperty
   */
  @Override
  public JMSProducer setProperty(String name, byte value) {
    setPropertyInternal(name, value);
    return this;
  }

  /**
   * Specifies that messages sent using this {@code JMSProducer} will have the specified property
   * set to the specified {@code short} value.
   *
   * <p>This will replace any property of the same name that is already set on the message being
   * sent.
   *
   * @param name the name of the property
   * @param value the {@code short} property value to set
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the property due to some internal
   *     error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @see JMSProducer#getShortProperty
   */
  @Override
  public JMSProducer setProperty(String name, short value) {
    setPropertyInternal(name, value);
    return this;
  }

  /**
   * Specifies that messages sent using this {@code JMSProducer} will have the specified property
   * set to the specified {@code int} value.
   *
   * <p>This will replace any property of the same name that is already set on the message being
   * sent.
   *
   * @param name the name of the property
   * @param value the {@code int} property value to set
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the property due to some internal
   *     error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @see JMSProducer#getIntProperty
   */
  @Override
  public JMSProducer setProperty(String name, int value) {
    setPropertyInternal(name, value);
    return this;
  }

  /**
   * Specifies that messages sent using this {@code JMSProducer} will have the specified property
   * set to the specified {@code long} value.
   *
   * <p>This will replace any property of the same name that is already set on the message being
   * sent.
   *
   * @param name the name of the property
   * @param value the {@code long} property value to set
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the property due to some internal
   *     error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @see JMSProducer#getLongProperty
   */
  @Override
  public JMSProducer setProperty(String name, long value) {
    setPropertyInternal(name, value);
    return this;
  }

  private void setPropertyInternal(String name, Object value) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Invalid empty property name");
    }
    Utils.runtimeException(() -> PulsarMessage.validateWritableObject(value));
    this.properties.put(name, value);
  }

  /**
   * Specifies that messages sent using this {@code JMSProducer} will have the specified property
   * set to the specified {@code float} value.
   *
   * <p>This will replace any property of the same name that is already set on the message being
   * sent.
   *
   * @param name the name of the property
   * @param value the {@code float} property value to set
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the property due to some internal
   *     error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @see JMSProducer#getFloatProperty
   */
  @Override
  public JMSProducer setProperty(String name, float value) {
    setPropertyInternal(name, value);
    return this;
  }

  /**
   * Specifies that messages sent using this {@code JMSProducer} will have the specified property
   * set to the specified {@code double} value.
   *
   * <p>This will replace any property of the same name that is already set on the message being
   * sent.
   *
   * @param name the name of the property
   * @param value the {@code double} property value to set
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the property due to some internal
   *     error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @see JMSProducer#getDoubleProperty
   */
  @Override
  public JMSProducer setProperty(String name, double value) {
    setPropertyInternal(name, value);
    return this;
  }

  /**
   * Specifies that messages sent using this {@code JMSProducer} will have the specified property
   * set to the specified {@code String} value.
   *
   * <p>This will replace any property of the same name that is already set on the message being
   * sent.
   *
   * @param name the name of the property
   * @param value the {@code String} property value to set
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the property due to some internal
   *     error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @see JMSProducer#getStringProperty
   */
  @Override
  public JMSProducer setProperty(String name, String value) {
    setPropertyInternal(name, value);
    return this;
  }

  /**
   * Specifies that messages sent using this {@code JMSProducer} will have the specified property
   * set to the specified Java object value.
   *
   * <p>Note that this method works only for the objectified primitive object types ({@code
   * Integer}, {@code Double}, {@code Long} ...) and {@code String} objects.
   *
   * <p>This will replace any property of the same name that is already set on the message being
   * sent.
   *
   * @param name the name of the property
   * @param value the Java object property value to set
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the property due to some internal
   *     error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageFormatRuntimeException if the object is invalid
   * @see JMSProducer#getObjectProperty
   */
  @Override
  public JMSProducer setProperty(String name, Object value) {
    setPropertyInternal(name, value);
    return this;
  }

  /**
   * Clears any message properties set on this {@code JMSProducer}
   *
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to clear the message properties due to
   *     some internal error.
   */
  @Override
  public JMSProducer clearProperties() {
    this.properties.clear();
    return this;
  }

  /**
   * Indicates whether a message property with the specified name has been set on this {@code
   * JMSProducer}
   *
   * @param name the name of the property
   * @return true whether the property exists
   * @throws JMSRuntimeException if the JMS provider fails to determine whether the property exists
   *     due to some internal error.
   */
  @Override
  public boolean propertyExists(String name) {
    return properties.containsKey(name);
  }

  /**
   * Returns the message property with the specified name that has been set on this {@code
   * JMSProducer}, converted to a {@code boolean}.
   *
   * @param name the name of the property
   * @return the property value, converted to a {@code boolean}
   * @throws JMSRuntimeException if the JMS provider fails to get the property value due to some
   *     internal error.
   * @throws MessageFormatRuntimeException if this type conversion is invalid.
   * @see JMSProducer#setProperty(String, boolean)
   */
  @Override
  public boolean getBooleanProperty(String name) {
    switch (properties.getOrDefault(name, "false").toString()) {
      case "true":
        return true;
      case "false":
        return false;
    }
    throw new MessageFormatRuntimeException("Invalid value for boolean");
  }

  /**
   * Returns the message property with the specified name that has been set on this {@code
   * JMSProducer}, converted to a {@code String}.
   *
   * @param name the name of the property
   * @return the property value, converted to a {@code byte}
   * @throws JMSRuntimeException if the JMS provider fails to get the property value due to some
   *     internal error.
   * @throws MessageFormatRuntimeException if this type conversion is invalid.
   * @see JMSProducer#setProperty(String, byte)
   */
  @Override
  public byte getByteProperty(String name) {
    if (!properties.containsKey(name)) {
      throw new NumberFormatException();
    }
    final Object currentValue = properties.getOrDefault(name, "0");
    if (currentValue instanceof Number) {
      if (!(currentValue instanceof Byte)) {
        throw new MessageFormatRuntimeException("unsupported conversion");
      } else {
        return ((Number) currentValue).byteValue();
      }
    }
    return Utils.runtimeException(() -> Byte.parseByte(currentValue.toString()));
  }

  /**
   * Returns the message property with the specified name that has been set on this {@code
   * JMSProducer}, converted to a {@code short}.
   *
   * @param name the name of the property
   * @return the property value, converted to a {@code short}
   * @throws JMSRuntimeException if the JMS provider fails to get the property value due to some
   *     internal error.
   * @throws MessageFormatRuntimeException if this type conversion is invalid.
   * @see JMSProducer#setProperty(String, short)
   */
  @Override
  public short getShortProperty(String name) {
    if (!properties.containsKey(name)) {
      throw new NumberFormatException();
    }
    final Object currentValue = properties.getOrDefault(name, "0");
    if (currentValue instanceof Number) {
      if (!(currentValue instanceof Short) && !(currentValue instanceof Byte)) {
        throw new MessageFormatRuntimeException("unsupported conversion");
      } else {
        return ((Number) currentValue).shortValue();
      }
    }
    return Utils.runtimeException(() -> Short.parseShort(currentValue.toString()));
  }

  /**
   * Returns the message property with the specified name that has been set on this {@code
   * JMSProducer}, converted to a {@code int}.
   *
   * @param name the name of the property
   * @return the property value, converted to a {@code int}
   * @throws JMSRuntimeException if the JMS provider fails to get the property value due to some
   *     internal error.
   * @throws MessageFormatRuntimeException if this type conversion is invalid.
   * @see JMSProducer#setProperty(String, int)
   */
  @Override
  public int getIntProperty(String name) {
    if (!properties.containsKey(name)) {
      throw new NumberFormatException();
    }
    final Object currentValue = properties.getOrDefault(name, "0");
    if (currentValue instanceof Number) {
      if (!(currentValue instanceof Short)
          && !(currentValue instanceof Integer)
          && !(currentValue instanceof Byte)) {
        throw new MessageFormatRuntimeException("unsupported conversion");
      } else {
        return ((Number) currentValue).intValue();
      }
    }
    return Utils.runtimeException(() -> Integer.parseInt(currentValue.toString()));
  }

  /**
   * Returns the message property with the specified name that has been set on this {@code
   * JMSProducer}, converted to a {@code long}.
   *
   * @param name the name of the property
   * @return the property value, converted to a {@code long}
   * @throws JMSRuntimeException if the JMS provider fails to get the property value due to some
   *     internal error.
   * @throws MessageFormatRuntimeException if this type conversion is invalid.
   * @see JMSProducer#setProperty(String, long)
   */
  @Override
  public long getLongProperty(String name) {
    if (!properties.containsKey(name)) {
      throw new NumberFormatException();
    }
    final Object currentValue = properties.getOrDefault(name, "0");
    if (currentValue instanceof Number) {
      if (!(currentValue instanceof Short)
          && !(currentValue instanceof Integer)
          && !(currentValue instanceof Byte)
          && !(currentValue instanceof Long)) {
        throw new MessageFormatRuntimeException("unsupported conversion");
      } else {
        return ((Number) currentValue).longValue();
      }
    }
    return Utils.runtimeException(() -> Long.parseLong(currentValue.toString()));
  }

  /**
   * Returns the message property with the specified name that has been set on this {@code
   * JMSProducer}, converted to a {@code float}.
   *
   * @param name the name of the property
   * @return the property value, converted to a {@code float}
   * @throws JMSRuntimeException if the JMS provider fails to get the property value due to some
   *     internal error.
   * @throws MessageFormatRuntimeException if this type conversion is invalid.
   * @see JMSProducer#setProperty(String, float)
   */
  @Override
  public float getFloatProperty(String name) {
    if (!properties.containsKey(name)) {
      throw new NullPointerException();
    }
    final Object currentValue = properties.getOrDefault(name, "0");
    if (currentValue instanceof Number) {
      if (!(currentValue instanceof Float)) {
        throw new MessageFormatRuntimeException("unsupported conversion");
      } else {
        return (Float) currentValue;
      }
    }
    return Utils.runtimeException(() -> Float.parseFloat(currentValue.toString()));
  }

  /**
   * Returns the message property with the specified name that has been set on this {@code
   * JMSProducer}, converted to a {@code double}.
   *
   * @param name the name of the property
   * @return the property value, converted to a {@code double}
   * @throws JMSRuntimeException if the JMS provider fails to get the property value due to some
   *     internal error.
   * @throws MessageFormatRuntimeException if this type conversion is invalid.
   * @see JMSProducer#setProperty(String, double)
   */
  @Override
  public double getDoubleProperty(String name) {
    if (!properties.containsKey(name)) {
      throw new NullPointerException();
    }
    final Object currentValue = properties.getOrDefault(name, "0");
    if (currentValue instanceof Number) {
      if (!(currentValue instanceof Double) && !(currentValue instanceof Float)) {
        throw new MessageFormatRuntimeException("unsupported conversion");
      } else {
        return ((Number) currentValue).doubleValue();
      }
    }
    return Utils.runtimeException(() -> Double.parseDouble(currentValue.toString()));
  }

  /**
   * Returns the message property with the specified name that has been set on this {@code
   * JMSProducer}, converted to a {@code String}.
   *
   * @param name the name of the property
   * @return the property value, converted to a {@code boolean}; if there is no property by this
   *     name, a null value is returned
   * @throws JMSRuntimeException if the JMS provider fails to get the property value due to some
   *     internal error.
   * @throws MessageFormatRuntimeException if this type conversion is invalid.
   * @see JMSProducer#setProperty(String, String)
   */
  @Override
  public String getStringProperty(String name) {
    return Utils.runtimeException(() -> properties.getOrDefault(name, "").toString());
  }

  /**
   * Returns the message property with the specified name that has been set on this {@code
   * JMSProducer}, converted to objectified format.
   *
   * <p>This method can be used to return, in objectified format, an object that has been stored as
   * a property in the message with the equivalent {@code setObjectProperty} method call, or its
   * equivalent primitive <code>set<I>type</I>Property</code> method.
   *
   * @param name the name of the property
   * @return the Java object property value with the specified name, in objectified format (for
   *     example, if the property was set as an {@code int}, an {@code Integer} is returned); if
   *     there is no property by this name, a null value is returned
   * @throws JMSRuntimeException if the JMS provider fails to get the property value due to some
   *     internal error.
   * @see JMSProducer#setProperty(String, Object)
   */
  @Override
  public Object getObjectProperty(String name) {
    return Utils.runtimeException(() -> properties.getOrDefault(name, null));
  }

  /**
   * Returns an unmodifiable {@code Set} view of the names of all the message properties that have
   * been set on this JMSProducer.
   *
   * <p>Note that JMS standard header fields are not considered properties and are not returned in
   * this Set.
   *
   * <p>The set is backed by the {@code JMSProducer}, so changes to the map are reflected in the
   * set. However the set may not be modified. Attempts to modify the returned collection, whether
   * directly or via its iterator, will result in an {@code
   * java.lang.UnsupportedOperationException}. Its behaviour matches that defined in the {@code
   * java.util.Collections} method {@code unmodifiableSet}.
   *
   * @return a {@code Set} containing the names of all the message properties that have been set on
   *     this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to get the property names due to some
   *     internal error.
   * @see Collections#unmodifiableSet
   */
  @Override
  public Set<String> getPropertyNames() {
    return properties.keySet();
  }

  /**
   * Specifies that messages sent using this {@code JMSProducer} will have their {@code
   * JMSCorrelationID} header value set to the specified correlation ID, where correlation ID is
   * specified as an array of bytes.
   *
   * <p>This will override any {@code JMSCorrelationID} header value that is already set on the
   * message being sent.
   *
   * <p>The array is copied before the method returns, so future modifications to the array will not
   * alter the value in this {@code JMSProducer}.
   *
   * <p>If a provider supports the native concept of correlation ID, a JMS client may need to assign
   * specific {@code JMSCorrelationID} values to match those expected by native messaging clients.
   * JMS providers without native correlation ID values are not required to support this method and
   * its corresponding get method; their implementation may throw a {@code
   * java.lang.UnsupportedOperationException}.
   *
   * <p>The use of a {@code byte[]} value for {@code JMSCorrelationID} is non-portable.
   *
   * @param correlationID the correlation ID value as an array of bytes
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the correlation ID due to some
   *     internal error.
   * @see JMSProducer#setJMSCorrelationID(String)
   * @see JMSProducer#getJMSCorrelationID()
   * @see JMSProducer#getJMSCorrelationIDAsBytes()
   */
  @Override
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationID) {
    this.correlationID = correlationID;
    return this;
  }

  /**
   * Returns the {@code JMSCorrelationID} header value that has been set on this {@code
   * JMSProducer}, as an array of bytes.
   *
   * <p>The use of a {@code byte[]} value for {@code JMSCorrelationID} is non-portable.
   *
   * @return the correlation ID as an array of bytes
   * @throws JMSRuntimeException if the JMS provider fails to get the correlation ID due to some
   *     internal error.
   * @see JMSProducer#setJMSCorrelationID(String)
   * @see JMSProducer#getJMSCorrelationID()
   * @see JMSProducer#setJMSCorrelationIDAsBytes(byte[])
   */
  @Override
  @SuppressFBWarnings("EI_EXPOSE_REP")
  public byte[] getJMSCorrelationIDAsBytes() {
    return correlationID;
  }

  /**
   * Specifies that messages sent using this {@code JMSProducer} will have their {@code
   * JMSCorrelationID} header value set to the specified correlation ID, where correlation ID is
   * specified as a {@code String}.
   *
   * <p>This will override any {@code JMSCorrelationID} header value that is already set on the
   * message being sent.
   *
   * <p>A client can use the {@code JMSCorrelationID} header field to link one message with another.
   * A typical use is to link a response message with its request message.
   *
   * <p>{@code JMSCorrelationID} can hold one of the following:
   *
   * <ul>
   *   <li>A provider-specific message ID
   *   <li>An application-specific {@code String}
   *   <li>A provider-native {@code byte[]} value
   * </ul>
   *
   * <p>Since each message sent by a JMS provider is assigned a message ID value, it is convenient
   * to link messages via message ID. All message ID values must start with the {@code 'ID:'}
   * prefix.
   *
   * <p>In some cases, an application (made up of several clients) needs to use an
   * application-specific value for linking messages. For instance, an application may use {@code
   * JMSCorrelationID} to hold a value referencing some external information. Application-specified
   * values must not start with the {@code 'ID:'} prefix; this is reserved for provider-generated
   * message ID values.
   *
   * <p>If a provider supports the native concept of correlation ID, a JMS client may need to assign
   * specific {@code JMSCorrelationID} values to match those expected by clients that do not use the
   * JMS API. A {@code byte[]} value is used for this purpose. JMS providers without native
   * correlation ID values are not required to support {@code byte[]} values. The use of a {@code
   * byte[]} value for {@code JMSCorrelationID} is non-portable.
   *
   * @param correlationID the message ID of a message being referred to
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the correlation ID due to some
   *     internal error.
   * @see JMSProducer#getJMSCorrelationID()
   * @see JMSProducer#getJMSCorrelationIDAsBytes()
   * @see JMSProducer#setJMSCorrelationIDAsBytes(byte[])
   */
  @Override
  public JMSProducer setJMSCorrelationID(String correlationID) {
    this.correlationID = correlationID.getBytes(StandardCharsets.UTF_8);
    return this;
  }

  /**
   * Returns the {@code JMSCorrelationID} header value that has been set on this {@code
   * JMSProducer}, as a {@code String}.
   *
   * <p>This method is used to return correlation ID values that are either provider-specific
   * message IDs or application-specific {@code String} values.
   *
   * @return the correlation ID of a message as a {@code String}
   * @throws JMSRuntimeException if the JMS provider fails to get the correlation ID due to some
   *     internal error.
   * @see JMSProducer#setJMSCorrelationID(String)
   * @see JMSProducer#getJMSCorrelationIDAsBytes()
   * @see JMSProducer#setJMSCorrelationIDAsBytes(byte[])
   */
  @Override
  public String getJMSCorrelationID() {
    return correlationID == null ? null : new String(correlationID, StandardCharsets.UTF_8);
  }

  /**
   * Specifies that messages sent using this {@code JMSProducer} will have their {@code JMSType}
   * header value set to the specified message type.
   *
   * <p>This will override any {@code JMSType} header value that is already set on the message being
   * sent.
   *
   * <p>Some JMS providers use a message repository that contains the definitions of messages sent
   * by applications. The {@code JMSType} header field may reference a message's definition in the
   * provider's repository.
   *
   * <p>The JMS API does not define a standard message definition repository, nor does it define a
   * naming policy for the definitions it contains.
   *
   * <p>Some messaging systems require that a message type definition for each application message
   * be created and that each message specify its type. In order to work with such JMS providers,
   * JMS clients should assign a value to {@code JMSType}, whether the application makes use of it
   * or not. This ensures that the field is properly set for those providers that require it.
   *
   * <p>To ensure portability, JMS clients should use symbolic values for {@code JMSType} that can
   * be configured at installation time to the values defined in the current provider's message
   * repository. If string literals are used, they may not be valid type names for some JMS
   * providers.
   *
   * @param type the message type
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the message type due to some
   *     internal error.
   * @see JMSProducer#getJMSType()
   */
  @Override
  public JMSProducer setJMSType(String type) {
    this.jmsType = type;
    return this;
  }

  /**
   * Returns the {@code JMSType} header value that has been set on this {@code JMSProducer}.
   *
   * @return the message type
   * @throws JMSRuntimeException if the JMS provider fails to get the message type due to some
   *     internal error.
   * @see JMSProducer#setJMSType(String)
   */
  @Override
  public String getJMSType() {
    return jmsType;
  }

  /**
   * Specifies that messages sent using this {@code JMSProducer} will have their {@code JMSReplyTo}
   * header value set to the specified {@code Destination} object.
   *
   * <p>This will override any {@code JMSReplyTo} header value that is already set on the message
   * being sent.
   *
   * <p>The {@code JMSReplyTo} header field contains the destination where a reply to the current
   * message should be sent. If it is null, no reply is expected. The destination may be either a
   * {@code Queue} object or a {@code Topic} object.
   *
   * <p>Messages sent with a null {@code JMSReplyTo} value may be a notification of some event, or
   * they may just be some data the sender thinks is of interest.
   *
   * <p>Messages with a {@code JMSReplyTo} value typically expect a response. A response is
   * optional; it is up to the client to decide. These messages are called requests. A message sent
   * in response to a request is called a reply.
   *
   * <p>In some cases a client may wish to match a request it sent earlier with a reply it has just
   * received. The client can use the {@code JMSCorrelationID} header field for this purpose.
   *
   * @param replyTo {@code Destination} to which to send a response to this message
   * @return this {@code JMSProducer}
   * @throws JMSRuntimeException if the JMS provider fails to set the {@code JMSReplyTo} destination
   *     due to some internal error.
   * @see JMSProducer#getJMSReplyTo()
   */
  @Override
  public JMSProducer setJMSReplyTo(Destination replyTo) {
    this.jmsReplyTo = replyTo;
    return this;
  }

  /**
   * Returns the {@code JMSReplyTo} header value that has been set on this {@code JMSProducer}.
   *
   * @return {@code Destination} the {@code JMSReplyTo} header value
   * @throws JMSRuntimeException if the JMS provider fails to get the {@code JMSReplyTo} destination
   *     due to some internal error.
   * @see JMSProducer#setJMSReplyTo(Destination)
   */
  @Override
  public Destination getJMSReplyTo() {
    return jmsReplyTo;
  }
}
