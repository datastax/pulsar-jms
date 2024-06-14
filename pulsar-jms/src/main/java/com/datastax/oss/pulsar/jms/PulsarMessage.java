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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.EOFException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.schema.KeyValue;

@Slf4j
public abstract class PulsarMessage implements Message {

  /* JMS reserves the "JMSX" property name prefix for
   * the following JMS defined properties.
   */
  static final String[] JMSX_PROVIDER_IDENTIFIERS = {
    "JMSXUserID",
    "JMSXAppID",
    "JMSXDeliveryCount",
    "JMSXProducerTXID",
    "JMSXConsumerTXID",
    "JMSXRcvTimestamp",
    "JMSXState"
  };

  /*
   * Names reserved for message selectors. These are case-insensitive.
   */
  static final String[] RESERVED = {
    "NULL", "TRUE", "FALSE", "NOT", "AND", "OR", "BETWEEN", "LIKE", "IN", "IS", "ESCAPE"
  };

  private volatile String messageId;
  protected boolean writable = true;
  private volatile long jmsTimestamp;
  private byte[] correlationId;
  private Destination jmsReplyTo;
  private Destination destination;
  private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
  private String jmsType;
  private boolean jmsRedelivered;
  private volatile long jmsExpiration;
  private volatile long jmsDeliveryTime;
  private int jmsPriority = Message.DEFAULT_PRIORITY;

  protected final Map<String, String> properties = new HashMap<>();
  private PulsarMessageConsumer consumer;
  private Consumer<?> pulsarConsumer;
  private boolean negativeAcked;
  private org.apache.pulsar.client.api.Message<?> receivedPulsarMessage;

  /**
   * Gets the message ID.
   *
   * <p>The {@code JMSMessageID} header field contains a value that uniquely identifies each message
   * sent by a provider.
   *
   * <p>When a message is sent, {@code JMSMessageID} can be ignored. When the {@code send} or {@code
   * publish} method returns, it contains a provider-assigned value.
   *
   * <p>A {@code JMSMessageID} is a {@code String} value that should function as a unique key for
   * identifying messages in a historical repository. The exact scope of uniqueness is
   * provider-defined. It should at least cover all messages for a specific installation of a
   * provider, where an installation is some connected set of message routers.
   *
   * <p>All {@code JMSMessageID} values must start with the prefix {@code 'ID:'}. Uniqueness of
   * message ID values across different providers is not required.
   *
   * <p>Since message IDs take some effort to create and increase a message's size, some JMS
   * providers may be able to optimize message overhead if they are given a hint that the message ID
   * is not used by an application. By calling the {@code MessageProducer.setDisableMessageID}
   * method, a JMS client enables this potential optimization for all messages sent by that message
   * producer. If the JMS provider accepts this hint, these messages must have the message ID set to
   * null; if the provider ignores the hint, the message ID must be set to its normal unique value.
   *
   * @return the message ID
   * @throws JMSException if the JMS provider fails to get the message ID due to some internal
   *     error.
   * @see Message#setJMSMessageID(String)
   * @see MessageProducer#setDisableMessageID(boolean)
   */
  @Override
  public String getJMSMessageID() throws JMSException {
    return messageId;
  }

  /**
   * Sets the message ID.
   *
   * <p>This method is for use by JMS providers only to set this field when a message is sent. This
   * message cannot be used by clients to configure the message ID. This method is public to allow a
   * JMS provider to set this field when sending a message whose implementation is not its own.
   *
   * @param id the ID of the message
   * @throws JMSException if the JMS provider fails to set the message ID due to some internal
   *     error.
   * @see Message#getJMSMessageID()
   */
  @Override
  public void setJMSMessageID(String id) throws JMSException {
    this.messageId = id;
  }

  /**
   * Gets the message timestamp.
   *
   * <p>The {@code JMSTimestamp} header field contains the time a message was handed off to a
   * provider to be sent. It is not the time the message was actually transmitted, because the
   * actual send may occur later due to transactions or other client-side queueing of messages.
   *
   * <p>When a message is sent, {@code JMSTimestamp} is ignored. When the {@code send} or {@code
   * publish} method returns, it contains a time value somewhere in the interval between the call
   * and the return. The value is in the format of a normal millis time value in the Java
   * programming language.
   *
   * <p>Since timestamps take some effort to create and increase a message's size, some JMS
   * providers may be able to optimize message overhead if they are given a hint that the timestamp
   * is not used by an application. By calling the {@code
   * MessageProducer.setDisableMessageTimestamp} method, a JMS client enables this potential
   * optimization for all messages sent by that message producer. If the JMS provider accepts this
   * hint, these messages must have the timestamp set to zero; if the provider ignores the hint, the
   * timestamp must be set to its normal value.
   *
   * @return the message timestamp
   * @throws JMSException if the JMS provider fails to get the timestamp due to some internal error.
   * @see Message#setJMSTimestamp(long)
   * @see MessageProducer#setDisableMessageTimestamp(boolean)
   */
  @Override
  public long getJMSTimestamp() throws JMSException {
    return jmsTimestamp;
  }

  /**
   * Sets the message timestamp.
   *
   * <p>This method is for use by JMS providers only to set this field when a message is sent. This
   * message cannot be used by clients to configure the message timestamp. This method is public to
   * allow a JMS provider to set this field when sending a message whose implementation is not its
   * own.
   *
   * @param timestamp the timestamp for this message
   * @throws JMSException if the JMS provider fails to set the timestamp due to some internal error.
   * @see Message#getJMSTimestamp()
   */
  @Override
  public void setJMSTimestamp(long timestamp) throws JMSException {
    this.jmsTimestamp = timestamp;
  }

  /**
   * Gets the correlation ID as an array of bytes for the message.
   *
   * <p>The use of a {@code byte[]} value for {@code JMSCorrelationID} is non-portable.
   *
   * @return the correlation ID of a message as an array of bytes
   * @throws JMSException if the JMS provider fails to get the correlation ID due to some internal
   *     error.
   * @see Message#setJMSCorrelationID(String)
   * @see Message#getJMSCorrelationID()
   * @see Message#setJMSCorrelationIDAsBytes(byte[])
   */
  @Override
  @SuppressFBWarnings("EI_EXPOSE_REP")
  public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
    return correlationId;
  }

  /**
   * Sets the correlation ID as an array of bytes for the message.
   *
   * <p>The array is copied before the method returns, so future modifications to the array will not
   * alter this message header.
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
   * @throws JMSException if the JMS provider fails to set the correlation ID due to some internal
   *     error.
   * @see Message#setJMSCorrelationID(String)
   * @see Message#getJMSCorrelationID()
   * @see Message#getJMSCorrelationIDAsBytes()
   */
  @Override
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
    this.correlationId = correlationID;
  }

  /**
   * Sets the correlation ID for the message.
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
   * @throws JMSException if the JMS provider fails to set the correlation ID due to some internal
   *     error.
   * @see Message#getJMSCorrelationID()
   * @see Message#getJMSCorrelationIDAsBytes()
   * @see Message#setJMSCorrelationIDAsBytes(byte[])
   */
  @Override
  public void setJMSCorrelationID(String correlationID) throws JMSException {
    if (correlationID != null) {
      this.correlationId = correlationID.getBytes(StandardCharsets.UTF_8);
    } else {
      this.correlationId = null;
    }
  }

  /**
   * Gets the correlation ID for the message.
   *
   * <p>This method is used to return correlation ID values that are either provider-specific
   * message IDs or application-specific {@code String} values.
   *
   * @return the correlation ID of a message as a {@code String}
   * @throws JMSException if the JMS provider fails to get the correlation ID due to some internal
   *     error.
   * @see Message#setJMSCorrelationID(String)
   * @see Message#getJMSCorrelationIDAsBytes()
   * @see Message#setJMSCorrelationIDAsBytes(byte[])
   */
  @Override
  public String getJMSCorrelationID() throws JMSException {
    return correlationId != null ? new String(correlationId, StandardCharsets.UTF_8) : null;
  }

  /**
   * Gets the {@code Destination} object to which a reply to this message should be sent.
   *
   * @return {@code Destination} to which to send a response to this message
   * @throws JMSException if the JMS provider fails to get the {@code JMSReplyTo} destination due to
   *     some internal error.
   * @see Message#setJMSReplyTo(Destination)
   */
  @Override
  public Destination getJMSReplyTo() throws JMSException {
    return jmsReplyTo;
  }

  /**
   * Sets the {@code Destination} object to which a reply to this message should be sent.
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
   * @throws JMSException if the JMS provider fails to set the {@code JMSReplyTo} destination due to
   *     some internal error.
   * @see Message#getJMSReplyTo()
   */
  @Override
  public void setJMSReplyTo(Destination replyTo) throws JMSException {
    this.jmsReplyTo = replyTo;
  }

  /**
   * Gets the {@code Destination} object for this message.
   *
   * <p>The {@code JMSDestination} header field contains the destination to which the message is
   * being sent.
   *
   * <p>When a message is sent, this field is ignored. After completion of the {@code send} or
   * {@code publish} method, the field holds the destination specified by the method.
   *
   * <p>When a message is received, its {@code JMSDestination} value must be equivalent to the value
   * assigned when it was sent.
   *
   * @return the destination of this message
   * @throws JMSException if the JMS provider fails to get the destination due to some internal
   *     error.
   * @see Message#setJMSDestination(Destination)
   */
  @Override
  public Destination getJMSDestination() throws JMSException {
    return destination;
  }

  /**
   * Sets the {@code Destination} object for this message.
   *
   * <p>This method is for use by JMS providers only to set this field when a message is sent. This
   * message cannot be used by clients to configure the destination of the message. This method is
   * public to allow a JMS provider to set this field when sending a message whose implementation is
   * not its own.
   *
   * @param destination the destination for this message
   * @throws JMSException if the JMS provider fails to set the destination due to some internal
   *     error.
   * @see Message#getJMSDestination()
   */
  @Override
  public void setJMSDestination(Destination destination) throws JMSException {
    this.destination = destination;
  }

  /**
   * Gets the {@code DeliveryMode} value specified for this message.
   *
   * @return the delivery mode for this message
   * @throws JMSException if the JMS provider fails to get the delivery mode due to some internal
   *     error.
   * @see Message#setJMSDeliveryMode(int)
   * @see DeliveryMode
   */
  @Override
  public int getJMSDeliveryMode() throws JMSException {
    return deliveryMode;
  }

  /**
   * Sets the {@code DeliveryMode} value for this message.
   *
   * <p>This method is for use by JMS providers only to set this field when a message is sent. This
   * message cannot be used by clients to configure the delivery mode of the message. This method is
   * public to allow a JMS provider to set this field when sending a message whose implementation is
   * not its own.
   *
   * @param deliveryMode the delivery mode for this message
   * @throws JMSException if the JMS provider fails to set the delivery mode due to some internal
   *     error.
   * @see Message#getJMSDeliveryMode()
   * @see DeliveryMode
   */
  @Override
  public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
    this.deliveryMode = deliveryMode;
  }

  /**
   * Gets an indication of whether this message is being redelivered.
   *
   * <p>If a client receives a message with the {@code JMSRedelivered} field set, it is likely, but
   * not guaranteed, that this message was delivered earlier but that its receipt was not
   * acknowledged at that time.
   *
   * @return true if this message is being redelivered
   * @throws JMSException if the JMS provider fails to get the redelivered state due to some
   *     internal error.
   * @see Message#setJMSRedelivered(boolean)
   */
  @Override
  public boolean getJMSRedelivered() throws JMSException {
    return jmsRedelivered;
  }

  /**
   * Specifies whether this message is being redelivered.
   *
   * <p>This method is for use by JMS providers only to set this field when a message is delivered.
   * This message cannot be used by clients to configure the redelivered status of the message. This
   * method is public to allow a JMS provider to set this field when sending a message whose
   * implementation is not its own.
   *
   * @param redelivered an indication of whether this message is being redelivered
   * @throws JMSException if the JMS provider fails to set the redelivered state due to some
   *     internal error.
   * @see Message#getJMSRedelivered()
   */
  @Override
  public void setJMSRedelivered(boolean redelivered) throws JMSException {
    this.jmsRedelivered = redelivered;
  }

  /**
   * Gets the message type identifier supplied by the client when the message was sent.
   *
   * @return the message type
   * @throws JMSException if the JMS provider fails to get the message type due to some internal
   *     error.
   * @see Message#setJMSType(String)
   */
  @Override
  public String getJMSType() throws JMSException {
    return jmsType;
  }

  /**
   * Sets the message type.
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
   * @throws JMSException if the JMS provider fails to set the message type due to some internal
   *     error.
   * @see Message#getJMSType()
   */
  @Override
  public void setJMSType(String type) throws JMSException {
    this.jmsType = type;
  }

  /**
   * Gets the message's expiration time.
   *
   * <p>When a message is sent, the {@code JMSExpiration} header field is left unassigned. After
   * completion of the {@code send} or {@code publish} method, it holds the expiration time of the
   * message. This is the the difference, measured in milliseconds, between the expiration time and
   * midnight, January 1, 1970 UTC.
   *
   * <p>If the time-to-live is specified as zero, {@code JMSExpiration} is set to zero to indicate
   * that the message does not expire.
   *
   * <p>When a message's expiration time is reached, a provider should discard it. The JMS API does
   * not define any form of notification of message expiration.
   *
   * <p>Clients should not receive messages that have expired; however, the JMS API does not
   * guarantee that this will not happen.
   *
   * @return the message's expiration time value
   * @throws JMSException if the JMS provider fails to get the message expiration due to some
   *     internal error.
   * @see Message#setJMSExpiration(long)
   */
  @Override
  public long getJMSExpiration() throws JMSException {
    return jmsExpiration;
  }

  /**
   * Sets the message's expiration value.
   *
   * <p>This method is for use by JMS providers only to set this field when a message is sent. This
   * message cannot be used by clients to configure the expiration time of the message. This method
   * is public to allow a JMS provider to set this field when sending a message whose implementation
   * is not its own.
   *
   * @param expiration the message's expiration time
   * @throws JMSException if the JMS provider fails to set the message expiration due to some
   *     internal error.
   * @see Message#getJMSExpiration()
   */
  @Override
  public void setJMSExpiration(long expiration) throws JMSException {
    this.jmsExpiration = expiration;
  }

  /**
   * Gets the message's delivery time value.
   *
   * <p>When a message is sent, the {@code JMSDeliveryTime} header field is left unassigned. After
   * completion of the {@code send} or {@code publish} method, it holds the delivery time of the
   * message. This is the the difference, measured in milliseconds, between the delivery time and
   * midnight, January 1, 1970 UTC.
   *
   * <p>A message's delivery time is the earliest time when a JMS provider may deliver the message
   * to a consumer. The provider must not deliver messages before the delivery time has been
   * reached.
   *
   * @return the message's delivery time value
   * @throws JMSException if the JMS provider fails to get the delivery time due to some internal
   *     error.
   * @see Message#setJMSDeliveryTime(long)
   * @since JMS 2.0
   */
  @Override
  public long getJMSDeliveryTime() throws JMSException {
    return jmsDeliveryTime;
  }

  /**
   * Sets the message's delivery time value.
   *
   * <p>This method is for use by JMS providers only to set this field when a message is sent. This
   * message cannot be used by clients to configure the delivery time of the message. This method is
   * public to allow a JMS provider to set this field when sending a message whose implementation is
   * not its own.
   *
   * @param deliveryTime the message's delivery time value
   * @throws JMSException if the JMS provider fails to set the delivery time due to some internal
   *     error.
   * @see Message#getJMSDeliveryTime()
   * @since JMS 2.0
   */
  @Override
  public void setJMSDeliveryTime(long deliveryTime) throws JMSException {
    this.jmsDeliveryTime = deliveryTime;
  }

  /**
   * Gets the message priority level.
   *
   * <p>The JMS API defines ten levels of priority value, with 0 as the lowest priority and 9 as the
   * highest. In addition, clients should consider priorities 0-4 as gradations of normal priority
   * and priorities 5-9 as gradations of expedited priority.
   *
   * <p>The JMS API does not require that a provider strictly implement priority ordering of
   * messages; however, it should do its best to deliver expedited messages ahead of normal
   * messages.
   *
   * @return the default message priority
   * @throws JMSException if the JMS provider fails to get the message priority due to some internal
   *     error.
   * @see Message#setJMSPriority(int)
   */
  @Override
  public int getJMSPriority() throws JMSException {
    return jmsPriority;
  }

  /**
   * Sets the priority level for this message.
   *
   * <p>This method is for use by JMS providers only to set this field when a message is sent. This
   * message cannot be used by clients to configure the priority level of the message. This method
   * is public to allow a JMS provider to set this field when sending a message whose implementation
   * is not its own.
   *
   * @param priority the priority of this message
   * @throws JMSException if the JMS provider fails to set the message priority due to some internal
   *     error.
   * @see Message#getJMSPriority()
   */
  @Override
  public void setJMSPriority(int priority) throws JMSException {
    this.jmsPriority = priority;
  }

  /**
   * Clears a message's properties.
   *
   * <p>The message's header fields and body are not cleared.
   *
   * @throws JMSException if the JMS provider fails to clear the message properties due to some
   *     internal error.
   */
  @Override
  public void clearProperties() throws JMSException {
    properties.clear();
  }

  /**
   * Indicates whether a property value exists.
   *
   * @param name the name of the property to test
   * @return true if the property exists
   * @throws JMSException if the JMS provider fails to determine if the property exists due to some
   *     internal error.
   */
  @Override
  public boolean propertyExists(String name) throws JMSException {
    return properties.containsKey(name);
  }

  /**
   * Returns the value of the {@code boolean} property with the specified name.
   *
   * @param name the name of the {@code boolean} property
   * @return the {@code boolean} property value for the specified name
   * @throws JMSException if the JMS provider fails to get the property value due to some internal
   *     error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public boolean getBooleanProperty(String name) throws JMSException {
    Object value = getObjectProperty(name);
    if (value == null) {
      return false;
    }
    if (value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof String) {
      return Boolean.parseBoolean(value.toString());
    } else {
      throw new MessageFormatException("Unsupported conversion to boolean for " + value);
    }
  }

  /**
   * Returns the value of the {@code byte} property with the specified name.
   *
   * @param name the name of the {@code byte} property
   * @return the {@code byte} property value for the specified name
   * @throws JMSException if the JMS provider fails to get the property value due to some internal
   *     error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public byte getByteProperty(String name) throws JMSException {
    Object value = getObjectProperty(name);
    if (value == null) {
      throw new NumberFormatException("null not allowed");
    }
    if (value instanceof Byte) {
      return ((Number) value).byteValue();
    } else if (value instanceof String) {
      return Utils.invoke(() -> Byte.parseByte(value.toString()));
    } else {
      throw new MessageFormatException("Unsupported conversion");
    }
  }

  /**
   * Returns the value of the {@code short} property with the specified name.
   *
   * @param name the name of the {@code short} property
   * @return the {@code short} property value for the specified name
   * @throws JMSException if the JMS provider fails to get the property value due to some internal
   *     error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public short getShortProperty(String name) throws JMSException {
    Object value = getObjectProperty(name);
    if (value == null) {
      throw new NumberFormatException("null not allowed");
    }
    if ((value instanceof Byte) || (value instanceof Short)) {
      return ((Number) value).shortValue();
    } else if (value instanceof String) {
      return Utils.invoke(() -> Short.parseShort(value.toString()));
    } else {
      throw new MessageFormatException("Unsupported conversion");
    }
  }

  /**
   * Returns the value of the {@code int} property with the specified name.
   *
   * @param name the name of the {@code int} property
   * @return the {@code int} property value for the specified name
   * @throws JMSException if the JMS provider fails to get the property value due to some internal
   *     error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public int getIntProperty(String name) throws JMSException {
    Object value = getObjectProperty(name);
    if (value == null) {
      throw new NumberFormatException("null not allowed");
    }
    if ((value instanceof Byte) || (value instanceof Short) || (value instanceof Integer)) {
      return ((Number) value).intValue();
    } else if (value instanceof String) {
      return Utils.invoke(() -> Integer.parseInt(value.toString()));
    } else {
      throw new MessageFormatException("Unsupported conversion");
    }
  }

  /**
   * Returns the value of the {@code long} property with the specified name.
   *
   * @param name the name of the {@code long} property
   * @return the {@code long} property value for the specified name
   * @throws JMSException if the JMS provider fails to get the property value due to some internal
   *     error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public long getLongProperty(String name) throws JMSException {
    Object value = getObjectProperty(name);
    if (value == null) {
      throw new NumberFormatException("null not allowed");
    }
    if ((value instanceof Byte)
        || (value instanceof Short)
        || (value instanceof Integer)
        || (value instanceof Long)) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      return Utils.invoke(() -> Long.parseLong(value.toString()));
    } else {
      throw new MessageFormatException("Unsupported conversion");
    }
  }

  /**
   * Returns the value of the {@code float} property with the specified name.
   *
   * @param name the name of the {@code float} property
   * @return the {@code float} property value for the specified name
   * @throws JMSException if the JMS provider fails to get the property value due to some internal
   *     error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public float getFloatProperty(String name) throws JMSException {
    Object value = getObjectProperty(name);
    if (value == null) {
      throw new NumberFormatException("null not allowed");
    }
    if (value instanceof Float) {
      return ((Number) value).floatValue();
    } else if (value instanceof String) {
      return Utils.invoke(() -> Float.parseFloat(value.toString()));
    } else {
      throw new MessageFormatException("Unsuppported");
    }
  }

  /**
   * Returns the value of the {@code double} property with the specified name.
   *
   * @param name the name of the {@code double} property
   * @return the {@code double} property value for the specified name
   * @throws JMSException if the JMS provider fails to get the property value due to some internal
   *     error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public double getDoubleProperty(String name) throws JMSException {
    Object value = getObjectProperty(name);
    if (value == null) {
      throw new NumberFormatException("null not allowed");
    }
    if ((value instanceof Float) || (value instanceof Double)) {
      return ((Number) value).doubleValue();
    } else if (value instanceof String) {
      return Utils.invoke(() -> Double.parseDouble(value.toString()));
    } else {
      throw new MessageFormatException("Unsuppported");
    }
  }

  /**
   * Returns the value of the {@code String} property with the specified name.
   *
   * @param name the name of the {@code String} property
   * @return the {@code String} property value for the specified name; if there is no property by
   *     this name, a null value is returned
   * @throws JMSException if the JMS provider fails to get the property value due to some internal
   *     error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public String getStringProperty(String name) throws JMSException {
    return Utils.invoke(() -> properties.getOrDefault(name, null));
  }

  /**
   * Returns the value of the Java object property with the specified name.
   *
   * <p>This method can be used to return, in objectified format, an object that has been stored as
   * a property in the message with the equivalent <code>setObjectProperty</code> method call, or
   * its equivalent primitive <code>set<I>type</I>Property</code> method.
   *
   * @param name the name of the Java object property
   * @return the Java object property value with the specified name, in objectified format (for
   *     example, if the property was set as an {@code int}, an {@code Integer} is returned); if
   *     there is no property by this name, a null value is returned
   * @throws JMSException if the JMS provider fails to get the property value due to some internal
   *     error.
   */
  @Override
  public Object getObjectProperty(String name) throws JMSException {
    return Utils.invoke(
        () -> {
          String value = properties.getOrDefault(name, null);
          if (value == null) {
            return null;
          }
          String type = properties.getOrDefault(propertyType(name), "string");
          switch (type) {
            case "string":
              return value;
            case "boolean":
              return Boolean.parseBoolean(value);
            case "float":
              return Float.parseFloat(value);
            case "double":
              return Double.parseDouble(value);
            case "int":
              return Integer.parseInt(value);
            case "short":
              return Short.parseShort(value);
            case "byte":
              return Byte.parseByte(value);
            case "long":
              return Long.parseLong(value);
            default:
              // string
              return value;
          }
        });
  }

  /**
   * Returns an {@code Enumeration} of all the property names.
   *
   * <p>Note that JMS standard header fields are not considered properties and are not returned in
   * this enumeration.
   *
   * @return an enumeration of all the names of property values
   * @throws JMSException if the JMS provider fails to get the property names due to some internal
   *     error.
   */
  @Override
  public Enumeration getPropertyNames() throws JMSException {
    return Collections.enumeration(
        properties
            .keySet()
            .stream()
            .filter(n -> !n.endsWith("_jsmtype"))
            .filter(n -> !n.startsWith("JMS"))
            .collect(Collectors.toList()));
  }

  /**
   * Sets a {@code boolean} property value with the specified name into the message.
   *
   * @param name the name of the {@code boolean} property
   * @param value the {@code boolean} property value to set
   * @throws JMSException if the JMS provider fails to set the property due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if properties are read-only
   */
  @Override
  public void setBooleanProperty(String name, boolean value) throws JMSException {
    checkWritableProperty(name);
    properties.put(name, Boolean.toString(value));
    properties.put(propertyType(name), "boolean");
  }

  private static String propertyType(String name) {
    return name + "_jsmtype";
  }

  /**
   * Sets a {@code byte} property value with the specified name into the message.
   *
   * @param name the name of the {@code byte} property
   * @param value the {@code byte} property value to set
   * @throws JMSException if the JMS provider fails to set the property due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if properties are read-only
   */
  @Override
  public void setByteProperty(String name, byte value) throws JMSException {
    checkWritableProperty(name);
    properties.put(name, Byte.toString(value));
    properties.put(propertyType(name), "byte");
  }

  /**
   * Sets a {@code short} property value with the specified name into the message.
   *
   * @param name the name of the {@code short} property
   * @param value the {@code short} property value to set
   * @throws JMSException if the JMS provider fails to set the property due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if properties are read-only
   */
  @Override
  public void setShortProperty(String name, short value) throws JMSException {
    checkWritableProperty(name);
    properties.put(name, Short.toString(value));
    properties.put(propertyType(name), "short");
  }

  /**
   * Sets an {@code int} property value with the specified name into the message.
   *
   * @param name the name of the {@code int} property
   * @param value the {@code int} property value to set
   * @throws JMSException if the JMS provider fails to set the property due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if properties are read-only
   */
  @Override
  public void setIntProperty(String name, int value) throws JMSException {
    checkWritableProperty(name);
    properties.put(name, Integer.toString(value));
    properties.put(propertyType(name), "int");
  }

  /**
   * Sets a {@code long} property value with the specified name into the message.
   *
   * @param name the name of the {@code long} property
   * @param value the {@code long} property value to set
   * @throws JMSException if the JMS provider fails to set the property due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if properties are read-only
   */
  @Override
  public void setLongProperty(String name, long value) throws JMSException {
    checkWritableProperty(name);
    properties.put(name, Long.toString(value));
    properties.put(propertyType(name), "long");
  }

  /**
   * Sets a {@code float} property value with the specified name into the message.
   *
   * @param name the name of the {@code float} property
   * @param value the {@code float} property value to set
   * @throws JMSException if the JMS provider fails to set the property due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if properties are read-only
   */
  @Override
  public void setFloatProperty(String name, float value) throws JMSException {
    checkWritableProperty(name);
    properties.put(name, Float.toString(value));
    properties.put(propertyType(name), "float");
  }

  /**
   * Sets a {@code double} property value with the specified name into the message.
   *
   * @param name the name of the {@code double} property
   * @param value the {@code double} property value to set
   * @throws JMSException if the JMS provider fails to set the property due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if properties are read-only
   */
  @Override
  public void setDoubleProperty(String name, double value) throws JMSException {
    checkWritableProperty(name);
    properties.put(name, Double.toString(value));
    properties.put(propertyType(name), "double");
  }

  /**
   * Sets a {@code String} property value with the specified name into the message.
   *
   * @param name the name of the {@code String} property
   * @param value the {@code String} property value to set
   * @throws JMSException if the JMS provider fails to set the property due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if properties are read-only
   */
  @Override
  public void setStringProperty(String name, String value) throws JMSException {
    checkWritableProperty(name);
    properties.put(name, value);
    properties.put(propertyType(name), "string");
  }

  /**
   * Sets a Java object property value with the specified name into the message.
   *
   * <p>Note that this method works only for the objectified primitive object types ({@code
   * Integer}, {@code Double}, {@code Long} ...) and {@code String} objects.
   *
   * @param name the name of the Java object property
   * @param value the Java object property value to set
   * @throws JMSException if the JMS provider fails to set the property due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageFormatException if the object is invalid
   * @throws MessageNotWriteableException if properties are read-only
   */
  @Override
  public void setObjectProperty(String name, Object value) throws JMSException {
    checkWritableProperty(name);
    if (value != null) {
      if (value instanceof String) {
        setStringProperty(name, (String) value);
      } else if (value instanceof Boolean) {
        setBooleanProperty(name, (Boolean) value);
      } else if (value instanceof Integer) {
        setIntProperty(name, (Integer) value);
      } else if (value instanceof Long) {
        setLongProperty(name, (Long) value);
      } else if (value instanceof Float) {
        setFloatProperty(name, (Float) value);
      } else if (value instanceof Short) {
        setShortProperty(name, (Short) value);
      } else if (value instanceof Double) {
        setDoubleProperty(name, (Double) value);
      } else if (value instanceof Byte) {
        setByteProperty(name, (Byte) value);
      } else {
        throw new MessageFormatException("Invalid property type " + value.getClass());
      }
    } else {
      properties.put(propertyType(name), "null");
      properties.put(name, null);
    }
  }

  /**
   * Acknowledges all consumed messages of the session of this consumed message.
   *
   * <p>All consumed JMS messages support the {@code acknowledge} method for use when a client has
   * specified that its JMS session's consumed messages are to be explicitly acknowledged. By
   * invoking {@code acknowledge} on a consumed message, a client acknowledges all messages consumed
   * by the session that the message was delivered to.
   *
   * <p>Calls to {@code acknowledge} are ignored for both transacted sessions and sessions specified
   * to use implicit acknowledgement modes.
   *
   * <p>A client may individually acknowledge each message as it is consumed, or it may choose to
   * acknowledge messages as an application-defined group (which is done by calling acknowledge on
   * the last received message of the group, thereby acknowledging all messages consumed by the
   * session.)
   *
   * <p>Messages that have been received but not acknowledged may be redelivered.
   *
   * @throws JMSException if the JMS provider fails to acknowledge the messages due to some internal
   *     error.
   * @throws IllegalStateException if this method is called on a closed session.
   * @see Session#CLIENT_ACKNOWLEDGE
   */
  @Override
  public void acknowledge() throws JMSException {
    consumer.checkNotClosed();
    if (consumer.getSession().getAcknowledgeMode() == PulsarJMSConstants.INDIVIDUAL_ACKNOWLEDGE) {
      acknowledgeInternal();
    } else {
      consumer.getSession().acknowledgeAllMessages();
    }
  }

  void acknowledgeInternal() throws JMSException {
    if (pulsarConsumer == null) {
      throw new IllegalStateException("not received by a consumer");
    }
    if (negativeAcked) {
      return;
    }
    try {
      if (consumer != null) {
        consumer.acknowledge(receivedPulsarMessage, this, pulsarConsumer);
      }
    } catch (Exception err) {
      throw Utils.handleException(err);
    }
  }

  boolean isNegativeAcked() {
    return negativeAcked;
  }

  public void negativeAck() {
    if (consumer == null) {
      throw new IllegalStateRuntimeException("not received by a consumer");
    }

    try {
      consumer.checkNotClosed();
    } catch (JMSException err) {
      // ignore
      log.error("Cannot nAck message {}", this);
      return;
    }

    consumer.negativeAck(receivedPulsarMessage);
    negativeAcked = true;
  }

  protected final void checkWritable() throws MessageNotWriteableException {
    if (!writable) throw new MessageNotWriteableException("not writable");
  }

  protected final void checkReadable() throws MessageNotReadableException {
    if (writable) throw new MessageNotReadableException("not readable");
  }

  protected final void checkWritableProperty(String name) throws JMSException {
    if (!writable) {
      throw new MessageNotWriteableException("Not writeable");
    } else if (!isValidIdentifier(name)) {
      throw new IllegalArgumentException("Invalid map key " + name);
    }
  }

  /**
   * Property names must obey the rules for a message selector identifier.
   *
   * <p>An identifier is an unlimited-length character sequence that must begin with a Java
   * identifier start character; all following characters must be Java identifier part characters.
   *
   * @param name
   * @return Section 3.8 “Message selection” for more information.
   */
  protected final boolean isValidIdentifier(String name) {
    if (StringUtils.isBlank(name)
        || isJmsProperty(name)
        || isJmsxProviderProperty(name)
        || isReservedProperty(name)
        || !Character.isJavaIdentifierStart(name.charAt(0))) {
      return false;
    }

    for (char b : name.substring(1).toCharArray()) {
      if (!Character.isJavaIdentifierPart(b) && b != '.') {
        return false;
      }
    }

    return true;
  }

  /**
   * JMS reserves the "JMS_ " property name prefix.
   *
   * @param name
   * @return
   */
  protected final boolean isJmsProperty(String name) {
    return name.startsWith("JMS_");
  }

  protected final boolean isJmsxProviderProperty(String name) {
    return (name != null)
        && Arrays.stream(JMSX_PROVIDER_IDENTIFIERS).anyMatch(id -> id.equals(name));
  }

  protected final boolean isReservedProperty(String name) {
    return (name != null) && Arrays.stream(RESERVED).anyMatch(id -> id.equalsIgnoreCase(name));
  }

  protected abstract String messageType();

  final void sendAsync(
      TypedMessageBuilder<byte[]> message,
      CompletionListener completionListener,
      PulsarSession session,
      PulsarMessageProducer pulsarProducer,
      boolean disableMessageTimestamp)
      throws JMSException {
    prepareForSend(message);
    fillSystemPropertiesBeforeSend(message, disableMessageTimestamp, session);

    message
        .sendAsync()
        .whenComplete(
            (messageIdFromServer, error) -> {
              Utils.executeCompletionListenerInSessionContext(
                  session,
                  pulsarProducer,
                  () -> {
                    this.writable = false;
                    if (error != null) {
                      completionListener.onException(this, Utils.handleException(error));
                    } else {
                      assignSystemMessageId(messageIdFromServer);

                      completionListener.onCompletion(this);
                    }
                  });
            });
  }

  private void fillSystemPropertiesBeforeSend(
      TypedMessageBuilder<byte[]> message, boolean disableMessageTimestamp, PulsarSession session)
      throws MessageNotWriteableException {
    //    if (!writable) {
    //      throw new MessageNotWriteableException("Message is not writable");
    //    }
    //   is this required only by JMS 2 ?
    //    if (consumer != null) {
    //      throw new MessageNotWriteableException(
    //          "Message is not writable because consumer is not null");
    //    }
    consumer = null;
    message.properties(properties);
    // useful for deserialization
    message.property("JMSPulsarMessageType", messageType());

    if (messageId != null) {
      message.property("JMSMessageId", messageId);
    }

    if (jmsReplyTo != null) {
      // here we want to keep the original name passed by the user
      // if we have the subscription name in the form Queue:Subscription
      // then here we want to keep the Subscription name
      message.property(
          "JMSReplyTo",
          session.getFactory().applySystemNamespace(((PulsarDestination) jmsReplyTo).topicName));
      if (((PulsarDestination) jmsReplyTo).isTopic()) {
        message.property("JMSReplyToType", "topic");
      }
    }
    if (jmsType != null) {
      message.property("JMSType", jmsType);
    }
    if (correlationId != null) {
      message.property("JMSCorrelationID", Base64.getEncoder().encodeToString(correlationId));
    }
    if (deliveryMode != DeliveryMode.PERSISTENT) {
      message.property("JMSDeliveryMode", deliveryMode + "");
    }
    if (jmsPriority != Message.DEFAULT_PRIORITY) {
      message.property("JMSPriority", jmsPriority + "");
    }

    this.jmsTimestamp = System.currentTimeMillis();

    if (!disableMessageTimestamp) {
      message.eventTime(jmsTimestamp);
    }
    this.jmsDeliveryTime = jmsTimestamp;
    if (jmsDeliveryTime == 0) {
      this.jmsDeliveryTime = System.currentTimeMillis();
    }

    message.property("JMSDeliveryTime", jmsDeliveryTime + "");

    long stickyKey = session.getTransactionStickyKey();
    if (stickyKey > 0) {
      message.property("JMSTX", Long.toString(stickyKey));
    }

    // we can use JMSXGroupID as key in order to provide
    // a behaviour similar to https://activemq.apache.org/message-groups
    String JMSXGroupID = properties.get("JMSXGroupID");
    if (JMSXGroupID != null) {
      message.key(JMSXGroupID);
    }
  }

  final void send(
      TypedMessageBuilder<byte[]> producer, boolean disableMessageTimestamp, PulsarSession session)
      throws JMSException {
    prepareForSend(producer);
    fillSystemPropertiesBeforeSend(producer, disableMessageTimestamp, session);

    MessageId messageIdFromServer = Utils.invoke(() -> producer.send());
    assignSystemMessageId(messageIdFromServer);
  }

  protected abstract void prepareForSend(TypedMessageBuilder<byte[]> producer) throws JMSException;

  static PulsarMessage decode(
      PulsarMessageConsumer consumer,
      Consumer<?> pulsarConsumer,
      org.apache.pulsar.client.api.Message<?> msg)
      throws JMSException {
    if (msg == null) {
      return null;
    }
    Object value = msg.getValue();
    if (value instanceof byte[] || value == null) {
      String type = msg.getProperty("JMSPulsarMessageType");
      if (type == null) {
        type = "bytes"; // non JMS clients
      }
      byte[] valueAsArray = (byte[]) value;
      switch (type) {
        case "map":
          return new PulsarMapMessage(valueAsArray).applyMessage(msg, consumer, pulsarConsumer);
        case "object":
          return new PulsarObjectMessage(valueAsArray).applyMessage(msg, consumer, pulsarConsumer);
        case "stream":
          return new PulsarStreamMessage(valueAsArray).applyMessage(msg, consumer, pulsarConsumer);
        case "bytes":
          return new PulsarBytesMessage(valueAsArray).applyMessage(msg, consumer, pulsarConsumer);
        case "text":
          return new PulsarTextMessage(valueAsArray).applyMessage(msg, consumer, pulsarConsumer);
        default:
          return new PulsarSimpleMessage().applyMessage(msg, consumer, pulsarConsumer);
      }
    } else if (value instanceof GenericObject) {
      GenericObject genericObject = (GenericObject) value;
      Object nativeObject = genericObject.getNativeObject();
      Object unwrapped = unwrapNativeObject(nativeObject);
      if (unwrapped instanceof String) {
        return new PulsarTextMessage((String) unwrapped)
            .applyMessage(msg, consumer, pulsarConsumer);
      } else if (unwrapped instanceof Map) {
        return new PulsarMapMessage((Map) unwrapped, false)
            .applyMessage(msg, consumer, pulsarConsumer);
      } else {
        return new PulsarObjectMessage((Serializable) unwrapped)
            .applyMessage(msg, consumer, pulsarConsumer);
      }
    } else {
      throw new IllegalStateException("Cannot decode message, payload type is " + value.getClass());
    }
  }

  private static Object unwrapNativeObject(Object nativeObject) {
    if (nativeObject instanceof KeyValue) {
      KeyValue keyValue = (KeyValue) nativeObject;
      Object keyPart = unwrapNativeObject(keyValue.getKey());
      Object valuePart = unwrapNativeObject(keyValue.getValue());
      Map<String, Object> result = new HashMap<>();
      result.put("key", keyPart);
      result.put("value", valuePart);
      return result;
    }
    if (nativeObject instanceof GenericObject) {
      return unwrapNativeObject(((GenericObject) nativeObject).getNativeObject());
    }
    if (nativeObject instanceof GenericRecord) {
      return genericRecordToMap((GenericRecord) nativeObject);
    }
    if (nativeObject instanceof GenericArray) {
      return genericArrayToList((GenericArray) nativeObject);
    }
    if (nativeObject instanceof Utf8) {
      return nativeObject.toString();
    }
    return nativeObject;
  }

  private static List<Object> genericArrayToList(GenericArray genericArray) {
    List<Object> res = new ArrayList<>();
    genericArray.forEach(
        fieldValue -> {
          res.add(unwrapNativeObject(fieldValue));
        });
    return res;
  }

  private static Map<String, Object> genericRecordToMap(GenericRecord genericRecord) {
    Map<String, Object> asMap = new HashMap<>();
    genericRecord
        .getSchema()
        .getFields()
        .forEach(
            f -> {
              Object fieldValue = unwrapNativeObject(genericRecord.get(f.name()));
              asMap.put(f.name(), fieldValue);
            });
    return asMap;
  }

  protected PulsarMessage applyMessage(
      org.apache.pulsar.client.api.Message<?> msg,
      PulsarMessageConsumer consumer,
      Consumer<?> pulsarConsumer) {
    this.writable = false;
    this.properties.putAll(msg.getProperties());
    if (consumer != null) {
      this.destination = consumer.getDestination();
    }
    String jmsReplyTo = msg.getProperty("JMSReplyTo");
    if (jmsReplyTo != null) {
      String jmsReplyToType = msg.getProperty("JMSReplyToType") + "";
      switch (jmsReplyToType) {
        case "topic":
          this.jmsReplyTo = new PulsarTopic(jmsReplyTo);
          break;
        default:
          this.jmsReplyTo = new PulsarQueue(jmsReplyTo);
      }
    }
    if (msg.hasProperty("JMSType")) {
      this.jmsType = msg.getProperty("JMSType");
    }

    if (msg.hasProperty("JMSMessageId")) {
      this.messageId = msg.getProperty("JMSMessageId");
    }
    assignSystemMessageId(msg.getMessageId());

    if (msg.hasProperty("JMSCorrelationID")) {
      this.correlationId = Base64.getDecoder().decode(msg.getProperty("JMSCorrelationID"));
    }
    if (msg.hasProperty("JMSPriority")) {
      try {
        this.jmsPriority = Integer.parseInt(msg.getProperty("JMSPriority"));
      } catch (NumberFormatException err) {
        // cannot decode priority, not a big deal as it is not supported in Pulsar
      }
    }
    if (msg.hasProperty("JMSDeliveryMode")) {
      try {
        this.deliveryMode = Integer.parseInt(msg.getProperty("JMSDeliveryMode"));
      } catch (NumberFormatException err) {
        // cannot decode deliveryMode, not a big deal as it is not supported in Pulsar
      }
    }
    if (msg.hasProperty("JMSExpiration")) {
      try {
        this.jmsExpiration = Long.parseLong(msg.getProperty("JMSExpiration"));
      } catch (NumberFormatException err) {
        // cannot decode JMSExpiration
      }
    }
    // this is optional
    this.jmsTimestamp = msg.getEventTime();

    this.jmsDeliveryTime = jmsTimestamp;
    if (msg.hasProperty("JMSDeliveryTime")) {
      try {
        this.jmsDeliveryTime = Long.parseLong(msg.getProperty("JMSDeliveryTime"));
      } catch (NumberFormatException err) {
        // cannot decode JMSDeliveryTime
      }
    }

    this.properties.put("JMSXDeliveryCount", (msg.getRedeliveryCount() + 1) + "");
    if (msg.getKey() != null) {
      this.properties.put("JMSXGroupID", msg.getKey());
    } else {
      this.properties.put("JMSXGroupID", "");
    }
    if (!properties.containsKey("JMSXGroupSeq")) {
      this.properties.put("JMSXGroupSeq", msg.getSequenceId() + "");
    }

    this.jmsRedelivered = msg.getRedeliveryCount() > 0;
    this.receivedPulsarMessage = msg;
    this.consumer = consumer;
    this.pulsarConsumer = pulsarConsumer;
    return this;
  }

  private void assignSystemMessageId(org.apache.pulsar.client.api.MessageId msgId) {
    if (this.messageId == null) {
      this.messageId = "ID:" + msgId; // MessageId toString is not bad
    }
  }

  protected static void validateWritableObject(Object value) throws MessageFormatException {
    if (value == null) {
      return;
    }
    if (value instanceof Integer) {
    } else if (value instanceof String) {
    } else if (value instanceof Short) {
    } else if (value instanceof Long) {
    } else if (value instanceof Double) {
    } else if (value instanceof Float) {
    } else if (value instanceof Boolean) {
    } else if (value instanceof Byte) {
    } else if (value instanceof Character) {
    } else if (value instanceof byte[]) {
    } else {
      throw new MessageFormatException("Unsupported type " + value.getClass());
    }
  }

  public boolean isReceivedFromConsumer(PulsarMessageConsumer consumer) {
    return this.consumer == consumer;
  }

  protected static JMSException handleExceptionAccordingToMessageSpecs(Throwable t)
      throws JMSException {
    if (t instanceof NumberFormatException) {
      // TCK
      throw (NumberFormatException) t;
    }
    if (t instanceof EOFException) {
      throw new MessageEOFException(t + "");
    }
    throw Utils.handleException(t);
  }

  public void setWritable(boolean b) {
    writable = b;
  }

  public CompletableFuture<?> acknowledgeInternalInTransaction(Transaction transaction) {
    return consumer
        .getInternalConsumer()
        .acknowledgeAsync(receivedPulsarMessage.getMessageId(), transaction);
  }

  public org.apache.pulsar.client.api.Message<?> getReceivedPulsarMessage() {
    return receivedPulsarMessage;
  }
}
