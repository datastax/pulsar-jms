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

import java.util.concurrent.TimeUnit;
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
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

@Slf4j
public class PulsarConsumer implements MessageConsumer, TopicSubscriber, QueueReceiver {

  private final String subscriptionName;
  private final PulsarSession session;
  private final PulsarDestination destination;
  private Consumer<byte[]> consumer;
  private MessageListener listener;
  private final SubscriptionMode subscriptionMode;
  private final SubscriptionType subscriptionType;
  private volatile boolean closed;

  public PulsarConsumer(
      String subscriptionName,
      PulsarDestination destination,
      PulsarSession session,
      SubscriptionMode subscriptionMode,
      SubscriptionType subscriptionType)
      throws JMSException {
    session.checkNotClosed();
    if (destination == null) {
      throw new InvalidDestinationException("Invalid destination");
    }
    this.subscriptionName = subscriptionName;
    this.session = session;
    this.destination = destination;
    this.subscriptionMode = destination.isQueue() ? SubscriptionMode.Durable : subscriptionMode;
    this.subscriptionType = destination.isQueue() ? SubscriptionType.Shared : subscriptionType;
  }

  public PulsarConsumer subscribe() throws JMSException {
    session.registerConsumer(this);
    if (destination.isQueue()) {
      try {
        session
            .getFactory()
            .getPulsarAdmin()
            .topics()
            .createSubscription(
                destination.topicName,
                PulsarConnectionFactory.QUEUE_SHARED_SUBCRIPTION_NAME,
                MessageId.earliest);
      } catch (PulsarAdminException.ConflictException exists) {
        log.debug(
            "Subscription {} already exists for {}",
            PulsarConnectionFactory.QUEUE_SHARED_SUBCRIPTION_NAME,
            destination.topicName);
      } catch (PulsarAdminException err) {
        throw Utils.handleException(err);
      }
      // to not create eagerly the Consumer for Queues
    } else {
      getConsumer();
    }
    return this;
  }

  private Consumer<byte[]> getConsumer() throws JMSException {
    if (closed) {
      throw new IllegalStateException("Consumer is closed");
    }
    if (consumer == null) {
      consumer =
          session
              .getFactory()
              .createConsumer(
                  destination,
                  subscriptionName,
                  session.getAcknowledgeMode(),
                  subscriptionMode,
                  subscriptionType);
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
  public String getMessageSelector() throws JMSException {
    checkNotClosed();
    return null;
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
  public MessageListener getMessageListener() throws JMSException {
    checkNotClosed();
    return listener;
  }

  void checkNotClosed() throws JMSException {
    session.checkNotClosed();
    if (closed) {
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
  public void setMessageListener(MessageListener listener) throws JMSException {
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
    return receiveAndValidateType(null);
  }

  private Message receiveAndValidateType(Class expectedType) throws JMSException {
    checkNotClosed();
    if (listener != null) {
      throw new IllegalStateException("cannot receive if you have a messageListener");
    }
    return session.executeOperationIfConnectionStarted(
        () -> {
          try {
            Consumer<byte[]> consumer = getConsumer();
            org.apache.pulsar.client.api.Message<byte[]> message = consumer.receive();
            if (message == null) {
              return null;
            }
            Message res = handleReceivedMessage(message, expectedType);
            return res;
          } catch (Exception err) {
            throw Utils.handleException(err);
          }
        },
        0);
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

  private Message receiveWithTimeoutAndValidateType(long timeout, Class expectedType)
      throws JMSException {
    checkNotClosed();
    if (listener != null) {
      throw new IllegalStateException("cannot receive if you have a messageListener");
    }

    return session.executeOperationIfConnectionStarted(
        () -> {
          try {
            Consumer<byte[]> consumer = getConsumer();
            org.apache.pulsar.client.api.Message<byte[]> message =
                consumer.receive((int) timeout, TimeUnit.MILLISECONDS);
            if (message == null) {
              return null;
            }
            Message res = handleReceivedMessage(message, expectedType);
            return res;
          } catch (Exception err) {
            throw Utils.handleException(err);
          }
        },
        (int) timeout);
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

  private PulsarMessage handleReceivedMessage(
      org.apache.pulsar.client.api.Message<byte[]> message, Class expectedType)
      throws JMSException, org.apache.pulsar.client.api.PulsarClientException {
    PulsarMessage result = PulsarMessage.decode(this, message);
    Consumer<byte[]> consumer = getConsumer();
    if (expectedType != null && !result.isBodyAssignableTo(expectedType)) {
      log.info(
          "negativeAcknowledge for message {} that cannot be converted to {}",
          message,
          expectedType);
      consumer.negativeAcknowledge(message);
      throw new MessageFormatException(
          "The message ("
              + result.messageType()
              + ","
              + result
              + ",) cannot be converted to a "
              + expectedType);
    }
    if (session.getTransacted()) {
      Utils.get(consumer.acknowledgeAsync(message.getMessageId(), session.getTransaction()));
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
    } else if (session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE) {
      session.registerUnacknowledgedMessage(result);
    }
    return result;
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
    Utils.checkNotOnListener(session);
    if (closed) {
      return;
    }
    closed = true;
    if (consumer == null) {
      return;
    }
    session.executeCriticalOperation(
        () -> {
          try {
            consumer.close();
            session.removeConsumer(consumer);
            return null;
          } catch (Exception err) {
            throw Utils.handleException(err);
          }
        });
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
  public Topic getTopic() throws JMSException {
    checkNotClosed();
    if (destination.isTopic()) {
      return (Topic) destination;
    }
    throw new JMSException("This consumer has been created on a Queue");
  }

  @Override
  public Queue getQueue() throws JMSException {
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
  public boolean getNoLocal() throws JMSException {
    return false;
  }

  public JMSConsumer asJMSConsumer() {
    return new JMSConsumer() {
      @Override
      public String getMessageSelector() {
        return Utils.runtimeException(() -> PulsarConsumer.this.getMessageSelector());
      }

      @Override
      public MessageListener getMessageListener() throws JMSRuntimeException {
        return Utils.runtimeException(() -> PulsarConsumer.this.getMessageListener());
      }

      @Override
      public void setMessageListener(MessageListener listener) throws JMSRuntimeException {
        Utils.runtimeException(() -> PulsarConsumer.this.setMessageListener(listener));
      }

      @Override
      public Message receive() {
        return Utils.runtimeException(() -> PulsarConsumer.this.receive());
      }

      @Override
      public Message receive(long timeout) {
        return Utils.runtimeException(() -> PulsarConsumer.this.receive(timeout));
      }

      @Override
      public Message receiveNoWait() {
        return Utils.runtimeException(() -> PulsarConsumer.this.receiveNoWait());
      }

      @Override
      public void close() {
        Utils.runtimeException(() -> PulsarConsumer.this.close());
      }

      @Override
      public <T> T receiveBody(Class<T> c) {
        return Utils.runtimeException(
            () -> {
              Message msg = PulsarConsumer.this.receiveAndValidateType(c);
              return msg == null ? null : msg.getBody(c);
            });
      }

      @Override
      public <T> T receiveBody(Class<T> c, long timeout) {
        return Utils.runtimeException(
            () -> {
              Message msg = PulsarConsumer.this.receiveWithTimeoutAndValidateType(timeout, c);
              return msg == null ? null : msg.getBody(c);
            });
      }

      @Override
      public <T> T receiveBodyNoWait(Class<T> c) {
        return Utils.runtimeException(
            () -> {
              Message msg = PulsarConsumer.this.receiveWithTimeoutAndValidateType(1, c);
              return msg == null ? null : msg.getBody(c);
            });
      }
    };
  }

  void acknowledge(org.apache.pulsar.client.api.Message<byte[]> receivedPulsarMessage)
      throws JMSException {
    Consumer<byte[]> consumer = getConsumer();
    try {
      consumer.acknowledge(receivedPulsarMessage);
    } catch (PulsarClientException err) {
      throw Utils.handleException(err);
    }
  }

  void runListener(int timeout) {
    if (closed || listener == null) {
      return;
    }
    if (listener != null) {
      // activate checks about methods that cannot be called inside a listener
      // and block any concurrent "close()" operations
      Utils.executeListenerInSessionContext(
          session,
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
              PulsarMessage pulsarMessage = handleReceivedMessage(message, null);
              if (message != null) {
                listener.onMessage(pulsarMessage);
              }
            } catch (PulsarClientException.AlreadyClosedException closed) {
              log.error("Error while receiving message con Closed consumer {}", this);
            } catch (JMSException | PulsarClientException err) {
              log.error("Error while receiving message con consumer {}", this, err);
              session.onError(err);
            }
          });
    }
  }

  public void closeInternal() throws JMSException {
    if (closed) {
      return;
    }
    closed = true;
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
}
