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

import java.io.Serializable;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.InvalidClientIDRuntimeException;
import javax.jms.InvalidDestinationRuntimeException;
import javax.jms.InvalidSelectorRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TransactionRolledBackRuntimeException;

public class PulsarJMSContext implements JMSContext {
  private final PulsarConnection connection;
  final PulsarSession session;
  private boolean autoStart = true;
  private boolean owningConnection;

  PulsarJMSContext(
      PulsarConnectionFactory factory,
      int sessionMode,
      boolean anonymous,
      String connectUsername,
      String connectPassword) {
    try {
      if (anonymous) {
        this.connection = factory.createConnection();
      } else {
        this.connection = factory.createConnection(connectUsername, connectPassword);
      }
      this.session = connection.createSession(sessionMode);
      this.session.setJms20(true);
      this.connection.setAllowSetClientId(true);
      this.owningConnection = true;
    } catch (JMSException err) {
      JMSRuntimeException jms = new JMSRuntimeException("Error while creating JMSContext");
      jms.initCause(err);
      throw jms;
    }
  }

  PulsarJMSContext(
      final PulsarConnection connection,
      int sessionMode,
      ConsumerConfiguration overrideConsumerConfiguration) {
    try {
      this.owningConnection = false;
      this.connection = connection;
      this.session =
          connection.createSession(
              sessionMode == Session.SESSION_TRANSACTED,
              sessionMode,
              overrideConsumerConfiguration);
      this.session.setJms20(true);
    } catch (JMSException err) {
      JMSRuntimeException jms = new JMSRuntimeException("error");
      jms.initCause(err);
      throw jms;
    }
  }

  /**
   * Creates a new {@code JMSContext} with the specified session mode using the same connection as
   * this {@code JMSContext} and creating a new session.
   *
   * <p>This method does not start the connection. If the connection has not already been started
   * then it will be automatically started when a {@code JMSConsumer} is created on any of the
   * {@code JMSContext} objects for that connection.
   *
   * <ul>
   *   <li>If {@code sessionMode} is set to {@code JMSContext.SESSION_TRANSACTED} then the session
   *       will use a local transaction which may subsequently be committed or rolled back by
   *       calling the {@code JMSContext}'s {@code commit} or {@code rollback} methods.
   *   <li>If {@code sessionMode} is set to any of {@code JMSContext.CLIENT_ACKNOWLEDGE}, {@code
   *       JMSContext.AUTO_ACKNOWLEDGE} or {@code JMSContext.DUPS_OK_ACKNOWLEDGE}. then the session
   *       will be non-transacted and messages received by this session will be acknowledged
   *       according to the value of {@code sessionMode}. For a definition of the meaning of these
   *       acknowledgement modes see the links below.
   * </ul>
   *
   * <p>This method must not be used by applications running in the Java EE web or EJB containers
   * because doing so would violate the restriction that such an application must not attempt to
   * create more than one active (not closed) {@code Session} object per connection. If this method
   * is called in a Java EE web or EJB container then a {@code JMSRuntimeException} will be thrown.
   *
   * @param sessionMode indicates which of four possible session modes will be used. The permitted
   *     values are {@code JMSContext.SESSION_TRANSACTED}, {@code JMSContext.CLIENT_ACKNOWLEDGE},
   *     {@code JMSContext.AUTO_ACKNOWLEDGE} and {@code JMSContext.DUPS_OK_ACKNOWLEDGE}.
   * @return a newly created JMSContext
   * @throws JMSRuntimeException if the JMS provider fails to create the JMSContext due to
   *     <ul>
   *       <li>some internal error or
   *       <li>because this method is being called in a Java EE web or EJB application.
   *     </ul>
   *
   * @see JMSContext#SESSION_TRANSACTED
   * @see JMSContext#CLIENT_ACKNOWLEDGE
   * @see JMSContext#AUTO_ACKNOWLEDGE
   * @see JMSContext#DUPS_OK_ACKNOWLEDGE
   * @see ConnectionFactory#createContext()
   * @see ConnectionFactory#createContext(int)
   * @see ConnectionFactory#createContext(String, String)
   * @see ConnectionFactory#createContext(String, String, int)
   * @see JMSContext#createContext(int)
   * @since JMS 2.0
   */
  @Override
  public JMSContext createContext(int sessionMode) {
    return new PulsarJMSContext(connection, sessionMode, null);
  }

  /**
   * Build a new JMSContext that shares the same Connection, as {@link #createContext(int)}. But you
   * can override some configuration settings, like "consumerConfig"
   *
   * @param sessionMode
   * @param customConfiguration
   * @return a new JMS Context
   */
  public JMSContext createContext(int sessionMode, Map<String, Object> customConfiguration) {
    return new PulsarJMSContext(
        connection,
        sessionMode,
        ConsumerConfiguration.buildConsumerConfiguration(
            customConfiguration != null
                ? (Map<String, Object>) customConfiguration.get("consumerConfig")
                : null));
  }

  /**
   * Creates a new {@code JMSProducer} object which can be used to configure and send messages
   *
   * @return A new {@code JMSProducer} object
   * @see JMSProducer
   */
  @Override
  public JMSProducer createProducer() {
    return new PulsarJMSProducer(this);
  }

  /**
   * Gets the client identifier for the JMSContext's connection.
   *
   * <p>This value is specific to the JMS provider. It is either preconfigured by an administrator
   * in a {@code ConnectionFactory} object or assigned dynamically by the application by calling the
   * {@code setClientID} method.
   *
   * @return the unique client identifier
   * @throws JMSRuntimeException if the JMS provider fails to return the client ID for the
   *     JMSContext's connection due to some internal error.
   */
  @Override
  public String getClientID() {
    return Utils.runtimeException(() -> connection.getClientID());
  }

  /**
   * Sets the client identifier for the JMSContext's connection.
   *
   * <p>The preferred way to assign a JMS client's client identifier is for it to be configured in a
   * client-specific {@code ConnectionFactory} object and transparently assigned to the {@code
   * Connection} object it creates.
   *
   * <p>Alternatively, a client can set the client identifier for the JMSContext's connection using
   * a provider-specific value. The facility to set its client identifier explicitly is not a
   * mechanism for overriding the identifier that has been administratively configured. It is
   * provided for the case where no administratively specified identifier exists. If one does exist,
   * an attempt to change it by setting it must throw an {@code IllegalStateRuntimeException}. If a
   * client sets the client identifier explicitly, it must do so immediately after it creates the
   * JMSContext and before any other action on the JMSContext is taken. After this point, setting
   * the client identifier is a programming error that should throw an {@code
   * IllegalStateRuntimeException}.
   *
   * <p>The purpose of the client identifier is to associate the JMSContext's connection and its
   * objects with a state maintained on behalf of the client by a provider. The only such state
   * identified by the JMS API is that required to support durable subscriptions.
   *
   * <p>If another connection with the same {@code clientID} is already running when this method is
   * called, the JMS provider should detect the duplicate ID and throw an {@code
   * InvalidClientIDException}.
   *
   * <p>This method must not be used in a Java EE web or EJB application. Doing so may cause a
   * {@code JMSRuntimeException} to be thrown though this is not guaranteed.
   *
   * <p>This method must not be used if the {@code JMSContext} is container-managed (injected).
   * Doing so will cause a {@code IllegalStateRuntimeException} to be thrown.
   *
   * @param clientID the unique client identifier
   * @throws InvalidClientIDRuntimeException if the JMS client specifies an invalid or duplicate
   *     client ID.
   * @throws IllegalStateRuntimeException
   *     <ul>
   *       <li>if the JMS client attempts to set the client ID for the JMSContext's connection at
   *           the wrong time
   *       <li>if the client ID has been administratively configured or
   *       <li>if the {@code JMSContext} is container-managed (injected).
   *     </ul>
   *
   * @throws JMSRuntimeException if the JMS provider fails to set the client ID for the the
   *     JMSContext's connection for one of the following reasons:
   *     <ul>
   *       <li>an internal error has occurred or
   *       <li>this method has been called in a Java EE web or EJB application (though it is not
   *           guaranteed that an exception is thrown in this case)
   *     </ul>
   */
  @Override
  public void setClientID(String clientID) {
    Utils.runtimeException(() -> this.connection.setClientID(clientID));
  }

  /**
   * Gets the connection metadata for the JMSContext's connection.
   *
   * @return the connection metadata
   * @throws JMSRuntimeException if the JMS provider fails to get the connection metadata
   * @see ConnectionMetaData
   */
  @Override
  public ConnectionMetaData getMetaData() {
    return Utils.runtimeException(() -> this.connection.getMetaData());
  }

  /**
   * Gets the {@code ExceptionListener} object for the JMSContext's connection. Not every {@code
   * Connection} has an {@code ExceptionListener} associated with it.
   *
   * @return the {@code ExceptionListener} for the JMSContext's connection, or null if no {@code
   *     ExceptionListener} is associated with that connection.
   * @throws JMSRuntimeException if the JMS provider fails to get the {@code ExceptionListener} for
   *     the JMSContext's connection.
   * @see Connection#setExceptionListener
   */
  @Override
  public ExceptionListener getExceptionListener() {
    return Utils.runtimeException(() -> connection.getExceptionListener());
  }

  /**
   * Sets an exception listener for the JMSContext's connection.
   *
   * <p>If a JMS provider detects a serious problem with a connection, it informs the connection's
   * {@code ExceptionListener}, if one has been registered. It does this by calling the listener's
   * {@code onException} method, passing it a {@code JMSRuntimeException} object describing the
   * problem.
   *
   * <p>An exception listener allows a client to be notified of a problem asynchronously. Some
   * connections only consume messages, so they would have no other way to learn their connection
   * has failed.
   *
   * <p>A connection serializes execution of its {@code ExceptionListener}.
   *
   * <p>A JMS provider should attempt to resolve connection problems itself before it notifies the
   * client of them.
   *
   * <p>This method must not be used in a Java EE web or EJB application. Doing so may cause a
   * {@code JMSRuntimeException} to be thrown though this is not guaranteed.
   *
   * <p>This method must not be used if the {@code JMSContext} is container-managed (injected).
   * Doing so will cause a {@code IllegalStateRuntimeException} to be thrown.
   *
   * @param listener the exception listener
   * @throws IllegalStateRuntimeException if the {@code JMSContext} is container-managed (injected).
   * @throws JMSRuntimeException if the JMS provider fails to set the exception listener for one of
   *     the following reasons:
   *     <ul>
   *       <li>an internal error has occurred or
   *       <li>this method has been called in a Java EE web or EJB application (though it is not
   *           guaranteed that an exception is thrown in this case)
   *     </ul>
   */
  @Override
  public void setExceptionListener(ExceptionListener listener) {
    Utils.runtimeException(() -> connection.setExceptionListener(listener));
  }

  /**
   * Starts (or restarts) delivery of incoming messages by the JMSContext's connection. A call to
   * {@code start} on a connection that has already been started is ignored.
   *
   * <p>This method must not be used if the {@code JMSContext} is container-managed (injected).
   * Doing so will cause a {@code IllegalStateRuntimeException} to be thrown.
   *
   * @throws IllegalStateRuntimeException if the {@code JMSContext} is container-managed (injected).
   * @throws JMSRuntimeException if the JMS provider fails to start message delivery due to some
   *     internal error.
   * @see JMSContext#stop
   */
  @Override
  public void start() {
    Utils.runtimeException(() -> connection.start());
  }

  /**
   * Temporarily stops the delivery of incoming messages by the JMSContext's connection. Delivery
   * can be restarted using the {@code start} method. When the connection is stopped, delivery to
   * all the connection's message consumers is inhibited: synchronous receives block, and messages
   * are not delivered to message listeners.
   *
   * <p>Stopping a connection has no effect on its ability to send messages. A call to {@code stop}
   * on a connection that has already been stopped is ignored.
   *
   * <p>A call to {@code stop} must not return until delivery of messages has paused. This means
   * that a client can rely on the fact that none of its message listeners will be called and that
   * all threads of control waiting for {@code receive} calls to return will not return with a
   * message until the connection is restarted. The receive timers for a stopped connection continue
   * to advance, so receives may time out while the connection is stopped.
   *
   * <p>If message listeners are running when {@code stop} is invoked, the {@code stop} call must
   * wait until all of them have returned before it may return. While these message listeners are
   * completing, they must have the full services of the connection available to them.
   *
   * <p>However if the stop method is called from a message listener on its own {@code JMSContext},
   * or any other {@code JMSContext} that uses the same connection, then it will either fail and
   * throw a {@code javax.jms.IllegalStateRuntimeException}, or it will succeed and stop the
   * connection, blocking until all other message listeners that may have been running have
   * returned.
   *
   * <p>Since two alternative behaviors are permitted in this case, applications should avoid
   * calling {@code stop} from a message listener on its own {@code JMSContext}, or any other {@code
   * JMSContext} that uses the same connection, because this is not portable.
   *
   * <p>For the avoidance of doubt, if an exception listener for the JMSContext's connection is
   * running when {@code stop} is invoked, there is no requirement for the {@code stop} call to wait
   * until the exception listener has returned before it may return.
   *
   * <p>This method must not be used in a Java EE web or EJB application. Doing so may cause a
   * {@code JMSRuntimeException} to be thrown though this is not guaranteed.
   *
   * <p>This method must not be used if the {@code JMSContext} is container-managed (injected).
   * Doing so will cause a {@code IllegalStateRuntimeException} to be thrown.
   *
   * @throws IllegalStateRuntimeException
   *     <ul>
   *       <li>if this method has been called by a <tt>MessageListener</tt> on its own
   *           <tt>JMSContext</tt>
   *       <li>if the {@code JMSContext} is container-managed (injected).
   *     </ul>
   *
   * @throws JMSRuntimeException if the JMS provider fails to stop message delivery for one of the
   *     following reasons:
   *     <ul>
   *       <li>an internal error has occurred or
   *       <li>this method has been called in a Java EE web or EJB application (though it is not
   *           guaranteed that an exception is thrown in this case)
   *     </ul>
   *
   * @see JMSContext#start
   */
  @Override
  public void stop() {
    Utils.runtimeException(
        () -> {
          Utils.checkNotOnMessageListener(session);
          connection.stop();
        });
  }

  /**
   * Specifies whether the underlying connection used by this {@code JMSContext} will be started
   * automatically when a consumer is created. This is the default behaviour, and it may be disabled
   * by calling this method with a value of {@code false}.
   *
   * <p>This method does not itself either start or stop the connection.
   *
   * <p>This method must not be used if the {@code JMSContext} is container-managed (injected).
   * Doing so will cause a {@code IllegalStateRuntimeException} to be thrown.
   *
   * @param autoStart Whether the underlying connection used by this {@code JMSContext} will be
   *     automatically started when a consumer is created.
   * @throws IllegalStateRuntimeException if the {@code JMSContext} is container-managed (injected)
   * @see JMSContext#getAutoStart
   */
  @Override
  public void setAutoStart(boolean autoStart) {
    this.autoStart = autoStart;
  }

  /**
   * Returns whether the underlying connection used by this {@code JMSContext} will be started
   * automatically when a consumer is created.
   *
   * @return whether the underlying connection used by this {@code JMSContext} will be started
   *     automatically when a consumer is created.
   * @see JMSContext#setAutoStart
   */
  @Override
  public boolean getAutoStart() {
    return autoStart;
  }

  /**
   * Closes the JMSContext
   *
   * <p>This closes the underlying session and any underlying producers and consumers. If there are
   * no other active (not closed) JMSContext objects using the underlying connection then this
   * method also closes the underlying connection.
   *
   * <p>Since a provider typically allocates significant resources outside the JVM on behalf of a
   * connection, clients should close these resources when they are not needed. Relying on garbage
   * collection to eventually reclaim these resources may not be timely enough.
   *
   * <p>Closing a connection causes all temporary destinations to be deleted.
   *
   * <p>When this method is invoked, it should not return until message processing has been shut
   * down in an orderly fashion. This means that all message listeners that may have been running
   * have returned, and that all pending receives have returned. A close terminates all pending
   * message receives on the connection's sessions' consumers. The receives may return with a
   * message or with null, depending on whether there was a message available at the time of the
   * close. If one or more of the connection's sessions' message listeners is processing a message
   * at the time when connection {@code close} is invoked, all the facilities of the connection and
   * its sessions must remain available to those listeners until they return control to the JMS
   * provider.
   *
   * <p>However if the close method is called from a message listener on its own {@code JMSContext},
   * then it will either fail and throw a {@code javax.jms.IllegalStateRuntimeException}, or it will
   * succeed and close the {@code JMSContext}. If {@code close} succeeds and the session mode of the
   * {@code JMSContext} is set to {@code AUTO_ACKNOWLEDGE}, the current message will still be
   * acknowledged automatically when the onMessage call completes.
   *
   * <p>Since two alternative behaviors are permitted in this case, applications should avoid
   * calling close from a message listener on its own {@code JMSContext} because this is not
   * portable.
   *
   * <p>This method must not return until any incomplete asynchronous send operations for this
   * <tt>JMSContext</tt> have been completed and any <tt>CompletionListener</tt> callbacks have
   * returned. Incomplete sends should be allowed to complete normally unless an error occurs.
   *
   * <p>For the avoidance of doubt, if an exception listener for the JMSContext's connection is
   * running when {@code close} is invoked, there is no requirement for the {@code close} call to
   * wait until the exception listener has returned before it may return.
   *
   * <p>Closing a connection causes any of its sessions' transactions in progress to be rolled back.
   * In the case where a session's work is coordinated by an external transaction manager, a
   * session's {@code commit} and {@code rollback} methods are not used and the result of a closed
   * session's work is determined later by the transaction manager.
   *
   * <p>Closing a connection does NOT force an acknowledgment of client-acknowledged sessions.
   *
   * <p>Invoking the {@code acknowledge} method of a received message from a closed connection's
   * session must throw an {@code IllegalStateRuntimeException}. Closing a closed connection must
   * NOT throw an exception.
   *
   * <p>A <tt>CompletionListener</tt> callback method must not call <tt>close</tt> on its own
   * <tt>JMSContext</tt>. Doing so will cause an <tt>IllegalStateRuntimeException</tt> to be thrown.
   *
   * <p>This method must not be used if the {@code JMSContext} is container-managed (injected).
   * Doing so will cause a {@code IllegalStateRuntimeException} to be thrown.
   *
   * @throws IllegalStateRuntimeException
   *     <ul>
   *       <li>if this method has been called by a <tt>MessageListener </tt> on its own
   *           <tt>JMSContext</tt>
   *       <li>if this method has been called by a <tt>CompletionListener</tt> callback method on
   *           its own <tt>JMSContext</tt>
   *       <li>if the {@code JMSContext} is container-managed (injected)
   *     </ul>
   *
   * @throws JMSRuntimeException if the JMS provider fails to close the {@code JMSContext} due to
   *     some internal error. For example, a failure to release resources or to close a socket
   *     connection can cause this exception to be thrown.
   */
  @Override
  public void close() {
    Utils.runtimeException(
        () -> {
          try {
            session.close();
          } finally {
            if (owningConnection) {
              connection.close();
            }
          }
        });
  }

  /**
   * Creates a {@code BytesMessage} object. A {@code BytesMessage} object is used to send a message
   * containing a stream of uninterpreted bytes.
   *
   * @return The created {@code BytesMessage} object
   * @throws JMSRuntimeException if the JMS provider fails to create this message due to some
   *     internal error.
   */
  @Override
  public BytesMessage createBytesMessage() {
    return Utils.runtimeException(session::createBytesMessage);
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
   * @return The created {@code MapMessage} object.
   * @throws JMSRuntimeException if the JMS provider fails to create this message due to some
   *     internal error.
   */
  @Override
  public MapMessage createMapMessage() {
    return Utils.runtimeException(() -> session.createMapMessage());
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
   * @return The created {@code Message} object.
   * @throws JMSRuntimeException if the JMS provider fails to create this message due to some
   *     internal error.
   */
  @Override
  public Message createMessage() {
    return Utils.runtimeException(session::createMessage);
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
   * @return The created {@code ObjectMessage} object.
   * @throws JMSRuntimeException if the JMS provider fails to create this message due to some
   *     internal error.
   */
  @Override
  public ObjectMessage createObjectMessage() {
    return Utils.runtimeException(() -> session.createObjectMessage());
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
   * @return The created {@code ObjectMessage} object.
   * @throws JMSRuntimeException if the JMS provider fails to create this message due to some
   *     internal error.
   */
  @Override
  public ObjectMessage createObjectMessage(Serializable object) {
    return Utils.runtimeException(() -> session.createObjectMessage(object));
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
   * @return The created {@code StreamMessage} object.
   * @throws JMSRuntimeException if the JMS provider fails to create this message due to some
   *     internal error.
   */
  @Override
  public StreamMessage createStreamMessage() {
    return Utils.runtimeException(() -> session.createStreamMessage());
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
   * @return The created {@code TextMessage} object.
   * @throws JMSRuntimeException if the JMS provider fails to create this message due to some
   *     internal error.
   */
  @Override
  public TextMessage createTextMessage() {
    return Utils.runtimeException(() -> session.createTextMessage());
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
   * @return The created {@code TextMessage} object.
   * @throws JMSRuntimeException if the JMS provider fails to create this message due to some
   *     internal error.
   */
  @Override
  public TextMessage createTextMessage(String text) {
    return Utils.runtimeException(() -> session.createTextMessage(text));
  }

  /**
   * Indicates whether the JMSContext's session is in transacted mode.
   *
   * @return true if the session is in transacted mode
   * @throws JMSRuntimeException if the JMS provider fails to return the transaction mode due to
   *     some internal error.
   */
  @Override
  public boolean getTransacted() {
    return Utils.runtimeException(() -> session.getTransacted());
  }

  /**
   * Returns the session mode of the JMSContext's session. This can be set at the time that the
   * JMSContext is created. Possible values are JMSContext.SESSION_TRANSACTED,
   * JMSContext.AUTO_ACKNOWLEDGE, JMSContext.CLIENT_ACKNOWLEDGE and JMSContext.DUPS_OK_ACKNOWLEDGE
   *
   * <p>If a session mode was not specified when the JMSContext was created a value of
   * JMSContext.AUTO_ACKNOWLEDGE will be returned.
   *
   * @return the session mode of the JMSContext's session
   * @throws JMSRuntimeException if the JMS provider fails to return the acknowledgment mode due to
   *     some internal error.
   * @see Connection#createSession
   * @since JMS 2.0
   */
  @Override
  public int getSessionMode() {
    return Utils.runtimeException(session::getAcknowledgeMode);
  }

  /**
   * Commits all messages done in this transaction and releases any locks currently held.
   *
   * <p>This method must not return until any incomplete asynchronous send operations for this
   * <tt>JMSContext</tt> have been completed and any <tt>CompletionListener</tt> callbacks have
   * returned. Incomplete sends should be allowed to complete normally unless an error occurs.
   *
   * <p>A <tt>CompletionListener</tt> callback method must not call <tt>commit</tt> on its own
   * <tt>JMSContext</tt>. Doing so will cause an <tt>IllegalStateRuntimeException</tt> to be thrown.
   *
   * <p>This method must not be used if the {@code JMSContext} is container-managed (injected).
   * Doing so will cause a {@code IllegalStateRuntimeException} to be thrown.
   *
   * @throws IllegalStateRuntimeException
   *     <ul>
   *       <li>if the <tt>JMSContext</tt>'s session is not using a local transaction
   *       <li>if this method has been called by a <tt> CompletionListener</tt> callback method on
   *           its own <tt> JMSContext</tt>
   *       <li>if the {@code JMSContext} is container-managed (injected)
   *     </ul>
   *
   * @throws TransactionRolledBackRuntimeException if the transaction is rolled back due to some
   *     internal error during commit.
   * @throws JMSRuntimeException if the JMS provider fails to commit the transaction due to some
   *     internal error
   */
  @Override
  public void commit() {
    Utils.runtimeException(
        () -> {
          session.commit();
        });
  }

  /**
   * Rolls back any messages done in this transaction and releases any locks currently held.
   *
   * <p>This method must not return until any incomplete asynchronous send operations for this
   * <tt>JMSContext</tt> have been completed and any <tt>CompletionListener</tt> callbacks have
   * returned. Incomplete sends should be allowed to complete normally unless an error occurs.
   *
   * <p>A <tt>CompletionListener</tt> callback method must not call <tt>rollback</tt> on its own
   * <tt>JMSContext</tt>. Doing so will cause an <tt>IllegalStateRuntimeException</tt> to be thrown.
   *
   * <p>This method must not be used if the {@code JMSContext} is container-managed (injected).
   * Doing so will cause a {@code IllegalStateRuntimeException} to be thrown.
   *
   * @throws IllegalStateRuntimeException
   *     <ul>
   *       <li>if the <tt>JMSContext</tt>'s session is not using a local transaction
   *       <li>if this method has been called by a <tt>CompletionListener</tt> callback method on
   *           its own <tt>JMSContext</tt>
   *       <li>if the {@code JMSContext} is container-managed (injected)
   *     </ul>
   *
   * @throws JMSRuntimeException if the JMS provider fails to roll back the transaction due to some
   *     internal error
   */
  @Override
  public void rollback() {
    Utils.runtimeException(() -> session.rollback());
  }

  /**
   * Stops message delivery in the JMSContext's session, and restarts message delivery with the
   * oldest unacknowledged message.
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
   * <p>This method must not be used if the {@code JMSContext} is container-managed (injected).
   * Doing so will cause a {@code IllegalStateRuntimeException} to be thrown.
   *
   * @throws IllegalStateRuntimeException
   *     <ul>
   *       <li>if the <tt>JMSContext</tt>'s session is using a transaction
   *       <li>if the {@code JMSContext} is container-managed (injected)
   *     </ul>
   *
   * @throws JMSRuntimeException if the JMS provider fails to stop and restart message delivery due
   *     to some internal error
   */
  @Override
  public void recover() {
    Utils.runtimeException(() -> session.recover());
  }

  /**
   * Creates a {@code JMSConsumer} for the specified destination.
   *
   * <p>A client uses a {@code JMSConsumer} object to receive messages that have been sent to a
   * destination.
   *
   * @param destination the {@code Destination} to access.
   * @return The created {@code JMSConsumer} object.
   * @throws JMSRuntimeException if the session fails to create a {@code JMSConsumer} due to some
   *     internal error.
   * @throws InvalidDestinationRuntimeException if an invalid destination is specified.
   */
  @Override
  public JMSConsumer createConsumer(Destination destination) {
    autoStartIfNeeded();
    return Utils.runtimeException(() -> session.createConsumer(destination).asJMSConsumer());
  }

  /**
   * Creates a {@code JMSConsumer} for the specified destination, using a message selector.
   *
   * <p>A client uses a {@code JMSConsumer} object to receive messages that have been sent to a
   * destination.
   *
   * @param destination the {@code Destination} to access
   * @param messageSelector only messages with properties matching the message selector expression
   *     are delivered. A value of null or an empty string indicates that there is no message
   *     selector for the {@code JMSConsumer}.
   * @return The created {@code JMSConsumer} object.
   * @throws JMSRuntimeException if the session fails to create a {@code JMSConsumer} due to some
   *     internal error.
   * @throws InvalidDestinationRuntimeException if an invalid destination is specified.
   * @throws InvalidSelectorRuntimeException if the message selector is invalid.
   */
  @Override
  public JMSConsumer createConsumer(Destination destination, String messageSelector) {
    autoStartIfNeeded();
    return Utils.runtimeException(
        () -> session.createConsumer(destination, messageSelector).asJMSConsumer());
  }

  /**
   * Creates a {@code JMSConsumer} for the specified destination, specifying a message selector and
   * the {@code noLocal} parameter.
   *
   * <p>A client uses a {@code JMSConsumer} object to receive messages that have been sent to a
   * destination.
   *
   * <p>The {@code noLocal} argument is for use when the destination is a topic and the JMSContext's
   * connection is also being used to publish messages to that topic. If {@code noLocal} is set to
   * true then the {@code JMSConsumer} will not receive messages published to the topic by its own
   * connection. The default value of this argument is false. If the destination is a queue then the
   * effect of setting {@code noLocal} to true is not specified.
   *
   * @param destination the {@code Destination} to access
   * @param messageSelector only messages with properties matching the message selector expression
   *     are delivered. A value of null or an empty string indicates that there is no message
   *     selector for the {@code JMSConsumer}.
   * @param noLocal if true, and the destination is a topic, then the {@code JMSConsumer} will not
   *     receive messages published to the topic by its own connection
   * @return The created {@code JMSConsumer} object.
   * @throws JMSRuntimeException if the session fails to create a {@code JMSConsumer} due to some
   *     internal error.
   * @throws InvalidDestinationRuntimeException if an invalid destination is specified.
   * @throws InvalidSelectorRuntimeException if the message selector is invalid.
   */
  @Override
  public JMSConsumer createConsumer(
      Destination destination, String messageSelector, boolean noLocal) {
    autoStartIfNeeded();
    return Utils.runtimeException(
        () -> session.createConsumer(destination, messageSelector, noLocal).asJMSConsumer());
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
   * @throws JMSRuntimeException if a Queue object cannot be created due to some internal error
   */
  @Override
  public Queue createQueue(String queueName) {
    return Utils.runtimeException(() -> session.createQueue(queueName));
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
   * @throws JMSRuntimeException if a Topic object cannot be created due to some internal error
   */
  @Override
  public Topic createTopic(String topicName) {
    return Utils.runtimeException(() -> session.createTopic(topicName));
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
   * creates a {@code JMSConsumer} on the existing durable subscription.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier,
   * and there is a consumer already active (i.e. not closed) on the durable subscription, then a
   * {@code JMSRuntimeException} will be thrown.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier
   * but a different topic, message selector or {@code noLocal} value has been specified, and there
   * is no consumer already active (i.e. not closed) on the durable subscription then this is
   * equivalent to unsubscribing (deleting) the old one and creating a new one.
   *
   * <p>A shared durable subscription and an unshared durable subscription may not have the same
   * name and client identifier. If a shared durable subscription already exists with the same name
   * and client identifier then a {@code JMSRuntimeException} is thrown.
   *
   * <p>There is no restriction on durable subscriptions and shared non-durable subscriptions having
   * the same name and clientId. Such subscriptions would be completely separate.
   *
   * @param topic the non-temporary {@code Topic} to subscribe to
   * @param name the name used to identify this subscription
   * @return The created {@code JMSConsumer} object.
   * @throws InvalidDestinationRuntimeException if an invalid topic is specified.
   * @throws IllegalStateRuntimeException if the client identifier is unset
   * @throws JMSRuntimeException
   *     <ul>
   *       <li>if the session fails to create the non-shared durable subscription and {@code
   *           JMSConsumer} due to some internal error
   *       <li>if an unshared durable subscription already exists with the same name and client
   *           identifier, and there is a consumer already active
   *       <li>if a shared durable subscription already exists with the same name and client
   *           identifier
   *     </ul>
   *
   * @since JMS 2.0
   */
  @Override
  public JMSConsumer createDurableConsumer(Topic topic, String name) {
    autoStartIfNeeded();
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
   * creates a {@code JMSConsumer} on the existing durable subscription.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier,
   * and there is a consumer already active (i.e. not closed) on the durable subscription, then a
   * {@code JMSRuntimeException} will be thrown.
   *
   * <p>If an unshared durable subscription already exists with the same name and client identifier
   * but a different topic, message selector or {@code noLocal} value has been specified, and there
   * is no consumer already active (i.e. not closed) on the durable subscription then this is
   * equivalent to unsubscribing (deleting) the old one and creating a new one.
   *
   * <p>If {@code noLocal} is set to true then any messages published to the topic using this {@code
   * JMSContext}'s connection, or any other connection with the same client identifier, will not be
   * added to the durable subscription.
   *
   * <p>A shared durable subscription and an unshared durable subscription may not have the same
   * name and client identifier. If a shared durable subscription already exists with the same name
   * and client identifier then a {@code JMSRuntimeException} is thrown.
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
   * @return The created {@code JMSConsumer} object.
   * @throws InvalidDestinationRuntimeException if an invalid topic is specified.
   * @throws InvalidSelectorRuntimeException if the message selector is invalid.
   * @throws IllegalStateRuntimeException if the client identifier is unset
   * @throws JMSRuntimeException
   *     <ul>
   *       <li>if the session fails to create the non-shared durable subscription and {@code
   *           JMSConsumer} due to some internal error
   *       <li>if an unshared durable subscription already exists with the same name and client
   *           identifier, and there is a consumer already active
   *       <li>if a shared durable subscription already exists with the same name and client
   *           identifier
   *     </ul>
   *
   * @since JMS 2.0
   */
  @Override
  public JMSConsumer createDurableConsumer(
      Topic topic, String name, String messageSelector, boolean noLocal) {
    autoStartIfNeeded();
    return Utils.runtimeException(
        () -> session.createDurableConsumer(topic, name, messageSelector, noLocal).asJMSConsumer());
  }

  /**
   * Creates a shared durable subscription on the specified topic (if one does not already exist),
   * specifying a message selector, and creates a consumer on that durable subscription. This method
   * creates the durable subscription without a message selector.
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
   * {@code JMSConsumer} on the existing shared durable subscription.
   *
   * <p>If a shared durable subscription already exists with the same name and client identifier (if
   * set) but a different topic or message selector has been specified, and there is no consumer
   * already active (i.e. not closed) on the durable subscription then this is equivalent to
   * unsubscribing (deleting) the old one and creating a new one.
   *
   * <p>If a shared durable subscription already exists with the same name and client identifier (if
   * set) but a different topic or message selector has been specified, and there is a consumer
   * already active (i.e. not closed) on the durable subscription, then a {@code
   * JMSRuntimeException} will be thrown.
   *
   * <p>A shared durable subscription and an unshared durable subscription may not have the same
   * name and client identifier (if set). If an unshared durable subscription already exists with
   * the same name and client identifier (if set) then a {@code JMSRuntimeException} is thrown.
   *
   * <p>If a message selector is specified then only messages with properties matching the message
   * selector expression will be added to the subscription.
   *
   * <p>There is no restriction on durable subscriptions and shared non-durable subscriptions having
   * the same name and clientId (which may be unset). Such subscriptions would be completely
   * separate.
   *
   * @param topic the non-temporary {@code Topic} to subscribe to
   * @param name the name used to identify this subscription
   * @return The created {@code JMSConsumer} object.
   * @throws InvalidDestinationRuntimeException if an invalid topic is specified.
   * @throws JMSRuntimeException
   *     <ul>
   *       <li>if the session fails to create the shared durable subscription and {@code
   *           MessageConsumer} due to some internal error
   *       <li>if a shared durable subscription already exists with the same name and client
   *           identifier, but a different topic, or message selector, and there is a consumer
   *           already active
   *       <li>if an unshared durable subscription already exists with the same name and client
   *           identifier
   *     </ul>
   *
   * @since JMS 2.0
   */
  @Override
  public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
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
   * set), and the same topic and message selector have been specified, then this method creates a
   * {@code JMSConsumer} on the existing shared durable subscription.
   *
   * <p>If a shared durable subscription already exists with the same name and client identifier (if
   * set), but a different topic or message selector has been specified, and there is no consumer
   * already active (i.e. not closed) on the durable subscription then this is equivalent to
   * unsubscribing (deleting) the old one and creating a new one.
   *
   * <p>If a shared durable subscription already exists with the same name and client identifier (if
   * set) but a different topic or message selector has been specified, and there is a consumer
   * already active (i.e. not closed) on the durable subscription, then a {@code
   * JMSRuntimeException} will be thrown.
   *
   * <p>A shared durable subscription and an unshared durable subscription may not have the same
   * name and client identifier (if set). If an unshared durable subscription already exists with
   * the same name and client identifier (if set) then a {@code JMSRuntimeException} is thrown.
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
   * @return The created {@code JMSConsumer} object.
   * @throws InvalidDestinationRuntimeException if an invalid topic is specified.
   * @throws InvalidSelectorRuntimeException if the message selector is invalid.
   * @throws JMSRuntimeException
   *     <ul>
   *       <li>if the session fails to create the shared durable subscription and {@code
   *           JMSConsumer} due to some internal error
   *       <li>if a shared durable subscription already exists with the same name and client
   *           identifier, but a different topic, or message selector, and there is a consumer
   *           already active
   *       <li>if an unshared durable subscription already exists with the same name and client
   *           identifier
   *     </ul>
   *
   * @since JMS 2.0
   */
  @Override
  public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) {
    autoStartIfNeeded();
    return Utils.runtimeException(
        () -> session.createSharedDurableConsumer(topic, name, messageSelector).asJMSConsumer());
  }

  /**
   * Creates a shared non-durable subscription with the specified name on the specified topic (if
   * one does not already exist) and creates a consumer on that subscription. This method creates
   * the non-durable subscription without a message selector.
   *
   * <p>If a shared non-durable subscription already exists with the same name and client identifier
   * (if set), and the same topic and message selector has been specified, then this method creates
   * a {@code JMSConsumer} on the existing subscription.
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
   * (if set) but a different topic or message selector value has been specified, and there is a
   * consumer already active (i.e. not closed) on the subscription, then a {@code
   * JMSRuntimeException} will be thrown.
   *
   * <p>There is no restriction on durable subscriptions and shared non-durable subscriptions having
   * the same name and clientId (which may be unset). Such subscriptions would be completely
   * separate.
   *
   * @param topic the {@code Topic} to subscribe to
   * @param sharedSubscriptionName the name used to identify the shared non-durable subscription
   * @return The created {@code JMSConsumer} object.
   * @throws JMSRuntimeException if the session fails to create the shared non-durable subscription
   *     and {@code JMSContext} due to some internal error.
   * @throws InvalidDestinationRuntimeException if an invalid topic is specified.
   * @throws InvalidSelectorRuntimeException if the message selector is invalid.
   */
  @Override
  public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
    return createSharedConsumer(topic, sharedSubscriptionName, null);
  }

  /**
   * Creates a shared non-durable subscription with the specified name on the specified topic (if
   * one does not already exist) specifying a message selector, and creates a consumer on that
   * subscription.
   *
   * <p>If a shared non-durable subscription already exists with the same name and client identifier
   * (if set), and the same topic and message selector has been specified, then this method creates
   * a {@code JMSConsumer} on the existing subscription.
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
   * already active (i.e. not closed) on the subscription, then a {@code JMSRuntimeException} will
   * be thrown.
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
   * @return The created {@code JMSConsumer} object.
   * @throws JMSRuntimeException if the session fails to create the shared non-durable subscription
   *     and {@code JMSConsumer} due to some internal error.
   * @throws InvalidDestinationRuntimeException if an invalid topic is specified.
   * @throws InvalidSelectorRuntimeException if the message selector is invalid.
   */
  @Override
  public JMSConsumer createSharedConsumer(
      Topic topic, String sharedSubscriptionName, String messageSelector) {
    autoStartIfNeeded();
    return Utils.runtimeException(
        () ->
            session
                .createSharedConsumer(topic, sharedSubscriptionName, messageSelector)
                .asJMSConsumer());
  }

  /**
   * Creates a {@code QueueBrowser} object to peek at the messages on the specified queue.
   *
   * @param queue the {@code queue} to access
   * @return The created {@code QueueBrowser} object.
   * @throws JMSRuntimeException if the session fails to create a browser due to some internal
   *     error.
   * @throws InvalidDestinationRuntimeException if an invalid destination is specified
   */
  @Override
  public QueueBrowser createBrowser(Queue queue) {
    return Utils.runtimeException(() -> session.createBrowser(queue));
  }

  /**
   * Creates a {@code QueueBrowser} object to peek at the messages on the specified queue using a
   * message selector.
   *
   * @param queue the {@code queue} to access
   * @param messageSelector only messages with properties matching the message selector expression
   *     are delivered. A value of null or an empty string indicates that there is no message
   *     selector for the message consumer.
   * @return The created {@code QueueBrowser} object.
   * @throws JMSRuntimeException if the session fails to create a browser due to some internal
   *     error.
   * @throws InvalidDestinationRuntimeException if an invalid destination is specified
   * @throws InvalidSelectorRuntimeException if the message selector is invalid.
   */
  @Override
  public QueueBrowser createBrowser(Queue queue, String messageSelector) {
    return Utils.runtimeException(() -> session.createBrowser(queue, messageSelector));
  }

  /**
   * Creates a {@code TemporaryQueue} object. Its lifetime will be that of the JMSContext's {@code
   * Connection} unless it is deleted earlier.
   *
   * @return a temporary queue identity
   * @throws JMSRuntimeException if the session fails to create a temporary queue due to some
   *     internal error.
   */
  @Override
  public TemporaryQueue createTemporaryQueue() {
    return Utils.runtimeException(() -> session.createTemporaryQueue());
  }

  /**
   * Creates a {@code TemporaryTopic} object. Its lifetime will be that of the JMSContext's {@code
   * Connection} unless it is deleted earlier.
   *
   * @return a temporary topic identity
   * @throws JMSRuntimeException if the session fails to create a temporary topic due to some
   *     internal error.
   */
  @Override
  public TemporaryTopic createTemporaryTopic() {
    return Utils.runtimeException(() -> session.createTemporaryTopic());
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
   * closed) consumer on that subscription, or while a consumed message is part of a pending
   * transaction or has not been acknowledged in the session.
   *
   * <p>If the active consumer is represented by a {@code JMSConsumer} then calling {@code close} on
   * either that object or the {@code JMSContext} used to create it will render the consumer
   * inactive and allow the subscription to be deleted.
   *
   * <p>If the active consumer was created by calling {@code setMessageListener} on the {@code
   * JMSContext} then calling {@code close} on the {@code JMSContext} will render the consumer
   * inactive and allow the subscription to be deleted.
   *
   * <p>If the active consumer is represented by a {@code MessageConsumer} or {@code
   * TopicSubscriber} then calling {@code close} on that object or on the {@code Session} or {@code
   * Connection} used to create it will render the consumer inactive and allow the subscription to
   * be deleted.
   *
   * @param name the name used to identify this subscription
   * @throws JMSRuntimeException if the session fails to unsubscribe to the durable subscription due
   *     to some internal error.
   * @throws InvalidDestinationRuntimeException if an invalid subscription name is specified.
   */
  @Override
  public void unsubscribe(String name) {
    Utils.runtimeException(() -> session.unsubscribe(name));
  }

  /**
   * Acknowledges all messages consumed by the JMSContext's session.
   *
   * <p>This method is for use when the session has an acknowledgement mode of CLIENT_ACKNOWLEDGE.
   * If the session is transacted or has an acknowledgement mode of AUTO_ACKNOWLEDGE or
   * DUPS_OK_ACKNOWLEDGE calling this method has no effect.
   *
   * <p>This method has identical behaviour to the {@code acknowledge} method on {@code Message}. A
   * client may individually acknowledge each message as it is consumed, or it may choose to
   * acknowledge messages as an application-defined group. In both cases it makes no difference
   * which of these two methods is used.
   *
   * <p>Messages that have been received but not acknowledged may be redelivered.
   *
   * <p>This method must not be used if the {@code JMSContext} is container-managed (injected).
   * Doing so will cause a {@code IllegalStateRuntimeException} to be thrown.
   *
   * @throws IllegalStateRuntimeException
   *     <ul>
   *       <li>if the {@code JMSContext} is closed.
   *       <li>if the {@code JMSContext} is container-managed (injected)
   *     </ul>
   *
   * @throws JMSRuntimeException if the JMS provider fails to acknowledge the messages due to some
   *     internal error
   * @see Session#CLIENT_ACKNOWLEDGE
   * @see Message#acknowledge
   */
  @Override
  public void acknowledge() {
    Utils.runtimeException(() -> session.acknowledgeAllMessages());
  }

  private void autoStartIfNeeded() {
    if (autoStart && !connection.isStarted()) {
      Utils.runtimeException(() -> connection.start());
    }
  }
}
