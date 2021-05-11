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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.InvalidClientIDException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarConnection implements Connection, QueueConnection, TopicConnection {

  private final PulsarConnectionFactory factory;
  private volatile ExceptionListener exceptionListener;
  private final List<PulsarSession> sessions = new CopyOnWriteArrayList<>();
  private final List<PulsarTemporaryDestination> temporaryDestinations =
      new CopyOnWriteArrayList<>();
  private final String connectionId;
  private volatile boolean closed = false;
  String clientId;
  private volatile boolean allowSetClientId = true;
  private final ReentrantReadWriteLock connectionPausedLock = new ReentrantReadWriteLock();
  private final Condition pausedCondition = connectionPausedLock.writeLock().newCondition();;
  private volatile boolean paused = true;

  public PulsarConnection(PulsarConnectionFactory factory) throws JMSException {
    this.factory = factory;
    this.clientId = factory.getDefaultClientId();
    if (this.clientId != null) {
      factory.registerClientId(this.clientId);
      connectionId = clientId + "_" + UUID.randomUUID().toString();
    } else {
      connectionId = UUID.randomUUID().toString();
    }
  }

  public PulsarConnectionFactory getFactory() {
    return factory;
  }

  /**
   * Creates a {@code Session} object, specifying {@code transacted} and {@code acknowledgeMode}.
   *
   * <p>This method has been superseded by the method {@code createSession(int sessionMode)} which
   * specifies the same information using a single argument, and by the method {@code
   * createSession()} which is for use in a Java EE JTA transaction. Applications should consider
   * using those methods instead of this one.
   *
   * <p>The effect of setting the {@code transacted} and {@code acknowledgeMode} arguments depends
   * on whether this method is called in a Java SE environment, in the Java EE application client
   * container, or in the Java EE web or EJB container. If this method is called in the Java EE web
   * or EJB container then the effect of setting the transacted} and {@code acknowledgeMode}
   * arguments also depends on whether or not there is an active JTA transaction in progress.
   *
   * <p>In a <b>Java SE environment</b> or in <b>the Java EE application client container</b>:
   *
   * <ul>
   *   <li>If {@code transacted} is set to {@code true} then the session will use a local
   *       transaction which may subsequently be committed or rolled back by calling the session's
   *       {@code commit} or {@code rollback} methods. The argument {@code acknowledgeMode} is
   *       ignored.
   *   <li>If {@code transacted} is set to {@code false} then the session will be non-transacted. In
   *       this case the argument {@code acknowledgeMode} is used to specify how messages received
   *       by this session will be acknowledged. The permitted values are {@code
   *       Session.CLIENT_ACKNOWLEDGE}, {@code Session.AUTO_ACKNOWLEDGE} and {@code
   *       Session.DUPS_OK_ACKNOWLEDGE}. For a definition of the meaning of these acknowledgement
   *       modes see the links below.
   * </ul>
   *
   * <p>In a <b>Java EE web or EJB container, when there is an active JTA transaction in
   * progress</b>:
   *
   * <ul>
   *   <li>Both arguments {@code transacted} and {@code acknowledgeMode} are ignored. The session
   *       will participate in the JTA transaction and will be committed or rolled back when that
   *       transaction is committed or rolled back, not by calling the session's {@code commit} or
   *       {@code rollback} methods. Since both arguments are ignored, developers are recommended to
   *       use {@code createSession()}, which has no arguments, instead of this method.
   * </ul>
   *
   * <p>In the <b>Java EE web or EJB container, when there is no active JTA transaction in
   * progress</b>:
   *
   * <ul>
   *   <li>If {@code transacted} is set to false and {@code acknowledgeMode} is set to {@code
   *       JMSContext.AUTO_ACKNOWLEDGE} or {@code Session.DUPS_OK_ACKNOWLEDGE} then the session will
   *       be non-transacted and messages will be acknowledged according to the value of {@code
   *       acknowledgeMode}.
   *   <li>If {@code transacted} is set to false and {@code acknowledgeMode} is set to {@code
   *       JMSContext.CLIENT_ACKNOWLEDGE} then the JMS provider is recommended to ignore the
   *       specified parameters and instead provide a non-transacted, auto-acknowledged session.
   *       However the JMS provider may alternatively provide a non-transacted session with client
   *       acknowledgement.
   *   <li>If {@code transacted} is set to true, then the JMS provider is recommended to ignore the
   *       specified parameters and instead provide a non-transacted, auto-acknowledged session.
   *       However the JMS provider may alternatively provide a local transacted session.
   *   <li>Applications are recommended to set {@code transacted} to false and {@code
   *       acknowledgeMode} to {@code JMSContext.AUTO_ACKNOWLEDGE} or {@code
   *       Session.DUPS_OK_ACKNOWLEDGE} since since applications which set {@code transacted} to
   *       false and set {@code acknowledgeMode} to {@code JMSContext.CLIENT_ACKNOWLEDGE}, or which
   *       set {@code transacted} to true, may not be portable.
   * </ul>
   *
   * <p>Applications running in the Java EE web and EJB containers must not attempt to create more
   * than one active (not closed) {@code Session} object per connection. If this method is called in
   * a Java EE web or EJB container when an active {@code Session} object already exists for this
   * connection then a {@code JMSException} may be thrown.
   *
   * @param transacted indicates whether the session will use a local transaction, except in the
   *     cases described above when this value is ignored..
   * @param acknowledgeMode when transacted is false, indicates how messages received by the session
   *     will be acknowledged, except in the cases described above when this value is ignored.
   * @return a newly created session
   * @throws JMSException if the {@code Connection} object fails to create a session due to
   *     <ul>
   *       <li>some internal error,
   *       <li>lack of support for the specific transaction and acknowledgement mode, or
   *       <li>because this method is being called in a Java EE web or EJB application and an active
   *           session already exists for this connection.
   *     </ul>
   *
   * @see Session#AUTO_ACKNOWLEDGE
   * @see Session#CLIENT_ACKNOWLEDGE
   * @see Session#DUPS_OK_ACKNOWLEDGE
   * @see Connection#createSession(int)
   * @see Connection#createSession()
   * @since JMS 1.1
   */
  @Override
  public PulsarSession createSession(boolean transacted, int acknowledgeMode) throws JMSException {
    checkNotClosed();
    allowSetClientId = false;
    PulsarSession session =
        new PulsarSession(transacted ? Session.SESSION_TRANSACTED : acknowledgeMode, this);
    sessions.add(session);
    return session;
  }

  void checkNotClosed() throws JMSException {
    if (closed) {
      throw new IllegalStateException("This connection is closed");
    }
  }

  /**
   * Creates a {@code Session} object, specifying {@code sessionMode}.
   *
   * <p>The effect of setting the {@code sessionMode} argument depends on whether this method is
   * called in a Java SE environment, in the Java EE application client container, or in the Java EE
   * web or EJB container. If this method is called in the Java EE web or EJB container then the
   * effect of setting the {@code sessionMode} argument also depends on whether or not there is an
   * active JTA transaction in progress.
   *
   * <p>In a <b>Java SE environment</b> or in <b>the Java EE application client container</b>:
   *
   * <ul>
   *   <li>If {@code sessionMode} is set to {@code Session.SESSION_TRANSACTED} then the session will
   *       use a local transaction which may subsequently be committed or rolled back by calling the
   *       session's {@code commit} or {@code rollback} methods.
   *   <li>If {@code sessionMode} is set to any of {@code Session.CLIENT_ACKNOWLEDGE}, {@code
   *       Session.AUTO_ACKNOWLEDGE} or {@code Session.DUPS_OK_ACKNOWLEDGE}. then the session will
   *       be non-transacted and messages received by this session will be acknowledged according to
   *       the value of {@code sessionMode}. For a definition of the meaning of these
   *       acknowledgement modes see the links below.
   * </ul>
   *
   * <p>In a <b>Java EE web or EJB container, when there is an active JTA transaction in
   * progress</b>:
   *
   * <ul>
   *   <li>The argument {@code sessionMode} is ignored. The session will participate in the JTA
   *       transaction and will be committed or rolled back when that transaction is committed or
   *       rolled back, not by calling the session's {@code commit} or {@code rollback} methods.
   *       Since the argument is ignored, developers are recommended to use {@code createSession()},
   *       which has no arguments, instead of this method.
   * </ul>
   *
   * <p>In the <b>Java EE web or EJB container, when there is no active JTA transaction in
   * progress</b>:
   *
   * <ul>
   *   <li>If {@code sessionMode} is set to {@code Session.AUTO_ACKNOWLEDGE} or {@code
   *       Session.DUPS_OK_ACKNOWLEDGE} then the session will be non-transacted and messages will be
   *       acknowledged according to the value of {@code sessionMode}.
   *   <li>If {@code sessionMode} is set to {@code Session.CLIENT_ACKNOWLEDGE} then the JMS provider
   *       is recommended to ignore the specified parameter and instead provide a non-transacted,
   *       auto-acknowledged session. However the JMS provider may alternatively provide a
   *       non-transacted session with client acknowledgement.
   *   <li>If {@code sessionMode} is set to {@code Session.SESSION_TRANSACTED}, then the JMS
   *       provider is recommended to ignore the specified parameter and instead provide a
   *       non-transacted, auto-acknowledged session. However the JMS provider may alternatively
   *       provide a local transacted session.
   *   <li>Applications are recommended to use only the values {@code Session.AUTO_ACKNOWLEDGE} and
   *       {@code Session.DUPS_OK_ACKNOWLEDGE} since applications which use {@code
   *       Session.CLIENT_ACKNOWLEDGE} or {@code Session.SESSION_TRANSACTED} may not be portable.
   * </ul>
   *
   * <p>Applications running in the Java EE web and EJB containers must not attempt to create more
   * than one active (not closed) {@code Session} object per connection. If this method is called in
   * a Java EE web or EJB container when an active {@code Session} object already exists for this
   * connection then a {@code JMSException} may be thrown.
   *
   * @param sessionMode specifies the session mode that will be used, except in the cases described
   *     above when this value is ignored. Legal values are {@code JMSContext.SESSION_TRANSACTED},
   *     {@code JMSContext.CLIENT_ACKNOWLEDGE}, {@code JMSContext.AUTO_ACKNOWLEDGE} and {@code
   *     JMSContext.DUPS_OK_ACKNOWLEDGE}.
   * @return a newly created session
   * @throws JMSException if the {@code Connection} object fails to create a session due to
   *     <ul>
   *       <li>some internal error,
   *       <li>lack of support for the specific transaction and acknowledgement mode, or
   *       <li>because this method is being called in a Java EE web or EJB application and an active
   *           session already exists for this connection.
   *     </ul>
   *
   * @see Session#SESSION_TRANSACTED
   * @see Session#AUTO_ACKNOWLEDGE
   * @see Session#CLIENT_ACKNOWLEDGE
   * @see Session#DUPS_OK_ACKNOWLEDGE
   * @see Connection#createSession(boolean, int)
   * @see Connection#createSession()
   * @since JMS 2.0
   */
  @Override
  public PulsarSession createSession(int sessionMode) throws JMSException {
    return createSession(sessionMode == Session.SESSION_TRANSACTED, sessionMode);
  }

  /**
   * Creates a {@code Session} object, specifying no arguments.
   *
   * <p>The behaviour of the session that is created depends on whether this method is called in a
   * Java SE environment, in the Java EE application client container, or in the Java EE web or EJB
   * container. If this method is called in the Java EE web or EJB container then the behaviour of
   * the session also depends on whether or not there is an active JTA transaction in progress.
   *
   * <p>In a <b>Java SE environment</b> or in <b>the Java EE application client container</b>:
   *
   * <ul>
   *   <li>The session will be non-transacted and received messages will be acknowledged
   *       automatically using an acknowledgement mode of {@code Session.AUTO_ACKNOWLEDGE} For a
   *       definition of the meaning of this acknowledgement mode see the link below.
   * </ul>
   *
   * <p>In a <b>Java EE web or EJB container, when there is an active JTA transaction in
   * progress</b>:
   *
   * <ul>
   *   <li>The session will participate in the JTA transaction and will be committed or rolled back
   *       when that transaction is committed or rolled back, not by calling the session's {@code
   *       commit} or {@code rollback} methods.
   * </ul>
   *
   * <p>In the <b>Java EE web or EJB container, when there is no active JTA transaction in
   * progress</b>:
   *
   * <ul>
   *   <li>The session will be non-transacted and received messages will be acknowledged
   *       automatically using an acknowledgement mode of {@code Session.AUTO_ACKNOWLEDGE} For a
   *       definition of the meaning of this acknowledgement mode see the link below.
   * </ul>
   *
   * <p>Applications running in the Java EE web and EJB containers must not attempt to create more
   * than one active (not closed) {@code Session} object per connection. If this method is called in
   * a Java EE web or EJB container when an active {@code Session} object already exists for this
   * connection then a {@code JMSException} may be thrown.
   *
   * @return a newly created session
   * @throws JMSException if the {@code Connection} object fails to create a session due to
   *     <ul>
   *       <li>some internal error or
   *       <li>because this method is being called in a Java EE web or EJB application and an active
   *           session already exists for this connection.
   *     </ul>
   *
   * @see Session#AUTO_ACKNOWLEDGE
   * @see Connection#createSession(boolean, int)
   * @see Connection#createSession(int)
   * @since JMS 2.0
   */
  @Override
  public PulsarSession createSession() throws JMSException {
    allowSetClientId = false;
    return createSession(Session.AUTO_ACKNOWLEDGE);
  }

  /**
   * Gets the client identifier for this connection.
   *
   * <p>This value is specific to the JMS provider. It is either preconfigured by an administrator
   * in a {@code ConnectionFactory} object or assigned dynamically by the application by calling the
   * {@code setClientID} method.
   *
   * @return the unique client identifier
   * @throws JMSException if the JMS provider fails to return the client ID for this connection due
   *     to some internal error.
   */
  @Override
  public String getClientID() throws JMSException {
    checkNotClosed();
    return clientId;
  }

  /**
   * Sets the client identifier for this connection.
   *
   * <p>The preferred way to assign a JMS client's client identifier is for it to be configured in a
   * client-specific {@code ConnectionFactory} object and transparently assigned to the {@code
   * Connection} object it creates.
   *
   * <p>Alternatively, a client can set a connection's client identifier using a provider-specific
   * value. The facility to set a connection's client identifier explicitly is not a mechanism for
   * overriding the identifier that has been administratively configured. It is provided for the
   * case where no administratively specified identifier exists. If one does exist, an attempt to
   * change it by setting it must throw an {@code IllegalStateException}. If a client sets the
   * client identifier explicitly, it must do so immediately after it creates the connection and
   * before any other action on the connection is taken. After this point, setting the client
   * identifier is a programming error that should throw an {@code IllegalStateException}.
   *
   * <p>The purpose of the client identifier is to associate a connection and its objects with a
   * state maintained on behalf of the client by a provider. The only such state identified by the
   * JMS API is that required to support durable subscriptions.
   *
   * <p>If another connection with the same {@code clientID} is already running when this method is
   * called, the JMS provider should detect the duplicate ID and throw an {@code
   * InvalidClientIDException}.
   *
   * <p>This method must not be used in a Java EE web or EJB application. Doing so may cause a
   * {@code JMSException} to be thrown though this is not guaranteed.
   *
   * @param clientID the unique client identifier
   * @throws JMSException if the JMS provider fails to set the client ID for the the connection for
   *     one of the following reasons:
   *     <ul>
   *       <li>an internal error has occurred or
   *       <li>this method has been called in a Java EE web or EJB application (though it is not
   *           guaranteed that an exception is thrown in this case)
   *     </ul>
   *
   * @throws InvalidClientIDException if the JMS client specifies an invalid or duplicate client ID.
   * @throws IllegalStateException if the JMS client attempts to set a connection's client ID at the
   *     wrong time or when it has been administratively configured.
   */
  @Override
  public void setClientID(String clientID) throws JMSException {
    log.info("setClientID {} on {}, factory {}", clientID, this, factory);
    checkNotClosed();
    if (!allowSetClientId) {
      throw new IllegalStateException("Cannot set clientId after performing any other operation");
    }
    if (factory.getDefaultClientId() != null) {
      throw new IllegalStateException(
          "ClientId has be administratively configured as "
              + factory.getDefaultClientId()
              + ", you cannot change it");
    }
    if (clientID == null || clientID.isEmpty()) {
      throw new InvalidClientIDException("Invalid empty clientId");
    }
    if (this.clientId != null) {
      throw new InvalidClientIDException("cannot set again the clientId");
    }
    allowSetClientId = false;
    factory.registerClientId(clientID);
    this.clientId = clientID;
  }

  /**
   * Gets the metadata for this connection.
   *
   * @return the connection metadata
   * @throws JMSException if the JMS provider fails to get the connection metadata for this
   *     connection.
   * @see ConnectionMetaData
   */
  @Override
  public ConnectionMetaData getMetaData() throws JMSException {
    checkNotClosed();
    return PulsarConnectionMetadata.INSTANCE;
  }

  /**
   * Gets the {@code ExceptionListener} object for this connection. Not every {@code Connection} has
   * an {@code ExceptionListener} associated with it.
   *
   * @return the {@code ExceptionListener} for this connection, or null. if no {@code
   *     ExceptionListener} is associated with this connection.
   * @throws JMSException if the JMS provider fails to get the {@code ExceptionListener} for this
   *     connection.
   * @see Connection#setExceptionListener
   */
  @Override
  public ExceptionListener getExceptionListener() throws JMSException {
    checkNotClosed();
    return exceptionListener;
  }

  /**
   * Sets an exception listener for this connection.
   *
   * <p>If a JMS provider detects a serious problem with a connection, it informs the connection's
   * {@code ExceptionListener}, if one has been registered. It does this by calling the listener's
   * {@code onException} method, passing it a {@code JMSException} object describing the problem.
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
   * {@code JMSException} to be thrown though this is not guaranteed.
   *
   * @param listener the exception listener
   * @throws JMSException if the JMS provider fails to set the exception listener for one of the
   *     following reasons:
   *     <ul>
   *       <li>an internal error has occurred or
   *       <li>this method has been called in a Java EE web or EJB application (though it is not
   *           guaranteed that an exception is thrown in this case)
   *     </ul>
   */
  @Override
  public void setExceptionListener(ExceptionListener listener) throws JMSException {
    checkNotClosed();
    this.exceptionListener = listener;
  }

  /**
   * Starts (or restarts) a connection's delivery of incoming messages. A call to {@code start} on a
   * connection that has already been started is ignored.
   *
   * @throws JMSException if the JMS provider fails to start message delivery due to some internal
   *     error.
   * @see Connection#stop
   */
  @Override
  public void start() throws JMSException {
    checkNotInSessionMessageListener();
    checkNotClosed();
    connectionPausedLock.writeLock().lock();
    try {
      paused = false;
      pausedCondition.signalAll();
    } catch (Throwable err) {
      throw Utils.handleException(err);
    } finally {
      connectionPausedLock.writeLock().unlock();
    }
  }

  /**
   * Temporarily stops a connection's delivery of incoming messages. Delivery can be restarted using
   * the connection's {@code start} method. When the connection is stopped, delivery to all the
   * connection's message consumers is inhibited: synchronous receives block, and messages are not
   * delivered to message listeners.
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
   * <p>However if the {@code stop} method is called from a message listener on its own connection,
   * then it will either fail and throw a {@code javax.jms.IllegalStateException}, or it will
   * succeed and stop the connection, blocking until all other message listeners that may have been
   * running have returned.
   *
   * <p>Since two alternative behaviors are permitted in this case, applications should avoid
   * calling {@code stop} from a message listener on its own Connection because this is not
   * portable.
   *
   * <p>For the avoidance of doubt, if an exception listener for this connection is running when
   * {@code stop} is invoked, there is no requirement for the {@code stop} call to wait until the
   * exception listener has returned before it may return.
   *
   * <p>This method must not be used in a Java EE web or EJB application. Doing so may cause a
   * {@code JMSException} to be thrown though this is not guaranteed.
   *
   * @throws IllegalStateException this method has been called by a <tt>MessageListener</tt> on its
   *     own <tt>Connection</tt>
   * @throws JMSException if the JMS provider fails to stop message delivery for one of the
   *     following reasons:
   *     <ul>
   *       <li>an internal error has occurred or
   *       <li>this method has been called in a Java EE web or EJB application (though it is not
   *           guaranteed that an exception is thrown in this case)
   *     </ul>
   *
   * @see Connection#start
   */
  @Override
  public void stop() throws JMSException {
    checkNotInSessionMessageListener();
    checkNotClosed();
    connectionPausedLock.writeLock().lock();
    try {
      paused = true;
      pausedCondition.signalAll();
    } catch (Throwable err) {
      throw Utils.handleException(err);
    } finally {
      connectionPausedLock.writeLock().unlock();
    }
  }

  private void checkNotInSessionMessageListener() throws JMSException {
    for (PulsarSession session : sessions) {
      Utils.checkNotOnMessageListener(session);
    }
  }

  /**
   * Closes the connection.
   *
   * <p>Since a provider typically allocates significant resources outside the JVM on behalf of a
   * connection, clients should close these resources when they are not needed. Relying on garbage
   * collection to eventually reclaim these resources may not be timely enough.
   *
   * <p>There is no need to close the sessions, producers, and consumers of a closed connection.
   *
   * <p>Closing a connection causes all temporary destinations to be deleted.
   *
   * <p>When this method is invoked, it should not return until message processing has been shut
   * down in an orderly fashion. This means that all message listeners that may have been running
   * have returned, and that all pending receives have returned.
   *
   * <p>However if the close method is called from a message listener on its own connection, then it
   * will either fail and throw a {@code javax.jms.IllegalStateException}, or it will succeed and
   * close the connection, blocking until all other message listeners that may have been running
   * have returned, and all pending receive calls have completed. If close succeeds and the
   * acknowledge mode of the session is set to {@code AUTO_ACKNOWLEDGE}, the current message will
   * still be acknowledged automatically when the {@code onMessage} call completes. Since two
   * alternative behaviors are permitted in this case, applications should avoid calling close from
   * a message listener on its own connection because this is not portable.
   *
   * <p>A close terminates all pending message receives on the connection's sessions' consumers. The
   * receives may return with a message or with null, depending on whether there was a message
   * available at the time of the close. If one or more of the connection's sessions' message
   * listeners is processing a message at the time when connection {@code close} is invoked, all the
   * facilities of the connection and its sessions must remain available to those listeners until
   * they return control to the JMS provider.
   *
   * <p>This method must not return until any incomplete asynchronous send operations for this
   * <tt>Connection</tt> have been completed and any <tt>CompletionListener</tt> callbacks have
   * returned. Incomplete sends should be allowed to complete normally unless an error occurs.
   *
   * <p>For the avoidance of doubt, if an exception listener for this connection is running when
   * {@code close} is invoked, there is no requirement for the {@code close} call to wait until the
   * exception listener has returned before it may return.
   *
   * <p>Closing a connection causes any of its sessions' transactions in progress to be rolled back.
   * In the case where a session's work is coordinated by an external transaction manager, a
   * session's {@code commit} and {@code rollback} methods are not used and the result of a closed
   * session's work is determined later by the transaction manager. Closing a connection does NOT
   * force an acknowledgment of client-acknowledged sessions.
   *
   * <p>A <tt>CompletionListener</tt> callback method must not call <tt>close</tt> on its own
   * <tt>Connection</tt>. Doing so will cause an <tt>IllegalStateException</tt> to be thrown.
   *
   * <p>Invoking the {@code acknowledge} method of a received message from a closed connection's
   * session must throw an {@code IllegalStateException}. Closing a closed connection must NOT throw
   * an exception.
   *
   * @throws IllegalStateException
   *     <ul>
   *       <li>this method has been called by a <tt>MessageListener </tt> on its own
   *           <tt>Connection</tt>
   *       <li>this method has been called by a <tt>CompletionListener</tt> callback method on its
   *           own <tt>Connection</tt>
   *     </ul>
   *
   * @throws JMSException if the JMS provider fails to close the connection due to some internal
   *     error. For example, a failure to release resources or to close a socket connection can
   *     cause this exception to be thrown.
   */
  @Override
  public void close() throws JMSException {
    checkNotInSessionMessageListener();
    if (closed) {
      return;
    }
    closed = true;
    for (PulsarSession session : sessions) {
      session.close();
    }
    sessions.clear();
    for (PulsarTemporaryDestination temporaryDestination : new ArrayList<>(temporaryDestinations)) {
      try {
        temporaryDestination.delete();
      } catch (JMSException err) {
        log.error("Cannot delete temporary destination {}", temporaryDestination.topicName, err);
      }
    }
    temporaryDestinations.clear();
    factory.unregisterConnection(this);
  }

  /**
   * Creates a connection consumer for this connection (optional operation) on the specific
   * destination.
   *
   * <p>This is an expert facility not used by ordinary JMS clients.
   *
   * <p>This method must not be used in a Java EE web or EJB application. Doing so may cause a
   * {@code JMSException} to be thrown though this is not guaranteed.
   *
   * @param destination the destination to access
   * @param messageSelector only messages with properties matching the message selector expression
   *     are delivered. A value of null or an empty string indicates that there is no message
   *     selector for the message consumer.
   * @param sessionPool the server session pool to associate with this connection consumer
   * @param maxMessages the maximum number of messages that can be assigned to a server session at
   *     one time
   * @return the connection consumer
   * @throws InvalidDestinationException if an invalid destination is specified.
   * @throws InvalidSelectorException if the message selector is invalid.
   * @throws JMSException if the {@code Connection} object fails to create a connection consumer for
   *     one of the following reasons:
   *     <ul>
   *       <li>an internal error has occurred
   *       <li>invalid arguments for {@code sessionPool} and {@code messageSelector} or
   *       <li>this method has been called in a Java EE web or EJB application (though it is not
   *           guaranteed that an exception is thrown in this case)
   *     </ul>
   *
   * @see ConnectionConsumer
   * @since JMS 1.1
   */
  @Override
  public ConnectionConsumer createConnectionConsumer(
      Destination destination,
      String messageSelector,
      ServerSessionPool sessionPool,
      int maxMessages)
      throws JMSException {
    checkNotClosed();
    throw new JMSException("not supported");
  }

  /**
   * Creates a connection consumer for this connection (optional operation) on the specific topic
   * using a shared non-durable subscription with the specified name.
   *
   * <p>This is an expert facility not used by ordinary JMS clients.
   *
   * <p>This method must not be used in a Java EE web or EJB application. Doing so may cause a
   * {@code JMSException} to be thrown though this is not guaranteed.
   *
   * @param topic the topic to access
   * @param subscriptionName the name used to identify the shared non-durable subscription
   * @param messageSelector only messages with properties matching the message selector expression
   *     are delivered. A value of null or an empty string indicates that there is no message
   *     selector for the message consumer.
   * @param sessionPool the server session pool to associate with this connection consumer
   * @param maxMessages the maximum number of messages that can be assigned to a server session at
   *     one time
   * @return the connection consumer
   * @throws IllegalStateException if called on a {@code QueueConnection}
   * @throws InvalidDestinationException if an invalid destination is specified.
   * @throws InvalidSelectorException if the message selector is invalid.
   * @throws JMSException if the {@code Connection} object fails to create a connection consumer for
   *     one of the following reasons:
   *     <ul>
   *       <li>an internal error has occurred
   *       <li>invalid arguments for {@code sessionPool} and {@code messageSelector} or
   *       <li>this method has been called in a Java EE web or EJB application (though it is not
   *           guaranteed that an exception is thrown in this case)
   *     </ul>
   *
   * @see ConnectionConsumer
   * @since JMS 2.0
   */
  @Override
  public ConnectionConsumer createSharedConnectionConsumer(
      Topic topic,
      String subscriptionName,
      String messageSelector,
      ServerSessionPool sessionPool,
      int maxMessages)
      throws JMSException {
    checkNotClosed();
    throw new JMSException("not supported");
  }

  /**
   * Creates a connection consumer for this connection (optional operation) on the specific topic
   * using an unshared durable subscription with the specified name.
   *
   * <p>This is an expert facility not used by ordinary JMS clients.
   *
   * <p>This method must not be used in a Java EE web or EJB application. Doing so may cause a
   * {@code JMSException} to be thrown though this is not guaranteed.
   *
   * @param topic topic to access
   * @param subscriptionName the name used to identify the unshared durable subscription
   * @param messageSelector only messages with properties matching the message selector expression
   *     are delivered. A value of null or an empty string indicates that there is no message
   *     selector for the message consumer.
   * @param sessionPool the server session pool to associate with this durable connection consumer
   * @param maxMessages the maximum number of messages that can be assigned to a server session at
   *     one time
   * @return the durable connection consumer
   * @throws IllegalStateException if called on a {@code QueueConnection}
   * @throws InvalidDestinationException if an invalid destination is specified.
   * @throws InvalidSelectorException if the message selector is invalid.
   * @throws JMSException if the {@code Connection} object fails to create a connection consumer for
   *     one of the following reasons:
   *     <ul>
   *       <li>an internal error has occurred
   *       <li>invalid arguments for {@code sessionPool} and {@code messageSelector} or
   *       <li>this method has been called in a Java EE web or EJB application (though it is not
   *           guaranteed that an exception is thrown in this case)
   *     </ul>
   *
   * @see ConnectionConsumer
   * @since JMS 1.1
   */
  @Override
  public ConnectionConsumer createDurableConnectionConsumer(
      Topic topic,
      String subscriptionName,
      String messageSelector,
      ServerSessionPool sessionPool,
      int maxMessages)
      throws JMSException {
    checkNotClosed();
    throw new JMSException("not supported");
  }

  /**
   * Creates a connection consumer for this connection (optional operation) on the specific topic
   * using a shared durable subscription with the specified name.
   *
   * <p>This is an expert facility not used by ordinary JMS clients.
   *
   * <p>This method must not be used in a Java EE web or EJB application. Doing so may cause a
   * {@code JMSException} to be thrown though this is not guaranteed.
   *
   * @param topic topic to access
   * @param subscriptionName the name used to identify the shared durable subscription
   * @param messageSelector only messages with properties matching the message selector expression
   *     are delivered. A value of null or an empty string indicates that there is no message
   *     selector for the message consumer.
   * @param sessionPool the server session pool to associate with this durable connection consumer
   * @param maxMessages the maximum number of messages that can be assigned to a server session at
   *     one time
   * @return the durable connection consumer
   * @throws IllegalStateException if called on a {@code QueueConnection}
   * @throws InvalidDestinationException if an invalid destination is specified.
   * @throws InvalidSelectorException if the message selector is invalid.
   * @throws JMSException if the {@code Connection} object fails to create a connection consumer for
   *     one of the following reasons:
   *     <ul>
   *       <li>an internal error has occurred
   *       <li>invalid arguments for {@code sessionPool} and {@code messageSelector} or
   *       <li>this method has been called in a Java EE web or EJB application (though it is not
   *           guaranteed that an exception is thrown in this case)
   *     </ul>
   *
   * @see ConnectionConsumer
   * @since JMS 2.0
   */
  @Override
  public ConnectionConsumer createSharedDurableConnectionConsumer(
      Topic topic,
      String subscriptionName,
      String messageSelector,
      ServerSessionPool sessionPool,
      int maxMessages)
      throws JMSException {
    checkNotClosed();
    throw new JMSException("not supported");
  }

  public void unregisterSession(PulsarSession session) {
    sessions.remove(session);
  }

  public String prependClientId(String subscriptionName, boolean allowUnset) throws JMSException {
    if (clientId != null) {
      return clientId + "_" + subscriptionName;
    } else {
      if (allowUnset) {
        return subscriptionName;
      } else {
        throw new IllegalStateException("ClientID must be set");
      }
    }
  }

  public void setAllowSetClientId(boolean value) {
    allowSetClientId = value;
  }

  public <T> T executeInConnectionPausedLock(Utils.SupplierWithException<T> run, int timeoutMillis)
      throws JMSException {

    boolean executedInTime = true;
    connectionPausedLock.readLock().lock();
    try {
      if (paused) {
        connectionPausedLock.readLock().unlock();
        connectionPausedLock.writeLock().lock();
        try {
          while (paused) {
            if (timeoutMillis > 0) {
              executedInTime = pausedCondition.await(timeoutMillis, TimeUnit.MILLISECONDS);
              if (!executedInTime) {
                break;
              }
            } else {
              pausedCondition.await();
            }
          }
          connectionPausedLock.readLock().lock();
        } finally {
          connectionPausedLock.writeLock().unlock();
        }
      }
      if (!executedInTime) {
        return null;
      }
      return run.run();
    } catch (Throwable err) {
      throw Utils.handleException(err);
    } finally {
      connectionPausedLock.readLock().unlock(); // let writers in
    }
  }

  public boolean isStarted() {
    connectionPausedLock.readLock().lock();
    try {
      return !paused;
    } finally {
      connectionPausedLock.readLock().unlock();
    }
  }

  public TemporaryQueue createTemporaryQueue(PulsarSession session) throws JMSException {
    checkNotClosed();
    String name =
        "persistent://" + factory.getSystemNamespace() + "/jms-temp-queue-" + UUID.randomUUID();
    try {
      factory.getPulsarAdmin().topics().createNonPartitionedTopic(name);
    } catch (Exception err) {
      throw Utils.handleException(err);
    }
    PulsarTemporaryQueue res = new PulsarTemporaryQueue(name, session);
    temporaryDestinations.add(res);
    return res;
  }

  public TemporaryTopic createTemporaryTopic(PulsarSession session) throws JMSException {
    checkNotClosed();
    String name =
        "persistent://" + factory.getSystemNamespace() + "/jms-temp-topic-" + UUID.randomUUID();
    try {
      factory.getPulsarAdmin().topics().createNonPartitionedTopic(name);
    } catch (Exception err) {
      throw Utils.handleException(err);
    }
    PulsarTemporaryTopic res = new PulsarTemporaryTopic(name, session);
    temporaryDestinations.add(res);
    return res;
  }

  @Override
  public QueueSession createQueueSession(boolean b, int i) throws JMSException {
    return createSession(b, i).emulateLegacySession(true, false);
  }

  @Override
  public ConnectionConsumer createConnectionConsumer(
      Queue queue, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
    checkNotClosed();
    throw new JMSException("Not implemented");
  }

  @Override
  public TopicSession createTopicSession(boolean b, int i) throws JMSException {
    return createSession(b, i).emulateLegacySession(false, true);
  }

  @Override
  public ConnectionConsumer createConnectionConsumer(
      Topic topic, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
    throw new JMSException("Not implemented");
  }

  void removeTemporaryDestination(PulsarTemporaryDestination pulsarTemporaryDestination) {
    temporaryDestinations.remove(pulsarTemporaryDestination);
  }

  public String getConnectionId() {
    return connectionId;
  }
}
