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
package com.datastax.oss.pulsar.jms.rar;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import com.datastax.oss.pulsar.jms.PulsarDestination;
import com.datastax.oss.pulsar.jms.PulsarMessage;
import com.datastax.oss.pulsar.jms.PulsarQueue;
import com.datastax.oss.pulsar.jms.PulsarTopic;
import java.lang.reflect.Method;
import java.util.Hashtable;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarMessageEndpoint implements MessageListener {

  private final PulsarConnectionFactory pulsarConnectionFactory;
  private final MessageEndpointFactory messageEndpointFactory;
  private final PulsarActivationSpec activationSpec;
  private final JMSContext context;

  private static final Method ON_MESSAGE;

  static {
    try {
      ON_MESSAGE = MessageListener.class.getMethod("onMessage", Message.class);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  public PulsarMessageEndpoint(
      PulsarConnectionFactory pulsarConnectionFactory,
      MessageEndpointFactory messageEndpointFactory,
      PulsarActivationSpec activationSpec) {
    this.pulsarConnectionFactory = pulsarConnectionFactory;
    this.messageEndpointFactory = messageEndpointFactory;
    this.activationSpec = activationSpec;
    this.context = pulsarConnectionFactory.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
  }

  public MessageEndpointFactory getMessageEndpointFactory() {
    return messageEndpointFactory;
  }

  public PulsarActivationSpec getActivationSpec() {
    return activationSpec;
  }

  public PulsarDestination getPulsarDestination(String destinationType, String destination) {
    if (destination != null && destination.startsWith("lookup://")) {
      try {
        String lookup = destination.substring(9);
        log.info("Lookup Destination from JNDI: '{}'", lookup);
        InitialContext contenxt = new InitialContext(new Hashtable<>());
        Destination destinationFromContext = (Destination) contenxt.lookup(lookup);
        log.info("Destination from JNDI: '{}': {}", lookup, destinationFromContext);
        return PulsarConnectionFactory.toPulsarDestination(destinationFromContext);
      } catch (NamingException | JMSException err) {
        throw new RuntimeException(err);
      }
    }
    if (destinationType == null || destinationType.toLowerCase().contains("queue")) {
      return new PulsarQueue(destination);
    } else {
      return new PulsarTopic(destination);
    }
  }

  public void start() {
    PulsarDestination pulsarDestination =
        getPulsarDestination(activationSpec.getDestinationType(), activationSpec.getDestination());
    if (pulsarDestination.isQueue()) {
      context.createConsumer(pulsarDestination).setMessageListener(this);
    } else {
      switch (activationSpec.getSubscriptionType()) {
        case "NonDurable":
          {
            switch (activationSpec.getSubscriptionMode()) {
              case "Exclusive":
                context.createConsumer(pulsarDestination).setMessageListener(this);
                return;
              case "Shared":
              default:
                context
                    .createSharedConsumer(
                        (Topic) pulsarDestination, activationSpec.getSubscriptionName())
                    .setMessageListener(this);
                return;
            }
          }
        case "Durable":
        default:
          switch (activationSpec.getSubscriptionMode()) {
            case "Exclusive":
              context
                  .createDurableConsumer(
                      (Topic) pulsarDestination, activationSpec.getSubscriptionName())
                  .setMessageListener(this);
              return;
            case "Shared":
            default:
              context
                  .createSharedDurableConsumer(
                      (Topic) pulsarDestination, activationSpec.getSubscriptionName())
                  .setMessageListener(this);
              return;
          }
      }
    }
  }

  public void stop() {
    context.close();
  }

  @Override
  public void onMessage(Message message) {
    PulsarMessage pulsarMessage = (PulsarMessage) message;
    MessageEndpoint handle;
    TransactionControlHandle txHandle = new TransactionControlHandle(pulsarMessage);
    try {
      handle = messageEndpointFactory.createEndpoint(txHandle);
    } catch (Exception err) {
      log.error("Cannot deliver message " + message + " - cannot create endpoint", err);
      throw new RuntimeException(err);
    }
    boolean processed = false;
    try {
      MessageListener endpoint = (MessageListener) handle;
      // here the Application server decides to start a transaction or not
      handle.beforeDelivery(ON_MESSAGE);
      try {
        endpoint.onMessage(message);
        processed = true;
      } finally {
        handle.afterDelivery();
        // the container did not start a transaction
        // we have to handle the message manually
        if (!txHandle.started) {
          if (processed) {
            pulsarMessage.acknowledge();
          } else {
            pulsarMessage.negativeAck();
          }
        }
      }
    } catch (Throwable err) {
      log.error("Cannot deliver message " + message + " to endpoint " + handle);
      throw new RuntimeException(err);
    } finally {
      handle.release();
    }
  }

  public boolean matches(
      MessageEndpointFactory messageEndpointFactory, ActivationSpec activationSpec) {
    return this.messageEndpointFactory == messageEndpointFactory
        && this.activationSpec == activationSpec;
  }

  @Slf4j
  private static class TransactionControlHandle implements XAResource {
    private final PulsarMessage message;
    private boolean started;

    public TransactionControlHandle(PulsarMessage message) {
      this.message = message;
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
      // we do not support XA transactions, simply acknowledge the message
      try {
        if (log.isDebugEnabled()) {
          log.debug("commit {} onePhase {} ack message {}", xid, onePhase, message);
        }
        message.acknowledge();
      } catch (JMSException err) {
        throw new XAException(err + "");
      }
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
    }

    @Override
    public void forget(Xid xid) throws XAException {
      throw new XAException("not implemented");
    }

    @Override
    public int getTransactionTimeout() throws XAException {
      return 0;
    }

    @Override
    public boolean isSameRM(XAResource xares) throws XAException {
      return xares.getClass() == this.getClass();
    }

    @Override
    public int prepare(Xid xid) throws XAException {
      return 0;
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
      throw new XAException("not implemented");
    }

    @Override
    public void rollback(Xid xid) throws XAException {
      try {
        message.negativeAck();
        if (log.isDebugEnabled()) {
          log.debug("rollback {} ack message {}", xid, message);
        }
      } catch (JMSException err) {
        throw new XAException(err + "");
      }
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
      return false;
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
      if (log.isDebugEnabled()) {
        log.debug("start {} flags {}", xid, flags);
      }
      started = true;
    }
  }
}
