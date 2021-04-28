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
import com.datastax.oss.pulsar.jms.PulsarMessage;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.UnavailableException;
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
  private final javax.jms.MessageListener endpoint;
  private final TransactionControlHandle handle;

  public PulsarMessageEndpoint(
      PulsarConnectionFactory pulsarConnectionFactory,
      MessageEndpointFactory messageEndpointFactory,
      PulsarActivationSpec activationSpec)
      throws UnavailableException {
    this.pulsarConnectionFactory = pulsarConnectionFactory;
    this.messageEndpointFactory = messageEndpointFactory;
    this.activationSpec = activationSpec;
    this.context = pulsarConnectionFactory.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
    this.handle = new TransactionControlHandle();
    this.endpoint = (MessageListener) messageEndpointFactory.createEndpoint(handle);
  }

  public void start() {
    context.createConsumer(activationSpec.getPulsarDestination()).setMessageListener(this);
  }

  public void stop() {
    context.close();
  }

  @Override
  public void onMessage(Message message) {
    PulsarMessage pulsarMessage = (PulsarMessage) message;
    try {
      handle.bindToMessage(pulsarMessage);
      endpoint.onMessage(message);
    } catch (Throwable err) {
      log.error("Cannot deliver message " + message + " to endpoint " + handle);
      throw new RuntimeException(err);
    } finally {
      handle.bindToMessage(null);
    }
  }

  public boolean matches(
      MessageEndpointFactory messageEndpointFactory, ActivationSpec activationSpec) {
    return this.messageEndpointFactory == messageEndpointFactory
        && this.activationSpec == activationSpec;
  }

  private static class TransactionControlHandle implements XAResource {
    private PulsarMessage message;

    public TransactionControlHandle() {}

    public void bindToMessage(PulsarMessage message) {
      this.message = message;
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
      if (message == null) {
        throw new XAException("no current message");
      }
      // we do not support XA transactions, simply acknowledge the message
      try {
        message.acknowledge();
        message = null;
      } catch (JMSException err) {
        throw new XAException(err + "");
      }
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {}

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
      if (message == null) {
        throw new XAException("no current message");
      }
      try {
        message.negativeAck();
        message = null;
      } catch (JMSException err) {
        throw new XAException(err + "");
      }
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
      return false;
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {}
  }
}
