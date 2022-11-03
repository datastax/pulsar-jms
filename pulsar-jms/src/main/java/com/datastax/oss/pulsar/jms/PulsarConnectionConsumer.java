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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.ConnectionConsumer;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class PulsarConnectionConsumer implements ConnectionConsumer {

  private static final long TIMEOUT_RECEIVE = 1000L;

  private final PulsarMessageConsumer consumer;
  private final PulsarSession dispatcherSession;
  private final ServerSessionPool serverSessionPool;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final Thread spool;
  private final int maxMessages;

  PulsarConnectionConsumer(
      PulsarSession dispatcherSession,
      PulsarMessageConsumer consumer,
      ServerSessionPool serverSessionPool,
      int maxMessages) {
    this.dispatcherSession = dispatcherSession;
    this.consumer = consumer;
    this.serverSessionPool = serverSessionPool;
    this.maxMessages = maxMessages;

    // unfortunately due to the blocking nature of ServerSessionPool.getServerSession()
    // we must
    this.spool = new Thread(new Spool());
    this.spool.setDaemon(true);
    this.spool.setName(
        "jms-connection-consumer-" + serverSessionPool + "-" + consumer.getDestination().getName());
  }

  public void start() {
    spool.start();
  }

  @Override
  public ServerSessionPool getServerSessionPool() throws JMSException {
    if (closed.get()) {
      throw new IllegalStateException("The Connection Consumer is closed");
    }
    return serverSessionPool;
  }

  @Override
  public void close() throws JMSException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    try {
      this.spool.join();
    } catch (InterruptedException err) {
      Utils.handleException(err);
    }
    this.consumer.close();
    this.dispatcherSession.close();
  }

  private class Spool implements Runnable {

    @Override
    public void run() {
      while (!closed.get()) {
        try {
          // this method may be "blocking" if the pool is exhausted
          ServerSession serverSession;
          try {
            serverSession = serverSessionPool.getServerSession();
          } catch (JMSException internalContainerError) {
            log.error("Container error", internalContainerError);
            break;
          }
          // pick a message after getting the ServerSession
          // otherwise the ackTimeout will make the message be negatively acknowledged
          // while waiting for the ServerSession to be available
          List<Message> messages = consumer.batchReceive(maxMessages, TIMEOUT_RECEIVE);
          if (!messages.isEmpty()) {
            // this session must have been created by this connection
            // it is a dummy session that is only a Holder for the MessageListener
            // that actually execute the MessageDriven bean code
            PulsarSession wrappedByServerSideSession = (PulsarSession) serverSession.getSession();

            wrappedByServerSideSession.setupConnectionConsumerTask(messages);

            // serverSession.start() starts a new "Work" (using WorkManager) to
            // execute the Session.run() method of the dummy session wrapped by
            // the ServerSession, that happens on a separate thread
            serverSession.start();
          } else {
            // retun the session to the pool
            serverSession.start();
          }
        } catch (JMSException error) {
          log.error("internal error", error);

          // back-off
          try {
            Thread.sleep(1000);
          } catch (InterruptedException stopThread) {
            break;
          }
        }
      }
    }
  }
}
