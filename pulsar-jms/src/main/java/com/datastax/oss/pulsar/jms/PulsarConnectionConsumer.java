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

import jakarta.jms.ConnectionConsumer;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.ServerSession;
import jakarta.jms.ServerSessionPool;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
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

  /**
   * This is a feature to help migrating ActiveMQ users to Pulsar. In ActiveMQ maxMessages
   * corresponds to the "prefetchSize" on the ConsumerInfo and this implies that it is a hard limit
   * on the maximum number of inflight messages. If you set maxMessages = 1 the MessageDrivenBean
   * will be processing only 1 message at a time.
   */
  private final boolean maxMessagesLimitParallelism;

  private final int closeTimeout;

  private final Semaphore permits;

  PulsarConnectionConsumer(
      PulsarSession dispatcherSession,
      PulsarMessageConsumer consumer,
      ServerSessionPool serverSessionPool,
      int maxMessages) {
    this.dispatcherSession = dispatcherSession;
    this.consumer = consumer;
    this.serverSessionPool = serverSessionPool;
    this.maxMessages = maxMessages;
    this.maxMessagesLimitParallelism =
        dispatcherSession.getConnection().getFactory().isMaxMessagesLimitsParallelism();
    this.closeTimeout =
        dispatcherSession.getConnection().getFactory().getConnectionConsumerStopTimeout();
    if (maxMessagesLimitParallelism) {
      permits = new Semaphore(maxMessages);
    } else {
      permits = null;
    }
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
      if (closeTimeout > 0) {
        this.spool.join(closeTimeout);
        if (this.spool.isAlive()) {
          log.warn(
              "Couldn't stop the ConnectionConsumer on {} within "
                  + "the configured jms.connectionConsumerStopTimeout={}",
              this.consumer.getDestination(),
              closeTimeout);
          this.spool.interrupt();
        }
      } else {
        this.spool.join();
      }
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
          if (permits != null) {
            try {
              permits.acquire();
            } catch (InterruptedException exit) {
              return;
            }
          }
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
          int maxMessagesToConsume = maxMessagesLimitParallelism ? 1 : maxMessages;

          List<Message> messages = consumer.batchReceive(maxMessagesToConsume, TIMEOUT_RECEIVE);

          // this session must have been created by this connection
          // it is a dummy session that is only a Holder for the MessageListener
          // that actually execute the MessageDriven bean code
          PulsarSession wrappedByServerSideSession = (PulsarSession) serverSession.getSession();
          Runnable postExecutionTask = null;
          if (permits != null) {
            postExecutionTask = () -> permits.release();
          }
          wrappedByServerSideSession.setupConnectionConsumerTask(messages, postExecutionTask);

          // serverSession.start() starts a new "Work" (using WorkManager) to
          // execute the Session.run() method of the dummy session wrapped by
          // the ServerSession, that happens on a separate thread
          serverSession.start();
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

  // visible for testing
  boolean isSpoolThreadAlive() {
    return spool.isAlive();
  }
}
