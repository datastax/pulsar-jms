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
import javax.jms.ConnectionConsumer;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;

class PulsarConnectionConsumer implements ConnectionConsumer {
  private final List<PulsarMessageConsumer> consumers;
  private final PulsarSession dispatcherSession;
  private final ServerSessionPool serverSessionPool;
  private volatile boolean closed;

  PulsarConnectionConsumer(
      PulsarSession dispatcherSession,
      List<PulsarMessageConsumer> consumers,
      ServerSessionPool serverSessionPool) {
    this.dispatcherSession = dispatcherSession;
    this.consumers = consumers;
    this.serverSessionPool = serverSessionPool;
  }

  @Override
  public ServerSessionPool getServerSessionPool() throws JMSException {
    if (closed) {
      throw new IllegalStateException("The Connection Consumer is closed");
    }
    return serverSessionPool;
  }

  @Override
  public void close() throws JMSException {
    closed = true;
    for (PulsarMessageConsumer consumer : consumers) {
      try {
        consumer.close();
      } catch (JMSException ignore) {

      }
    }
    dispatcherSession.close();
  }
}
