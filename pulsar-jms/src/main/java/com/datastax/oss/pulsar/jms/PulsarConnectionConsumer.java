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

import javax.jms.ConnectionConsumer;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;

class PulsarConnectionConsumer implements ConnectionConsumer {
  private final PulsarMessageConsumer consumer;
  private final ServerSessionPool serverSessionPool;
  private volatile boolean closed;

  PulsarConnectionConsumer(PulsarMessageConsumer consumer, ServerSessionPool serverSessionPool) {
    this.consumer = consumer;
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
    consumer.close();
  }
}
