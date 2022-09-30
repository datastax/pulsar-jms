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
import javax.jms.JMSConsumer;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarPriorityAwareMessageConsumer implements IPulsarMessageConsumer {

  private final List<PulsarMessageConsumer> consumers;
  private final String selector;
  private final Status status;

  public PulsarPriorityAwareMessageConsumer(List<PulsarMessageConsumer> consumers, String selector)
      throws JMSException {
    this.consumers = consumers;
    this.selector = selector;
    this.status = new Status(consumers.size());
  }

  @Override
  public synchronized String getMessageSelector() throws JMSException {
    checkNotClosed();
    return selector;
  }

  @Override
  public synchronized MessageListener getMessageListener() throws JMSException {
    return getFirst().getMessageListener();
  }

  private PulsarMessageConsumer getFirst() {
    return consumers.get(0);
  }

  synchronized void checkNotClosed() throws JMSException {
    getFirst().checkNotClosed();
  }

  @Override
  public synchronized void setMessageListener(MessageListener listener) throws JMSException {
    checkNotClosed();
    for (MessageConsumer consumer : consumers) {
      consumer.setMessageListener(listener);
    }
  }

  static final class Status {
    private final int maxConsumers;
    private int countCurrentConsumer;
    private int currentConsumer;

    Status(int maxConsumers) {
      this.maxConsumers = maxConsumers;
    }

    synchronized void matched() {
      countCurrentConsumer++;
      if (countCurrentConsumer >= 10) {
        currentConsumer = 0;
        countCurrentConsumer = 0;
      }
    }

    synchronized void notMatched() {
      currentConsumer++;
      if (currentConsumer >= maxConsumers) {
        currentConsumer = 0;
      }
      countCurrentConsumer = 0;
    }

    synchronized int currentConsumer() {
      return currentConsumer;
    }
  }

  @Override
  public Message receive() throws JMSException {
    while (true) {
      PulsarMessageConsumer pulsarMessageConsumer = consumers.get(status.currentConsumer());
      Message message = pulsarMessageConsumer.receive(1000);
      if (message != null) {
        status.matched();
        return message;
      } else {
        status.notMatched();
      }
    }
  }

  @Override
  public Message receive(long timeout) throws JMSException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      PulsarMessageConsumer pulsarMessageConsumer = consumers.get(status.currentConsumer());
      Message message = pulsarMessageConsumer.receive(1);
      if (message != null) {
        status.matched();
        return message;
      } else {
        status.notMatched();
      }
    }
    return null;
  }

  @Override
  public Message receiveNoWait() throws JMSException {
    PulsarMessageConsumer pulsarMessageConsumer = consumers.get(status.currentConsumer());
    Message message = pulsarMessageConsumer.receive(1);
    if (message != null) {
      status.matched();
      return message;
    } else {
      status.notMatched();
    }
    return null;
  }

  @Override
  public synchronized void close() throws JMSException {
    JMSException firstError = null;
    for (PulsarMessageConsumer consumer : consumers) {
      try {
        consumer.close();
      } catch (JMSException err) {
        if (firstError == null) {
          firstError = err;
        } else {
          firstError.addSuppressed(err);
        }
      }
    }
    if (firstError != null) {
      throw firstError;
    }
  }

  @Override
  public String toString() {
    PulsarMessageConsumer first = getFirst();
    return "PulsarPriorityAwareMessageConsumer{subscriptionName="
        + first.subscriptionName
        + ", destination="
        + first.getDestination()
        + '}';
  }

  @Override
  public synchronized Topic getTopic() throws JMSException {
    checkNotClosed();
    return getFirst().getTopic();
  }

  @Override
  public synchronized Queue getQueue() throws JMSException {
    checkNotClosed();
    return getFirst().getQueue();
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
  public synchronized boolean getNoLocal() throws JMSException {
    checkNotClosed();
    return getFirst().getNoLocal();
  }

  @Override
  public JMSConsumer asJMSConsumer() {
    throw new UnsupportedOperationException();
  }
}
