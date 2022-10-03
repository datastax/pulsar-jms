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
import org.apache.pulsar.client.impl.ConsumerBase;

@Slf4j
public class PulsarPriorityAwareMessageConsumer implements IPulsarMessageConsumer {

  private final PulsarMessageConsumer defaultConsumer;
  private final List<PulsarMessageConsumer> consumers;
  private final Status status;

  public PulsarPriorityAwareMessageConsumer(PulsarMessageConsumer defaultConsumer,
                                            List<PulsarMessageConsumer> consumers)
      throws JMSException {
    this.consumers = consumers;
    this.defaultConsumer = defaultConsumer;
    this.status = new Status(consumers.size());
  }

  @Override
  public synchronized String getMessageSelector() throws JMSException {
    return getDefaultConsumer().getMessageSelector();
  }

  @Override
  public synchronized MessageListener getMessageListener() throws JMSException {
    return getDefaultConsumer().getMessageListener();
  }

  private PulsarMessageConsumer getDefaultConsumer() {
    return consumers.get(0);
  }

  synchronized void checkNotClosed() throws JMSException {
    getDefaultConsumer().checkNotClosed();
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
      ConsumerBase consumerBase = (ConsumerBase) pulsarMessageConsumer.getConsumer();
      if (consumerBase.getTotalIncomingMessages() <= 0) {
        status.notMatched();
      } else {
        Message message = pulsarMessageConsumer.receive(1000);
        if (message != null) {
          status.matched();
          return message;
        } else {
          status.notMatched();
        }
      }
    }
  }

  @Override
  public Message receive(long timeout) throws JMSException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      PulsarMessageConsumer pulsarMessageConsumer = consumers.get(status.currentConsumer());
      ConsumerBase consumerBase = (ConsumerBase) pulsarMessageConsumer.getConsumer();
      if (consumerBase.getTotalIncomingMessages() <= 0) {
        status.notMatched();
      } else {
        Message message = pulsarMessageConsumer.receive(1);
        if (message != null) {
          status.matched();
          return message;
        } else {
          status.notMatched();
        }
      }
    }
    return null;
  }

  @Override
  public Message receiveWithTimeoutAndValidateType(long timeout, Class expectedType)
      throws JMSException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      PulsarMessageConsumer pulsarMessageConsumer = consumers.get(status.currentConsumer());
      ConsumerBase consumerBase = (ConsumerBase) pulsarMessageConsumer.getConsumer();
      if (consumerBase.getTotalIncomingMessages() <= 0) {
        status.notMatched();
      } else {
        Message message = pulsarMessageConsumer.receiveWithTimeoutAndValidateType(1, expectedType);
        if (message != null) {
          status.matched();
          return message;
        } else {
          status.notMatched();
        }
      }
    }
    return null;
  }

  @Override
  public Message receiveNoWait() throws JMSException {
    return receive(1);
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
    PulsarMessageConsumer first = getDefaultConsumer();
    return "PulsarPriorityAwareMessageConsumer{subscriptionName="
        + first.subscriptionName
        + ", destination="
        + first.getDestination()
        + '}';
  }

  @Override
  public synchronized Topic getTopic() throws JMSException {
    checkNotClosed();
    return getDefaultConsumer().getTopic();
  }

  @Override
  public synchronized Queue getQueue() throws JMSException {
    checkNotClosed();
    return getDefaultConsumer().getQueue();
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
    return getDefaultConsumer().getNoLocal();
  }

  @Override
  public JMSConsumer asJMSConsumer() {
    return new PulsarJMSConsumer(this);
  }
}
