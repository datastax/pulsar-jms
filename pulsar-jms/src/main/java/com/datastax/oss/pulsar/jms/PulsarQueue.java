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

import javax.jms.JMSException;
import javax.jms.Queue;

/** Represents a JMS Queue destination. In Pulsar there is no difference between Queue and Topic. */
public final class PulsarQueue extends PulsarDestination implements Queue {

  public PulsarQueue() {
    // Resource adapter
    this("unnamed");
  }

  public PulsarQueue(String topicName) {
    super(topicName);
  }

  @Override
  public String getQueueName() throws JMSException {
    return topicName;
  }

  @Override
  public boolean isQueue() {
    return true;
  }

  @Override
  public boolean isTopic() {
    return false;
  }

  @Override
  public String toString() {
    return "Queue{" + topicName + "}";
  }

  /**
   * Extract custom Queue Subscription Name
   *
   * @return the subscription name, if present
   */
  public String extractSubscriptionName() {
    int pos = topicName.lastIndexOf(":");
    if (pos < 0) {
      return null;
    }
    // only valid cases
    // queue:subscription
    // persistent://public/default/queue:subscription
    int slash = topicName.lastIndexOf("/");
    if (slash < 0 || slash < pos) {
      return topicName.substring(pos + 1);
    }
    return null;
  }

  public String getInternalTopicName() {
    int pos = topicName.lastIndexOf(":");
    if (pos < 0) {
      return topicName;
    }
    // only valid cases
    // queue:subscription
    // persistent://public/default/queue:subscription
    int slash = topicName.lastIndexOf("/");
    if (slash < 0 || slash < pos) {
      return topicName.substring(0, pos);
    }
    return topicName;
  }
}
