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

import javax.jms.InvalidDestinationException;
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
  protected PulsarDestination createSameType(String topicName) throws InvalidDestinationException {
    return new PulsarQueue(topicName);
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
  public String extractSubscriptionName(boolean prependTopicNameToCustomQueueSubscriptionName)
      throws InvalidDestinationException {

    // only valid cases
    // multi:persistent://public/default/queue:subscription
    // regexp:persistent://public/default/queue:subscription
    // regexp:non-persistent://public/default/queue:subscription
    // regexp:public/default/queue:subscription
    // regexp:queue:subscription
    // persistent://public/default/queue:subscription
    // non-persistent://public/default/queue:subscription
    // public/default/queue:subscription
    // queue:subscription

    // regexp:persistent://public/default/queue
    // regexp:non-persistent://public/default/queue
    // regexp:public/default/queue
    // regexp:queue
    // persistent://public/default/queue
    // non-persistent://public/default/queue
    // public/default/queue
    // queue

    String shortTopicName = topicName;

    if (shortTopicName.startsWith("multi:")) {
      shortTopicName = shortTopicName.substring("multi:".length());
    }

    if (shortTopicName.startsWith("regex:")) {
      shortTopicName = shortTopicName.substring("regex:".length());
    }
    int endSchema = shortTopicName.indexOf("://");
    if (endSchema > 0) {
      shortTopicName = shortTopicName.substring(endSchema + 3);
    }
    int lastSlash = shortTopicName.lastIndexOf('/');
    if (lastSlash > 0) {
      shortTopicName = shortTopicName.substring(lastSlash + 1);
    }

    // here we have only
    // queue
    // queue:subscription

    int pos = shortTopicName.lastIndexOf(":");
    if (pos < 0) {
      return null;
    }
    String subscriptionName = shortTopicName.substring(pos + 1);
    if (subscriptionName.isEmpty()) {
      throw new InvalidDestinationException("Subscription name cannot be empty");
    }
    if (prependTopicNameToCustomQueueSubscriptionName) {
      return shortTopicName;
    } else {
      return subscriptionName;
    }
  }

  /**
   * return the topic name, without the embedded subscription
   *
   * @return
   */
  public String getInternalTopicName() {
    String topicName = this.topicName;
    // regexp:persistent://public/default/queue:subscription
    // regexp:non-persistent://public/default/queue:subscription
    // regexp:public/default/queue:subscription
    // regexp:queue:subscription
    // persistent://public/default/queue:subscription
    // non-persistent://public/default/queue:subscription
    // public/default/queue:subscription
    // queue:subscription

    // regexp:persistent://public/default/queue
    // regexp:non-persistent://public/default/queue
    // regexp:public/default/queue
    // regexp:queue
    // persistent://public/default/queue
    // non-persistent://public/default/queue
    // public/default/queue
    // queue

    if (topicName.startsWith("regex:")) {
      topicName = topicName.substring("regex:".length());
    }
    if (topicName.startsWith("multi:")) {
      topicName = topicName.substring("multi:".length());
    }
    // no colon, early exit
    int pos = topicName.lastIndexOf(":");
    if (pos < 0) {
      return topicName;
    }

    int slash = topicName.lastIndexOf("/");
    if (slash < 0 || slash < pos) {
      return topicName.substring(0, pos);
    }
    return topicName;
  }
}
