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
import javax.jms.Topic;

/** Represents a JMS Topic destination. In Pulsar there is no difference between Queue and Topic. */
public final class PulsarTopic extends PulsarDestination implements Topic {

  public PulsarTopic() {
    // Resource adapter
    this("unnamed");
  }

  public PulsarTopic(String topicName) {
    super(topicName);
  }

  @Override
  public String getTopicName() throws JMSException {
    return topicName;
  }

  @Override
  public boolean isQueue() {
    return false;
  }

  @Override
  public boolean isTopic() {
    return true;
  }

  @Override
  public PulsarDestination createSameType(String topicName) throws InvalidDestinationException {
    return new PulsarTopic(topicName);
  }

  @Override
  public String toString() {
    return "Topic{" + topicName + "}";
  }
}
