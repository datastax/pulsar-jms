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

import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.TemporaryQueue;

class PulsarTemporaryQueue extends PulsarTemporaryDestination implements TemporaryQueue {

  public PulsarTemporaryQueue(String topicName, PulsarSession session)
      throws InvalidDestinationException {
    super(topicName, session);
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
  public String getQueueName() throws JMSException {
    return topicName;
  }

  @Override
  public String toString() {
    return "TemporaryQueue{" + topicName + "}";
  }
}
