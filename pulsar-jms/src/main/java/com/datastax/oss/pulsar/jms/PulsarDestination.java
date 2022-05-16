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

import java.util.Objects;
import javax.jms.Destination;

public abstract class PulsarDestination implements Destination {
  protected String topicName;

  protected PulsarDestination(String topicName) {
    this.topicName = Objects.requireNonNull(topicName);
  }

  public String getName() {
    return topicName;
  }

  public void setName(String name) {
    this.topicName = name;
  }

  /**
   * Extract custom Queue Subscription Name. This feature does not apply to JMS Topics.
   *
   * @return the subscription name, if present.
   */
  public String extractSubscriptionName() {
    return null;
  }

  public String getInternalTopicName() {
    return topicName;
  }

  public abstract boolean isQueue();

  public abstract boolean isTopic();

  public final boolean equals(Object other) {
    if (!(other instanceof PulsarDestination)) {
      return false;
    }
    PulsarDestination o = (PulsarDestination) other;
    return Objects.equals(o.topicName, this.topicName)
        && Objects.equals(o.isQueue(), this.isQueue())
        && Objects.equals(o.isTopic(), this.isTopic());
  }

  public final int hashCode() {
    return topicName.hashCode();
  }
}
