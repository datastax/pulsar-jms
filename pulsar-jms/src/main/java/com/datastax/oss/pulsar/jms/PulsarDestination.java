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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;

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
  public String extractSubscriptionName() throws InvalidDestinationException {
    return null;
  }

  public String getInternalTopicName() throws InvalidDestinationException {
    if (isRegExp()) {
      return topicName.substring("regex:".length());
    }
    if (isMultiTopic()) {
      throw new InvalidDestinationException(
          "Cannot get internal topic name for a multi-topic destination");
    }
    return topicName;
  }

  public final boolean isVirtualDestination() {
    return isRegExp() || isMultiTopic();
  }

  public boolean isRegExp() {
    return topicName.startsWith("regex:");
  }

  public boolean isMultiTopic() {
    return topicName.startsWith("multi:");
  }

  public List<PulsarDestination> getDestinations() throws InvalidDestinationException {
    if (!isMultiTopic()) {
      return Collections.singletonList(this);
    }
    String withoutPrefix = topicName.substring("multi:".length());
    if (withoutPrefix.isEmpty()) {
      throw new InvalidDestinationException("Invalid destination " + topicName);
    }
    String customSubscription = extractSubscriptionName();
    if (customSubscription != null) {
      withoutPrefix =
          withoutPrefix.substring(0, withoutPrefix.length() - customSubscription.length() - 1);
    }
    String[] split = withoutPrefix.split(",");
    List<PulsarDestination> destinations = new ArrayList<>(split.length);
    for (String part : split) {
      if (part.isEmpty()) {
        throw new InvalidDestinationException("Invalid destination " + topicName);
      }
      if (customSubscription != null) {
        destinations.add(createSameType(part + ":" + customSubscription));
      } else {
        destinations.add(createSameType(part));
      }
    }
    if (destinations.isEmpty()) {
      throw new InvalidDestinationException("Invalid destination " + topicName);
    }
    return destinations;
  }

  public PulsarDestination createSameType(String topicName) throws InvalidDestinationException {
    throw new InvalidDestinationException(
        "Multi topic syntax is not allowed " + "for this kind of destination (" + getClass() + ")");
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
