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
package io.streamnative.oss.pulsar.jms.api;

import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** High level description of a JMS Destination. */
@Setter
@ToString
public abstract class JMSDestinationMetadata {
  private final String destination;

  public abstract boolean isQueue();

  public abstract boolean isTopic();

  public abstract boolean isVirtualDestination();

  protected JMSDestinationMetadata(String destination) {
    this.destination = destination;
  }

  /** The destination maps to a physical topic, partitioned or non-partitioned. */
  @ToString
  public abstract static class PhysicalPulsarTopicMetadata extends JMSDestinationMetadata {
    public PhysicalPulsarTopicMetadata(
        String destination,
        boolean exists,
        String pulsarTopic,
        List<ProducerMetadata> producers,
        int partitions) {
      super(destination);
      this.exists = exists;
      this.pulsarTopic = pulsarTopic;
      this.producers = producers;
      this.partitions = partitions;
    }

    private final boolean exists;
    private final String pulsarTopic;
    private final List<ProducerMetadata> producers;
    private final int partitions;

    public boolean isPartitioned() {
      return partitions > 0;
    }

    public String getPulsarTopic() {
      return pulsarTopic;
    }

    public boolean isExists() {
      return exists;
    }

    public int getPartitions() {
      return partitions;
    }

    public List<ProducerMetadata> getProducers() {
      return producers;
    }

    @Override
    public boolean isVirtualDestination() {
      return false;
    }
  }

  /** The destination is a JMS Topic, that maps to a Pulsar Topic with a set of Subscriptions. */
  @ToString
  public static final class TopicMetadata extends PhysicalPulsarTopicMetadata {

    public TopicMetadata(
        String destination,
        boolean exists,
        String pulsarTopic,
        List<ProducerMetadata> producers,
        int partitions,
        List<SubscriptionMetadata> subscriptions) {
      super(destination, exists, pulsarTopic, producers, partitions);
      this.subscriptions = subscriptions;
    }

    private final List<SubscriptionMetadata> subscriptions;

    public List<SubscriptionMetadata> getSubscriptions() {
      return subscriptions;
    }

    @Override
    public boolean isQueue() {
      return false;
    }

    @Override
    public boolean isTopic() {
      return true;
    }
  }

  /** The destination is a JMS Queue. A Queue is mapped to a single Pulsar Subscription. */
  @ToString
  public static final class QueueMetadata extends PhysicalPulsarTopicMetadata {
    public QueueMetadata(
        String destination,
        boolean exists,
        String pulsarTopic,
        List<ProducerMetadata> producers,
        int partitions,
        String queueSubscription,
        boolean queueSubscriptionExists,
        SubscriptionMetadata subscriptionMetadata) {
      super(destination, exists, pulsarTopic, producers, partitions);
      this.queueSubscription = queueSubscription;
      this.queueSubscriptionExists = queueSubscriptionExists;
      this.subscriptionMetadata = subscriptionMetadata;
    }

    private final String queueSubscription;

    private final boolean queueSubscriptionExists;

    private final SubscriptionMetadata subscriptionMetadata;

    public boolean isQueueSubscriptionExists() {
      return queueSubscriptionExists;
    }

    public String getQueueSubscription() {
      return queueSubscription;
    }

    public SubscriptionMetadata getSubscriptionMetadata() {
      return subscriptionMetadata;
    }

    @Override
    public boolean isQueue() {
      return true;
    }

    @Override
    public boolean isTopic() {
      return false;
    }
  }

  /** The Destination is a Virtual Destination, with the set of actual physical destinations. */
  @ToString
  public static final class VirtualDestinationMetadata extends JMSDestinationMetadata {
    private final boolean multiTopic;
    private final boolean regex;
    private final boolean queue;
    private final List<JMSDestinationMetadata> destinations;

    public VirtualDestinationMetadata(
        String destination,
        boolean queue,
        boolean multiTopic,
        boolean regex,
        List<JMSDestinationMetadata> destinations) {
      super(destination);
      this.destinations = destinations;
      this.queue = queue;
      this.regex = regex;
      this.multiTopic = multiTopic;
    }

    public boolean isRegex() {
      return regex;
    }

    public boolean isMultiTopic() {
      return multiTopic;
    }

    public List<JMSDestinationMetadata> getDestinations() {
      return destinations;
    }

    @Override
    public boolean isQueue() {
      return queue;
    }

    @Override
    public boolean isTopic() {
      return !queue;
    }

    @Override
    public boolean isVirtualDestination() {
      return true;
    }
  }

  public final String getDestination() {
    return destination;
  }

  /** Metadata about a Pulsar Subscription. */
  @Data
  @ToString
  public static final class SubscriptionMetadata {
    private final String subscriptionName;

    public SubscriptionMetadata(String subscriptionName) {
      this.subscriptionName = subscriptionName;
    }

    private Map<String, String> subscriptionProperties;
    private boolean enableFilters;
    private String selector;
    private List<ConsumerMetadata> consumers;
  }

  /** Metadata about a Pulsar Consumer. */
  @Data
  @ToString
  public static final class ConsumerMetadata {
    @Getter private final String consumerName;

    public ConsumerMetadata(String consumerName) {
      this.consumerName = consumerName;
    }

    private String pulsarTopic;
    private String subscriptionName;
    private String acknowledgeMode;
    private Map<String, String> metadata;
    private boolean enableFilters;
    private boolean enablePriority;
    private String selector;
    private String address;
    private String clientVersion;
  }

  /** Metadata about a Pulsar Producer. */
  @Data
  @ToString
  public static final class ProducerMetadata {
    @Getter private final String producerName;

    public ProducerMetadata(String producerName) {
      this.producerName = producerName;
    }

    private String pulsarTopic;
    private Map<String, String> metadata;
    private boolean enablePriority;
    private boolean transacted;
    private String priorityMapping;
    private String address;
    private String clientVersion;
  }

  /**
   * Utility method to convert to a specific subclass
   *
   * @param clazz the desired class
   * @return this object
   * @param <T> the type
   */
  public <T extends JMSDestinationMetadata> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(this)) {
      return (T) this;
    }
    throw new IllegalArgumentException(
        "A instance of " + this.getClass() + " cannot be converted to " + clazz);
  }
}
