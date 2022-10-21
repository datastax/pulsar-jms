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
package com.datastax.oss.pulsar.jms.api;

import com.datastax.oss.pulsar.jms.PulsarDestination;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

/**
 * Admin API for JMS features. This is meant to be like an extension of the PulsarAdmin Java API.
 */
@Setter
public abstract class JMSDestinationMetadata {
  protected final PulsarDestination destination;

  public JMSDestinationMetadata(PulsarDestination destination) {
    this.destination = destination;
  }

  public abstract static class PhysicalPulsarTopicMetadata extends JMSDestinationMetadata {
    public PhysicalPulsarTopicMetadata(
        PulsarDestination destination,
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
  }

  public static final class TopicMetadata extends PhysicalPulsarTopicMetadata {

    public TopicMetadata(
        PulsarDestination destination,
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
  }

  public static final class QueueMetadata extends PhysicalPulsarTopicMetadata {
    public QueueMetadata(
        PulsarDestination destination,
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
  }

  public static final class VirtualDestinationMetadata extends JMSDestinationMetadata {
    public VirtualDestinationMetadata(
        PulsarDestination destination, List<JMSDestinationMetadata> destinations) {
      super(destination);
      this.destinations = destinations;
    }

    public final boolean isRegex() {
      return destination.isRegExp();
    }

    public final boolean isMultiTopic() {
      return destination.isMultiTopic();
    }

    private final List<JMSDestinationMetadata> destinations;

    public List<JMSDestinationMetadata> getDestinations() {
      return destinations;
    }
  }

  public final String getDestination() {
    return destination.getName();
  }

  public final boolean isQueue() {
    return destination.isQueue();
  }

  public final boolean isTopic() {
    return destination.isTopic();
  }

  public final boolean isVirtualDestination() {
    return destination.isVirtualDestination();
  }

  @Data
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

  @Data
  public static final class ConsumerMetadata {
    @Getter private final String consumerName;

    public ConsumerMetadata(String consumerName) {
      this.consumerName = consumerName;
    }

    private Map<String, String> metadata;
    private boolean enableFilters;
    private boolean enablePriority;
    private String selector;
    private String address;
    private String clientVersion;
  }

  @Data
  public static final class ProducerMetadata {
    @Getter private final String producerName;

    public ProducerMetadata(String producerName) {
      this.producerName = producerName;
    }

    private Map<String, String> metadata;
    private boolean enablePriority;
    private String address;
    private String clientVersion;
  }
}
