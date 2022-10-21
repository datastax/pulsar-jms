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
public class JMSDestinationMetadata {
  private final PulsarDestination destination;

  public JMSDestinationMetadata(PulsarDestination destination) {
    this.destination = destination;
  }

  private boolean exists;
  private String pulsarTopic;
  private String queueSubscription;

  private boolean queueSubscriptionExists;

  public boolean isQueueSubscriptionExists() {
    return queueSubscriptionExists;
  }

  public boolean isExists() {
    return exists;
  }

  public String getPulsarTopic() {
    return pulsarTopic;
  }

  public String getQueueSubscription() {
    return queueSubscription;
  }

  public int getPartitions() {
    return partitions;
  }

  public List<JMSDestinationMetadata> getDestinations() {
    return destinations;
  }

  public List<SubscriptionMetadata> getSubscriptions() {
    return subscriptions;
  }

  public List<ProducerMetadata> getProducers() {
    return producers;
  }

  private int partitions;

  public String getDestination() {
    return destination.getName();
  }

  public boolean isPartitioned() {
    return partitions > 0;
  }

  public boolean isQueue() {
    return destination.isQueue();
  }

  public boolean isTopic() {
    return destination.isTopic();
  }

  public boolean isVirtualDestination() {
    return destination.isVirtualDestination();
  }

  public boolean isRegex() {
    return destination.isRegExp();
  }

  public boolean isMultiTopic() {
    return destination.isMultiTopic();
  }

  private List<JMSDestinationMetadata> destinations;

  private List<SubscriptionMetadata> subscriptions;

  private List<ProducerMetadata> producers;

  @Data
  public static class SubscriptionMetadata {
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
  public static class ConsumerMetadata {
    @Getter
    private final String consumerName;

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
  public static class ProducerMetadata {
    @Getter
    private final String producerName;

    public ProducerMetadata(String producerName) {
      this.producerName = producerName;
    }

    private Map<String, String> metadata;
    private boolean enablePriority;
    private String address;
    private String clientVersion;
  }
}
