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

import com.datastax.oss.pulsar.jms.api.JMSAdmin;
import com.datastax.oss.pulsar.jms.api.JMSDestinationMetadata;
import com.datastax.oss.pulsar.jms.selectors.SelectorSupport;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;

@Slf4j
class PulsarJMSAdminImpl implements JMSAdmin {
  private final PulsarConnectionFactory factory;

  PulsarJMSAdminImpl(PulsarConnectionFactory factory) {
    this.factory = factory;
  }

  @Override
  public Queue getQueue(String queue) throws JMSException {
    return new PulsarQueue(queue);
  }

  @Override
  public Topic getTopic(String topic) throws JMSException {
    return new PulsarTopic(topic);
  }

  @Override
  public JMSDestinationMetadata describe(Destination dest) throws JMSException {
    PulsarDestination destination = PulsarConnectionFactory.toPulsarDestination(dest);

    if (destination.isMultiTopic()) {
      JMSDestinationMetadata result = new JMSDestinationMetadata(destination);
      List<JMSDestinationMetadata> subDestinationsMetadata = new ArrayList<>();
      result.setDestinations(subDestinationsMetadata);
      List<PulsarDestination> destinations = destination.getDestinations();
      if (destinations.isEmpty()) {
        return result;
      }
      result.setExists(true);
      for (PulsarDestination sub : destinations) {
        JMSDestinationMetadata subDestinationMetadata = describe(sub);
        subDestinationsMetadata.add(subDestinationMetadata);
      }
      return result;
    } else if (destination.isRegExp()) {
      JMSDestinationMetadata result = new JMSDestinationMetadata(destination);
      List<JMSDestinationMetadata> subDestinationsMetadata = new ArrayList<>();
      result.setDestinations(subDestinationsMetadata);
      PulsarClient pulsarClient = factory.ensureClient();
      String topicName = factory.getPulsarTopicName(destination);
      List<String> topics =
          TopicDiscoveryUtils.discoverTopicsByPattern(topicName, pulsarClient, 10000);
      if (topics.isEmpty()) {
        return result;
      }
      result.setExists(true);
      String customSubscription = destination.extractSubscriptionName();
      for (String topic : topics) {
        if (customSubscription != null) {
          topic = topic + ":" + customSubscription;
        }
        PulsarDestination sub = destination.createSameType(topic);
        JMSDestinationMetadata subDestinationMetadata = describe(sub);
        subDestinationsMetadata.add(subDestinationMetadata);
      }
      return result;
    } else {
      return describeDestination(destination);
    }
  }

  private JMSDestinationMetadata describeDestination(PulsarDestination destination)
      throws JMSException {
    JMSDestinationMetadata result = new JMSDestinationMetadata(destination);
    PulsarAdmin pulsarAdmin = factory.getPulsarAdmin();
    String topicName = factory.getPulsarTopicName(destination);
    result.setPulsarTopic(topicName);
    if (destination.isQueue()) {
      String subscription = factory.getQueueSubscriptionName(destination);
      result.setQueueSubscription(subscription);
    }
    Map<String, ? extends SubscriptionStats> subscriptions;
    List<? extends PublisherStats> publishers;
    PartitionedTopicMetadata partitionedTopicMetadata;
    try {
      partitionedTopicMetadata = pulsarAdmin.topics().getPartitionedTopicMetadata(topicName);
      result.setExists(true);
    } catch (PulsarAdminException.NotFoundException notFound) {
      result.setExists(false);
      return result;
    } catch (PulsarAdminException err) {
      throw Utils.handleException(err);
    }
    result.setPartitions(partitionedTopicMetadata.partitions);

    try {
      if (partitionedTopicMetadata.partitions > 0) {
        PartitionedTopicStats partitionedStats =
            pulsarAdmin.topics().getPartitionedStats(topicName, false);
        subscriptions = partitionedStats.getSubscriptions();
        publishers = partitionedStats.getPublishers();
      } else {
        TopicStats stats = pulsarAdmin.topics().getStats(topicName);
        subscriptions = stats.getSubscriptions();
        publishers = stats.getPublishers();
      }
    } catch (PulsarAdminException err) {
      throw Utils.handleException(err);
    }

    if (subscriptions == null) {
      subscriptions = Collections.emptyMap();
    }
    if (publishers == null) {
      publishers = Collections.emptyList();
    }

    List<JMSDestinationMetadata.SubscriptionMetadata> subscriptionMetadataList = new ArrayList<>();
    result.setSubscriptions(subscriptionMetadataList);
    if (destination.isQueue()) {
      result.setQueueSubscriptionExists(subscriptions.containsKey(result.getQueueSubscription()));
    }
    subscriptions.forEach(
        (name, sub) -> {
          if (destination.isQueue() && !name.equals(result.getQueueSubscription())) {
            // this is a JMS Queue, skip other subscriptions
            return;
          }

          JMSDestinationMetadata.SubscriptionMetadata md =
              new JMSDestinationMetadata.SubscriptionMetadata(name);
          subscriptionMetadataList.add(md);
          Map<String, String> subscriptionProperties =
              sub.getSubscriptionProperties() != null
                  ? sub.getSubscriptionProperties()
                  : Collections.emptyMap();
          md.setSubscriptionProperties(subscriptionProperties);

          String jmsFiltering = subscriptionProperties.getOrDefault("jms.filtering", "false");
          if ("true".equals(jmsFiltering)) {
            md.setEnableFilters(true);
            String jmsSelector = subscriptionProperties.getOrDefault("jms.selector", "");
            md.setSelector(jmsSelector);
          }
          List<JMSDestinationMetadata.ConsumerMetadata> consumers = new ArrayList<>();
          md.setConsumers(consumers);
          sub.getConsumers()
              .forEach(
                  c -> {
                    JMSDestinationMetadata.ConsumerMetadata cmd =
                        new JMSDestinationMetadata.ConsumerMetadata(c.getConsumerName());
                    consumers.add(cmd);
                    cmd.setAddress(c.getAddress());
                    cmd.setClientVersion(c.getClientVersion());
                    Map<String, String> metadata =
                        c.getMetadata() != null ? c.getMetadata() : Collections.emptyMap();
                    cmd.setMetadata(
                        c.getMetadata() != null ? c.getMetadata() : Collections.emptyMap());
                    String jmsConsumerFiltering = metadata.getOrDefault("jms.filtering", "false");
                    if ("true".equals(jmsConsumerFiltering)) {
                      cmd.setEnableFilters(true);
                      String jmsSelector = metadata.getOrDefault("jms.selector", "");
                      cmd.setSelector(jmsSelector);
                    }
                    String jmsConsumerPriority = metadata.getOrDefault("jms.priority", "");
                    cmd.setEnablePriority(jmsConsumerPriority.equals("enabled"));
                  });
        });

    List<JMSDestinationMetadata.ProducerMetadata> producerMetadataList = new ArrayList<>();
    result.setProducers(producerMetadataList);
    publishers.forEach(
        p -> {
          JMSDestinationMetadata.ProducerMetadata cmd =
              new JMSDestinationMetadata.ProducerMetadata(p.getProducerName());
          producerMetadataList.add(cmd);
          cmd.setAddress(p.getAddress());
          cmd.setClientVersion(p.getClientVersion());
          Map<String, String> metadata =
              p.getMetadata() != null ? p.getMetadata() : Collections.emptyMap();
          cmd.setMetadata(metadata);
          String jmsConsumerPriority = metadata.getOrDefault("jms.priority", "");
          cmd.setEnablePriority(jmsConsumerPriority.equals("enabled"));
        });
    return result;
  }

  void checkDestination(
      Destination destination, Function<Destination, Boolean> condition, String message)
      throws JMSException {
    if (!condition.apply(destination)) {
      throw new InvalidDestinationException(message);
    }
  }

  void checkArgument(Supplier<Boolean> condition, String message) throws JMSException {
    if (!condition.get()) {
      throw new IllegalStateException(message);
    }
  }

  void validateSelector(boolean enableFilters, String selector) throws JMSException {
    if (enableFilters) {
      SelectorSupport.build(selector, true);
    }
  }

  @Override
  public void createSubscription(
      Topic destination,
      String subscriptionName,
      boolean enableFilters,
      String selector,
      boolean fromBeginning)
      throws JMSException {
    try {
      PulsarDestination dest = PulsarConnectionFactory.toPulsarDestination(destination);
      validateSelector(enableFilters, selector);
      Map<String, String> properties = new HashMap<>();
      if (enableFilters) {
        properties.put("jms.filtering", "true");
        properties.put("jms.selector", selector);
      }
      String topicName = factory.getPulsarTopicName(dest);
      Topics topics = factory.getPulsarAdmin().topics();
      topics.createSubscription(
          topicName,
          subscriptionName,
          fromBeginning ? MessageId.earliest : MessageId.latest,
          false,
          properties);
    } catch (PulsarAdminException error) {
      throw Utils.handleException(error);
    }
  }

  @Override
  public void createQueue(Queue destination, int partitions, boolean enableFilters, String selector)
      throws JMSException {
    checkArgument(() -> partitions >= 0, "Invalid number of partitions " + partitions);
    validateSelector(enableFilters, selector);
    try {
      PulsarDestination dest = PulsarConnectionFactory.toPulsarDestination(destination);
      checkDestination(
          destination, d -> !dest.isVirtualDestination(), "Cannot create a VirtualDestination");

      String topicName = factory.getPulsarTopicName(dest);
      Topics topics = factory.getPulsarAdmin().topics();
      boolean exists = false;
      try {
        PartitionedTopicMetadata partitionedTopicMetadata =
            topics.getPartitionedTopicMetadata(topicName);
        checkDestination(
            destination,
            d -> partitionedTopicMetadata.partitions != partitions,
            "Destination exists and it has a different number of partitions "
                + partitionedTopicMetadata.partitions
                + " is different from "
                + partitions);
        exists = true;
      } catch (PulsarAdminException.NotFoundException notFound) {
        // ok
      }
      String subscriptionName = factory.getQueueSubscriptionName(dest);
      if (!exists) {
        if (partitions > 0) {
          topics.createPartitionedTopic(topicName, partitions);
        } else {
          topics.createNonPartitionedTopic(topicName);
        }
      }
      Map<String, String> properties = new HashMap<>();
      if (enableFilters) {
        properties.put("jms.filtering", "true");
        properties.put("jms.selector", selector);
      }
      try {
        topics.createSubscription(
            topicName, subscriptionName, MessageId.earliest, false, properties);
      } catch (PulsarAdminException.ConflictException alreadyExists) {
        log.debug("Already exists", alreadyExists);
        throw new InvalidDestinationException(
            "Subscription " + subscriptionName + " already exists on Pulsar Topic " + topicName);
      }
    } catch (PulsarAdminException error) {
      throw Utils.handleException(error);
    }
  }

  @Override
  public void createTopic(Topic destination, int partitions) throws JMSException {
    checkArgument(() -> partitions >= 0, "Invalid number of partitions " + partitions);
    try {
      PulsarDestination dest = PulsarConnectionFactory.toPulsarDestination(destination);
      checkDestination(
          destination, d -> !dest.isVirtualDestination(), "Cannot create a VirtualDestination");

      String topicName = factory.getPulsarTopicName(dest);
      Topics topics = factory.getPulsarAdmin().topics();
      try {
        PartitionedTopicMetadata partitionedTopicMetadata =
            topics.getPartitionedTopicMetadata(topicName);
        checkDestination(
            destination,
            d -> partitionedTopicMetadata.partitions != partitions,
            "Destination exists and it has a different number of partitions "
                + partitionedTopicMetadata.partitions
                + " is different from "
                + partitions);
      } catch (PulsarAdminException.NotFoundException notFound) {
        // ok
      }
      if (partitions > 0) {
        topics.createPartitionedTopic(topicName, partitions);
      } else {
        topics.createNonPartitionedTopic(topicName);
      }
    } catch (PulsarAdminException error) {
      throw Utils.handleException(error);
    }
  }

  @Override
  public void setSubscriptionSelector(Queue destination, boolean enableFilters, String selector)
      throws JMSException {
    try {
      PulsarDestination dest = PulsarConnectionFactory.toPulsarDestination(destination);
      String topicName = factory.getPulsarTopicName(dest);
      String subscriptionName = factory.getQueueSubscriptionName(dest);
      doUpdateSubscriptionSelector(enableFilters, selector, topicName, subscriptionName);
    } catch (PulsarAdminException error) {
      throw Utils.handleException(error);
    }
  }

  private void doUpdateSubscriptionSelector(
      boolean enableFilters, String selector, String topicName, String subscriptionName)
      throws JMSException, PulsarAdminException {
    validateSelector(enableFilters, selector);
    Topics topics = factory.getPulsarAdmin().topics();
    Map<String, String> currentProperties = new HashMap<>();
    try {
      currentProperties = topics.getSubscriptionProperties(topicName, subscriptionName);
    } catch (PulsarAdminException.NotFoundException notFoundException) {
    }
    currentProperties.put("jms.filtering", enableFilters + "");
    if (enableFilters) {
      currentProperties.put("jms.selector", selector);
    } else {
      currentProperties.remove("jms.selector");
    }
    topics.updateSubscriptionProperties(topicName, subscriptionName, currentProperties);
  }

  @Override
  public void setSubscriptionSelector(
      Topic destination, String subscriptionName, boolean enableFilters, String selector)
      throws JMSException {
    try {
      PulsarDestination dest = PulsarConnectionFactory.toPulsarDestination(destination);
      String topicName = factory.getPulsarTopicName(dest);
      doUpdateSubscriptionSelector(enableFilters, selector, topicName, subscriptionName);
    } catch (PulsarAdminException error) {
      throw Utils.handleException(error);
    }
  }
}
