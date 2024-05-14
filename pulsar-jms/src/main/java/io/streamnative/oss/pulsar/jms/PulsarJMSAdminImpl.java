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
package io.streamnative.oss.pulsar.jms;

import io.streamnative.oss.pulsar.jms.api.JMSAdmin;
import io.streamnative.oss.pulsar.jms.api.JMSDestinationMetadata;
import io.streamnative.oss.pulsar.jms.selectors.SelectorSupport;
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
import org.apache.pulsar.common.policies.data.ConsumerStats;
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

      List<JMSDestinationMetadata> subDestinationsMetadata = new ArrayList<>();
      List<PulsarDestination> destinations = destination.getDestinations();
      for (PulsarDestination sub : destinations) {
        JMSDestinationMetadata subDestinationMetadata = describe(sub);
        subDestinationsMetadata.add(subDestinationMetadata);
      }
      return new JMSDestinationMetadata.VirtualDestinationMetadata(
          destination.getName(),
          destination.isQueue(),
          destination.isMultiTopic(),
          destination.isRegExp(),
          subDestinationsMetadata);
    } else if (destination.isRegExp()) {

      List<JMSDestinationMetadata> subDestinationsMetadata = new ArrayList<>();
      PulsarClient pulsarClient = factory.ensureClient();
      String topicName = factory.getPulsarTopicName(destination);
      List<String> topics =
          TopicDiscoveryUtils.discoverTopicsByPattern(topicName, pulsarClient, 10000);
      String customSubscription = destination.extractSubscriptionName();
      for (String topic : topics) {
        if (customSubscription != null) {
          topic = topic + ":" + customSubscription;
        }
        PulsarDestination sub = destination.createSameType(topic);
        JMSDestinationMetadata subDestinationMetadata = describe(sub);
        subDestinationsMetadata.add(subDestinationMetadata);
      }
      return new JMSDestinationMetadata.VirtualDestinationMetadata(
          destination.getName(),
          destination.isQueue(),
          destination.isMultiTopic(),
          destination.isRegExp(),
          subDestinationsMetadata);
    } else {
      return describeDestination(destination);
    }
  }

  private JMSDestinationMetadata describeDestination(PulsarDestination destination)
      throws JMSException {
    PulsarAdmin pulsarAdmin = factory.ensurePulsarAdmin();
    String pulsarTopic = factory.getPulsarTopicName(destination);
    String queueSubscription;
    if (destination.isQueue()) {
      queueSubscription = factory.getQueueSubscriptionName(destination);
    } else {
      queueSubscription = null;
    }
    boolean exists = false;
    Map<String, ? extends SubscriptionStats> subscriptions = Collections.emptyMap();
    List<? extends PublisherStats> publishers = Collections.emptyList();
    PartitionedTopicMetadata partitionedTopicMetadata;
    try {
      partitionedTopicMetadata = pulsarAdmin.topics().getPartitionedTopicMetadata(pulsarTopic);
      exists = true;
    } catch (PulsarAdminException.NotFoundException notFound) {
      partitionedTopicMetadata = new PartitionedTopicMetadata(0);
    } catch (PulsarAdminException err) {
      throw Utils.handleException(err);
    }
    int partitions = partitionedTopicMetadata.partitions;
    final Map<String, ? extends TopicStats> partitionsStats;

    if (exists) {
      try {
        if (partitionedTopicMetadata.partitions > 0) {
          PartitionedTopicStats partitionedStats =
              pulsarAdmin.topics().getPartitionedStats(pulsarTopic, true);
          subscriptions = partitionedStats.getSubscriptions();
          publishers = partitionedStats.getPublishers();
          partitionsStats = partitionedStats.getPartitions();
        } else {
          TopicStats stats = pulsarAdmin.topics().getStats(pulsarTopic);
          subscriptions = stats.getSubscriptions();
          publishers = stats.getPublishers();
          partitionsStats = Collections.emptyMap();
        }
        if (subscriptions == null) {
          subscriptions = Collections.emptyMap();
        }
        if (publishers == null) {
          publishers = Collections.emptyList();
        }
      } catch (PulsarAdminException err) {
        throw Utils.handleException(err);
      }
    } else {
      partitionsStats = Collections.emptyMap();
    }

    List<JMSDestinationMetadata.SubscriptionMetadata> subscriptionMetadataList = new ArrayList<>();

    boolean queueSubscriptionExists = false;
    if (destination.isQueue()) {
      queueSubscriptionExists = subscriptions.containsKey(queueSubscription);
    }
    subscriptions.forEach(
        (subscriptionName, sub) -> {
          if (destination.isQueue() && !subscriptionName.equals(queueSubscription)) {
            // this is a JMS Queue, skip other subscriptions
            return;
          }

          JMSDestinationMetadata.SubscriptionMetadata md =
              new JMSDestinationMetadata.SubscriptionMetadata(subscriptionName);
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
          if (partitions > 0) {
            partitionsStats.forEach(
                (partitionName, topicStats) -> {
                  SubscriptionStats subscriptionStats =
                      topicStats.getSubscriptions().get(subscriptionName);
                  if (subscriptionStats != null) {
                    subscriptionStats
                        .getConsumers()
                        .forEach(
                            c -> {
                              JMSDestinationMetadata.ConsumerMetadata cmd =
                                  buildConsumerMetadata(partitionName, subscriptionName, c);
                              consumers.add(cmd);
                            });
                  }
                });
          } else {
            sub.getConsumers()
                .forEach(
                    c -> {
                      JMSDestinationMetadata.ConsumerMetadata cmd =
                          buildConsumerMetadata(pulsarTopic, subscriptionName, c);
                      consumers.add(cmd);
                    });
          }
        });

    List<JMSDestinationMetadata.ProducerMetadata> producerMetadataList = new ArrayList<>();
    if (partitions > 0) {
      // for partitioned topic we list all the producers connected to each partition
      partitionsStats.forEach(
          (partition, stats) -> {
            stats
                .getPublishers()
                .forEach(
                    p -> {
                      JMSDestinationMetadata.ProducerMetadata cmd =
                          buildProducerMetadata(partition, p);
                      producerMetadataList.add(cmd);
                    });
          });
    } else {
      publishers.forEach(
          p -> {
            JMSDestinationMetadata.ProducerMetadata cmd = buildProducerMetadata(pulsarTopic, p);
            producerMetadataList.add(cmd);
          });
    }
    if (destination.isQueue()) {
      JMSDestinationMetadata.SubscriptionMetadata subscriptionMetadata =
          subscriptionMetadataList.isEmpty() ? null : subscriptionMetadataList.get(0);
      return new JMSDestinationMetadata.QueueMetadata(
          destination.getName(),
          exists,
          pulsarTopic,
          producerMetadataList,
          partitions,
          queueSubscription,
          queueSubscriptionExists,
          subscriptionMetadata);
    } else {
      return new JMSDestinationMetadata.TopicMetadata(
          destination.getName(),
          exists,
          pulsarTopic,
          producerMetadataList,
          partitions,
          subscriptionMetadataList);
    }
  }

  private static JMSDestinationMetadata.ConsumerMetadata buildConsumerMetadata(
      String pulsarTopic, String subscriptionName, ConsumerStats c) {
    JMSDestinationMetadata.ConsumerMetadata cmd =
        new JMSDestinationMetadata.ConsumerMetadata(c.getConsumerName());
    cmd.setSubscriptionName(subscriptionName);
    cmd.setPulsarTopic(pulsarTopic);
    cmd.setAddress(c.getAddress());
    cmd.setClientVersion(c.getClientVersion());
    Map<String, String> metadata =
        c.getMetadata() != null ? c.getMetadata() : Collections.emptyMap();
    cmd.setMetadata(metadata);
    cmd.setAcknowledgeMode(metadata.getOrDefault("jms.acknowledgeMode", "?"));
    String jmsConsumerFiltering = metadata.getOrDefault("jms.filtering", "false");
    if ("true".equals(jmsConsumerFiltering)) {
      cmd.setEnableFilters(true);
      String jmsSelector = metadata.getOrDefault("jms.selector", "");
      cmd.setSelector(jmsSelector);
    }
    cmd.setEnablePriority(metadata.getOrDefault("jms.priority", "disabled").equals("enabled"));
    return cmd;
  }

  private static JMSDestinationMetadata.ProducerMetadata buildProducerMetadata(
      String pulsarTopic, PublisherStats p) {
    JMSDestinationMetadata.ProducerMetadata cmd =
        new JMSDestinationMetadata.ProducerMetadata(p.getProducerName());
    cmd.setPulsarTopic(pulsarTopic);
    cmd.setAddress(p.getAddress());
    cmd.setClientVersion(p.getClientVersion());
    Map<String, String> metadata =
        p.getMetadata() != null ? p.getMetadata() : Collections.emptyMap();
    cmd.setMetadata(metadata);
    cmd.setEnablePriority(metadata.getOrDefault("jms.priority", "disabled").equals("enabled"));
    cmd.setTransacted(metadata.getOrDefault("jms.transactions", "disabled").equals("enabled"));
    cmd.setPriorityMapping(metadata.getOrDefault("jms.priorityMapping", ""));
    return cmd;
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
      Topics topics = factory.ensurePulsarAdmin().topics();
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
      Topics topics = factory.ensurePulsarAdmin().topics();
      boolean exists = false;
      try {
        PartitionedTopicMetadata partitionedTopicMetadata =
            topics.getPartitionedTopicMetadata(topicName);
        checkDestination(
            destination,
            d -> partitionedTopicMetadata.partitions == partitions,
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
      Topics topics = factory.ensurePulsarAdmin().topics();
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
      try {
        if (partitions > 0) {
          topics.createPartitionedTopic(topicName, partitions);
        } else {
          topics.createNonPartitionedTopic(topicName);
        }
      } catch (PulsarAdminException.ConflictException exists) {
        throw new InvalidDestinationException("Topic " + topicName + " already exists");
      }
    } catch (PulsarAdminException error) {
      throw Utils.handleException(error);
    }
  }

  @Override
  public void setQueueSubscriptionSelector(
      Queue destination, boolean enableFilters, String selector) throws JMSException {
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
    Topics topics = factory.ensurePulsarAdmin().topics();
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
