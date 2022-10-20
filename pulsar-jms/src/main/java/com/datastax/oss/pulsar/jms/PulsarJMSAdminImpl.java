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
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

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
  public JMSDestinationMetadata describe(Destination destination) throws JMSException {
    return null;
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
      String subscriptionName = factory.getQueueSubscriptionName(dest);
      if (partitions > 0) {
        topics.createPartitionedTopic(topicName, partitions);
      } else {
        topics.createNonPartitionedTopic(topicName);
      }
      Map<String, String> properties = new HashMap<>();
      if (enableFilters) {
        properties.put("jms.filtering", "true");
        properties.put("jms.selector", selector);
      }
      topics.createSubscription(topicName, subscriptionName, MessageId.earliest, false, properties);
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
