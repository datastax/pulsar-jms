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
package com.datastax.oss.pulsar.jms.cli;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import com.datastax.oss.pulsar.jms.PulsarDestination;
import com.datastax.oss.pulsar.jms.TopicDiscoveryUtils;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;

@Slf4j
class DescribeCommand extends TopicBaseCommand {

  public DescribeCommand() {
    super(null);
  }

  @Override
  public String name() {
    return "describe";
  }

  @Override
  public String description() {
    return "Describe a JMS Destination";
  }

  @Override
  protected void executeInternal() throws Exception {

    PulsarDestination destination = getDestination(false, false);

    if (destination.isMultiTopic()) {
      List<PulsarDestination> destinations = destination.getDestinations();
      println(
          "JMS Destination {} is a virtual multi-topic destination, it maps to {} destinations",
          destination,
          destinations.size());
      if (destinations.isEmpty()) {
        return;
      }
      println("Destinations:");
      for (PulsarDestination sub : destinations) {
        println("     {}", sub);
      }
      println("Details:");
      for (PulsarDestination sub : destinations) {
        describeDestination(sub);
      }
    } else if (destination.isRegExp()) {
      PulsarConnectionFactory factory = getFactory(true);
      // trigger internal creation of the PulsarClient
      factory.createConnection().close();
      PulsarClient pulsarClient = factory.getPulsarClient();
      String topicName = factory.getPulsarTopicName(destination);
      println(
          "JMS Destination {} is a virtual regexp destination, the pattern is {}",
          destination,
          topicName);
      List<String> topics =
          TopicDiscoveryUtils.discoverTopicsByPattern(topicName, pulsarClient, 10000);
      if (topics.isEmpty()) {
        return;
      }
      println("Destinations:");
      for (String sub : topics) {
        println("     {}", sub);
      }
      println("Details:");
      for (String topic : topics) {
        PulsarDestination sub = destination.createSameType(topic);
        describeDestination(sub);
      }
    } else {
      describeDestination(destination);
    }
  }

  private void describeDestination(PulsarDestination destination) throws Exception {
    PulsarConnectionFactory factory = getFactory();
    PulsarAdmin pulsarAdmin = factory.getPulsarAdmin();
    String topicName = factory.getPulsarTopicName(destination);
    String subscription = null;
    if (destination.isQueue()) {
      subscription = factory.getQueueSubscriptionName(destination);
      println(
          "JMS Destination {} maps to the subscription {} on Pulsar Topic {}",
          destination,
          subscription,
          topicName);
    } else {
      println("JMS Destination {} maps to Pulsar Topic {}", destination, topicName);
    }
    final Map<String, ? extends SubscriptionStats> subscriptions;
    PartitionedTopicMetadata partitionedTopicMetadata =
        pulsarAdmin.topics().getPartitionedTopicMetadata(topicName);
    if (partitionedTopicMetadata.partitions > 0) {
      println("The Topic {} has {} partitions", topicName, partitionedTopicMetadata.partitions);
      try {
        PartitionedTopicStats partitionedStats =
            pulsarAdmin.topics().getPartitionedStats(topicName, false);
        subscriptions = partitionedStats.getSubscriptions();
      } catch (PulsarAdminException.NotFoundException notFound) {
        // not partitioned?
        println("The Topic {} does not exist", topicName);
        return;
      }
    } else {
      println("The Topic {} is not partitioned", topicName);
      try {
        TopicStats stats = pulsarAdmin.topics().getStats(topicName);
        subscriptions = stats.getSubscriptions();
      } catch (PulsarAdminException.NotFoundException notFound) {
        println("The Topic {} does not exist", topicName);
        return;
      }
    }

    if (subscriptions.isEmpty()) {
      println("Currently there are no subscriptions on this Pulsar topic");
      return;
    }
    println("Subscriptions on the Pulsar topic:");
    subscriptions.forEach(
        (name, sub) -> {
          println("Subscription: {}", name);
          Map<String, String> subscriptionProperties = sub.getSubscriptionProperties();
          if (subscriptionProperties != null) {
            subscriptionProperties.forEach(
                (k, v) -> {
                  println("  Property {}:{}", k, v);
                });
            String jmsFiltering = subscriptionProperties.getOrDefault("jms.filtering", "false");
            if ("true".equals(jmsFiltering)) {
              println("  JMS Server Side filters are enabled with a per-subscription filter");
              String jmsSelector = subscriptionProperties.getOrDefault("jms.selector", "");
              println("  Selector is: {}", jmsSelector);
            } else {
              println("  JMS Server Side filters: the per-subscription filter is not enabled here");
            }
            sub.getConsumers()
                .forEach(
                    c -> {
                      println("  Consumer {}", c.getConsumerName());
                      Map<String, String> metadata = c.getMetadata();
                      if (metadata != null) {
                        metadata.forEach(
                            (k, v) -> {
                              println("    Property {}:{}", k, v);
                            });
                        String jmsConsumerFiltering =
                            metadata.getOrDefault("jms.filtering", "false");
                        if ("true".equals(jmsConsumerFiltering)) {
                          println("    Consumer has jms.serverSideFiltering option enabled");
                        } else {
                          println("    Consumer is NOT using jms.serverSideFiltering feature");
                        }
                      }
                    });
          }
        });
  }
}
