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
import java.util.HashMap;
import java.util.Map;
import javax.jms.Destination;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;

@Slf4j
public class UpdateSubscriptionCommand extends SubscriptionBaseCommand {

  @Override
  public String name() {
    return "update-subscription";
  }

  @Override
  public String description() {
    return "Update a JMS Subscription";
  }

  public void executeInternal() throws Exception {
    validateSelector();
    String subscription = getSubscription();
    String selector = getSelector();
    Destination destination = getDestination(false);
    PulsarConnectionFactory factory = getFactory();
    String topicName = factory.getPulsarTopicName(destination);
    println("JMS Destination {} maps to Pulsar Topic {}", destination, topicName);
    PulsarAdmin pulsarAdmin = factory.getPulsarAdmin();
    Map<String, String> currentProperties = null;
    try {
      TopicStats stats = pulsarAdmin.topics().getStats(topicName);
      Map<String, ? extends SubscriptionStats> subscriptions = stats.getSubscriptions();
      if (!subscriptions.containsKey(subscription)) {
        throw new IllegalArgumentException(
            "Pulsar topic " + topicName + " does not have a subscription named " + subscription);
      }
      currentProperties = subscriptions.get(subscription).getSubscriptionProperties();
      if (currentProperties == null) {
        currentProperties = new HashMap<>();
      }
    } catch (PulsarAdminException.NotFoundException ok) {
      throw new IllegalArgumentException("Topic " + topicName + " does not exist");
    }

    Map<String, String> subscriptionProperties = new HashMap<>(currentProperties);
    if (selector != null && !selector.isEmpty() && isEnableFiltering()) {
      subscriptionProperties.put("jms.selector", selector);
      subscriptionProperties.put("jms.filtering", "true");
      println(
          "Activating filtering with selector {}. Subscription properties {}",
          selector,
          subscriptionProperties);
    } else {
      subscriptionProperties.put("jms.selector", "");
      subscriptionProperties.put("jms.filtering", "false");
      println("Disabling filtering. Subscription properties {}", selector, subscriptionProperties);
    }

    println(
        "Updating subscription {} on {} properties {}",
        subscription,
        topicName,
        subscriptionProperties);

    pulsarAdmin
        .topics()
        .updateSubscriptionProperties(topicName, subscription, subscriptionProperties);
  }
}
