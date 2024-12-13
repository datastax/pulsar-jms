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

import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.TopicStats;

public interface PulsarAdminWrapper {

  void close();

  void createSubscription(
      String fullQualifiedTopicName, String subscriptionName, MessageId earliest)
      throws PulsarAdminException;

  Map<String, String> getSubscriptionProperties(
      String fullQualifiedTopicName, String subscriptionName) throws PulsarAdminException;

  PartitionedTopicMetadata getPartitionedTopicMetadata(String fullQualifiedTopicName)
      throws PulsarAdminException;

  List<Message<byte[]>> peekMessages(
      String fullQualifiedTopicName, String queueSubscriptionName, int i)
      throws PulsarAdminException;

  void deleteSubscription(String fullQualifiedTopicName, String name, boolean b)
      throws PulsarAdminException;

  List<String> getTopicList(String systemNamespace) throws PulsarAdminException;

  List<String> getSubscriptions(String topic) throws PulsarAdminException;

  <T> T getPulsarAdmin();

  void createNonPartitionedTopic(String name) throws PulsarAdminException;

  List<String> getPartitionedTopicList(String systemNamespace) throws PulsarAdminException;

  TopicStats getStats(String fullQualifiedTopicName) throws PulsarAdminException;

  void deletePartitionedTopic(
      String fullQualifiedTopicName, boolean forceDeleteTemporaryDestinations)
      throws PulsarAdminException;

  void deleteTopic(String fullQualifiedTopicName, boolean forceDeleteTemporaryDestinations)
      throws PulsarAdminException;

  void createPartitionedTopic(String topicName, int i) throws PulsarAdminException;

  PartitionedTopicStats getPartitionedTopicStats(String topicName, boolean b)
      throws PulsarAdminException;
}
