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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.TopicStats;

public class RealPulsarAdminWrapperFactory {

  public static PulsarAdminWrapper createPulsarAdmin(
      String webServiceUrl,
      boolean tlsAllowInsecureConnection,
      boolean tlsEnableHostnameVerification,
      String tlsTrustCertsFilePath,
      boolean useKeyStoreTls,
      String tlsTrustStoreType,
      String tlsTrustStorePath,
      String tlsTrustStorePassword,
      Authentication authentication)
      throws PulsarClientException {

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(PulsarAdmin.class.getClassLoader());
      return new RealPulsarAdminWrapper(
          webServiceUrl,
          tlsAllowInsecureConnection,
          tlsEnableHostnameVerification,
          tlsTrustCertsFilePath,
          useKeyStoreTls,
          tlsTrustStoreType,
          tlsTrustStorePath,
          tlsTrustStorePassword,
          authentication);
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
  }

  private static class RealPulsarAdminWrapper implements PulsarAdminWrapper {
    private final PulsarAdmin pulsarAdmin;

    private RealPulsarAdminWrapper(
        String webServiceUrl,
        boolean tlsAllowInsecureConnection,
        boolean tlsEnableHostnameVerification,
        String tlsTrustCertsFilePath,
        boolean useKeyStoreTls,
        String tlsTrustStoreType,
        String tlsTrustStorePath,
        String tlsTrustStorePassword,
        Authentication authentication)
        throws PulsarClientException {
      this.pulsarAdmin =
          PulsarAdmin.builder()
              .serviceHttpUrl(webServiceUrl)
              .allowTlsInsecureConnection(tlsAllowInsecureConnection)
              .enableTlsHostnameVerification(tlsEnableHostnameVerification)
              .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
              .useKeyStoreTls(useKeyStoreTls)
              .tlsTrustStoreType(tlsTrustStoreType)
              .tlsTrustStorePath(tlsTrustStorePath)
              .tlsTrustStorePassword(tlsTrustStorePassword)
              .authentication(authentication)
              .build();
    }

    @Override
    public void close() {
      pulsarAdmin.close();
    }

    @Override
    public void createSubscription(
        String fullQualifiedTopicName,
        String subscriptionName,
        org.apache.pulsar.client.api.MessageId earliest)
        throws PulsarAdminException {
      pulsarAdmin.topics().createSubscription(fullQualifiedTopicName, subscriptionName, earliest);
    }

    @Override
    public java.util.Map<String, String> getSubscriptionProperties(
        String fullQualifiedTopicName, String subscriptionName) throws PulsarAdminException {
      return pulsarAdmin
          .topics()
          .getSubscriptionProperties(fullQualifiedTopicName, subscriptionName);
    }

    @Override
    public org.apache.pulsar.common.partition.PartitionedTopicMetadata getPartitionedTopicMetadata(
        String fullQualifiedTopicName) throws PulsarAdminException {
      return pulsarAdmin.topics().getPartitionedTopicMetadata(fullQualifiedTopicName);
    }

    @Override
    public java.util.List<org.apache.pulsar.client.api.Message<byte[]>> peekMessages(
        String fullQualifiedTopicName, String queueSubscriptionName, int i)
        throws PulsarAdminException {
      return pulsarAdmin.topics().peekMessages(fullQualifiedTopicName, queueSubscriptionName, i);
    }

    @Override
    public void deleteSubscription(String fullQualifiedTopicName, String name, boolean b)
        throws PulsarAdminException {
      pulsarAdmin.topics().deleteSubscription(fullQualifiedTopicName, name, b);
    }

    @Override
    public java.util.List<String> getTopicList(String systemNamespace) throws PulsarAdminException {
      return pulsarAdmin.topics().getList(systemNamespace);
    }

    @Override
    public java.util.List<String> getSubscriptions(String topic) throws PulsarAdminException {
      return pulsarAdmin.topics().getSubscriptions(topic);
    }

    @Override
    public void createNonPartitionedTopic(String name) throws PulsarAdminException {
      pulsarAdmin.topics().createNonPartitionedTopic(name);
    }

    @Override
    public List<String> getPartitionedTopicList(String systemNamespace)
        throws PulsarAdminException {
      return pulsarAdmin.topics().getPartitionedTopicList(systemNamespace);
    }

    @Override
    public TopicStats getStats(String fullQualifiedTopicName) throws PulsarAdminException {
      return pulsarAdmin.topics().getStats(fullQualifiedTopicName);
    }

    @Override
    public void deletePartitionedTopic(
        String fullQualifiedTopicName, boolean forceDeleteTemporaryDestinations)
        throws PulsarAdminException {
      pulsarAdmin
          .topics()
          .deletePartitionedTopic(fullQualifiedTopicName, forceDeleteTemporaryDestinations);
    }

    @Override
    public void deleteTopic(String fullQualifiedTopicName, boolean forceDeleteTemporaryDestinations)
        throws PulsarAdminException {
      pulsarAdmin.topics().delete(fullQualifiedTopicName, forceDeleteTemporaryDestinations);
    }

    @Override
    public void createPartitionedTopic(String topicName, int i) throws PulsarAdminException {
      pulsarAdmin.topics().createPartitionedTopic(topicName, i);
    }

    @Override
    public PartitionedTopicStats getPartitionedTopicStats(String topicName, boolean b)
        throws PulsarAdminException {
      return pulsarAdmin.topics().getPartitionedStats(topicName, b);
    }

    public <T> T getPulsarAdmin() {
      return (T) pulsarAdmin;
    }
  }
}
