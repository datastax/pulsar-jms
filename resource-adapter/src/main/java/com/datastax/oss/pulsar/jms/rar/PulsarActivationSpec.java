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
package com.datastax.oss.pulsar.jms.rar;

import com.datastax.oss.pulsar.jms.Utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.InvalidPropertyException;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterAssociation;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarActivationSpec implements ActivationSpec, ResourceAdapterAssociation {

  private ResourceAdapter resourceAdapter;
  private String destination;
  private String destinationType = "queue";
  /**
   * Configuration for the underlying PulsarConnectionFactory. Factories are cached, using this
   * configuration as key.
   */
  private String configuration = "{}";
  /**
   * Override the consumer configuration. This allows you to have different consumerConfig but still
   * share the PulsarConnectionFactory
   */
  private String consumerConfig = "{}";

  private String subscriptionType = "Durable";
  private String subscriptionMode = "Shared";
  private String subscriptionName = "";

  private int numSessions = 1;

  public String getConfiguration() {
    return configuration;
  }

  public void setConfiguration(String configuration) {
    log.info("setConfiguration {}", configuration);
    this.configuration = configuration;
  }

  public String getDestination() {
    return destination;
  }

  public void setDestination(String destination) {
    this.destination = destination;
  }

  public String getDestinationType() {
    return destinationType;
  }

  public void setDestinationType(String destinationType) {
    this.destinationType = destinationType;
  }

  public String getSubscriptionName() {
    return subscriptionName;
  }

  public void setSubscriptionName(String subscriptionName) {
    this.subscriptionName = subscriptionName;
  }

  public String getSubscriptionType() {
    return subscriptionType;
  }

  public void setSubscriptionType(String destinationType) {
    this.subscriptionType = destinationType;
  }

  public String getSubscriptionMode() {
    return subscriptionMode;
  }

  public void setSubscriptionMode(String subscriptionMode) {
    this.subscriptionMode = subscriptionMode;
  }

  public int getNumSessions() {
    return numSessions;
  }

  public void setNumSessions(int numSessions) {
    this.numSessions = numSessions;
  }

  @Override
  public void validate() throws InvalidPropertyException {
    if (destinationType == null) {
      throw new InvalidPropertyException("invalid null destinationType");
    }
    boolean isTopic;
    switch (destinationType) {
      case "queue":
      case "jakarta.jms.Queue":
      case "Queue":
        isTopic = false;
        break;
      case "topic":
      case "Topic":
      case "jakarta.jms.Topic":
        isTopic = true;
        break;
      default:
        throw new InvalidPropertyException(
            "Invalid destinationType '"
                + destinationType
                + "', only 'queue','topic','jakarta.jms.Queue','jakarta.jms.Topic'");
    }
    if (destination == null || destination.isEmpty()) {
      throw new InvalidPropertyException(
          "Invalid '" + destination + "' destination, it must be non empty");
    }
    switch (subscriptionType + "") {
      case "Durable":
      case "NonDurable":
        break;
      default:
        throw new InvalidPropertyException(
            "Invalid '"
                + subscriptionType
                + "' subscriptionType, it must be Durable or NonDurable");
    }
    switch (subscriptionMode + "") {
      case "Exclusive":
      case "Shared":
        break;
      default:
        throw new InvalidPropertyException(
            "Invalid '" + subscriptionMode + "' subscriptionMode, it must be Exclusive or Shared");
    }
    boolean requireSubscriptionName =
        isTopic && !(subscriptionMode.equals("Exclusive") && subscriptionType.equals("NonDurable"));

    if (requireSubscriptionName && (subscriptionName == null || subscriptionName.isEmpty())) {
      throw new InvalidPropertyException(
          "Invalid '"
              + subscriptionName
              + "' subscriptionName, it must be non empty with subscriptionMode "
              + subscriptionMode
              + " and subscriptionType "
              + subscriptionType);
    }
  }

  @Override
  public ResourceAdapter getResourceAdapter() {
    return resourceAdapter;
  }

  @Override
  public void setResourceAdapter(ResourceAdapter resourceAdapter) throws ResourceException {
    this.resourceAdapter = resourceAdapter;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PulsarActivationSpec that = (PulsarActivationSpec) o;
    return Objects.equals(destination, that.destination)
        && Objects.equals(destinationType, that.destinationType)
        && Objects.equals(configuration, that.configuration)
        && Objects.equals(subscriptionType, that.subscriptionType)
        && Objects.equals(subscriptionMode, that.subscriptionMode)
        && Objects.equals(subscriptionName, that.subscriptionName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        destination,
        destinationType,
        configuration,
        subscriptionType,
        subscriptionMode,
        subscriptionName);
  }

  @Override
  public String toString() {
    return "PulsarActivationSpec{"
        + "destination='"
        + destination
        + '\''
        + ", destinationType='"
        + destinationType
        + '\''
        + ", configuration='"
        + configuration
        + '\''
        + ", subscriptionType='"
        + subscriptionType
        + '\''
        + ", subscriptionMode='"
        + subscriptionMode
        + '\''
        + ", subscriptionName='"
        + subscriptionName
        + '\''
        + '}';
  }

  public String getMergedConfiguration(String configuration) {
    if (this.configuration == null
        || this.configuration.trim().isEmpty()
        || this.configuration.replace(" ", "").equals("{}")) { // empty "{}"
      return configuration;
    } else {
      return this.configuration;
    }
  }

  public String getConsumerConfig() {
    return consumerConfig;
  }

  public void setConsumerConfig(String consumerConfig) {
    log.info("setConsumerConfig {}", consumerConfig);
    this.consumerConfig = consumerConfig;
  }

  Map<String, Object> buildConsumerConfiguration() {
    String overrideConsumerConfiguration = consumerConfig;
    if (overrideConsumerConfiguration != null) {
      overrideConsumerConfiguration = overrideConsumerConfiguration.trim();
    } else {
      return Collections.emptyMap();
    }
    Map<String, Object> result = null;
    if (!overrideConsumerConfiguration.isEmpty() && !overrideConsumerConfiguration.equals("{}")) {
      String jsonConfig = overrideConsumerConfiguration;
      result =
          (Map<String, Object>)
              Utils.runtimeException(() -> new ObjectMapper().readValue(jsonConfig, Map.class));
    }
    if (result == null) {
      return Collections.emptyMap();
    } else {
      return result;
    }
  }
}
