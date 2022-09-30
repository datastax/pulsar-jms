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

import static com.datastax.oss.pulsar.jms.Utils.getAndRemoveString;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff;

final class ConsumerConfiguration {

  static ConsumerConfiguration DEFAULT =
      new ConsumerConfiguration(Collections.emptyMap(), null, null, null, null, null);

  private final Map<String, Object> consumerConfiguration;
  private Schema<?> consumerSchema;
  private Boolean emulateJMSPriority;
  private DeadLetterPolicy deadLetterPolicy;
  private RedeliveryBackoff negativeAckRedeliveryBackoff;
  private RedeliveryBackoff ackTimeoutRedeliveryBackoff;

  ConsumerConfiguration(
      Map<String, Object> consumerConfiguration,
      Schema<?> consumerSchema,
      Boolean emulateJMSPriority,
      DeadLetterPolicy deadLetterPolicy,
      RedeliveryBackoff negativeAckRedeliveryBackoff,
      RedeliveryBackoff ackTimeoutRedeliveryBackoff) {
    this.consumerConfiguration = Objects.requireNonNull(consumerConfiguration);
    this.consumerSchema = consumerSchema;
    this.emulateJMSPriority = emulateJMSPriority;
    this.deadLetterPolicy = deadLetterPolicy;
    this.negativeAckRedeliveryBackoff = negativeAckRedeliveryBackoff;
    this.ackTimeoutRedeliveryBackoff = ackTimeoutRedeliveryBackoff;
  }

  public Map<String, Object> getConsumerConfiguration() {
    return consumerConfiguration;
  }

  public Schema<?> getConsumerSchema() {
    return consumerSchema;
  }

  public DeadLetterPolicy getDeadLetterPolicy() {
    return deadLetterPolicy;
  }

  public RedeliveryBackoff getNegativeAckRedeliveryBackoff() {
    return negativeAckRedeliveryBackoff;
  }

  public RedeliveryBackoff getAckTimeoutRedeliveryBackoff() {
    return ackTimeoutRedeliveryBackoff;
  }

  public Boolean isEmulateJMSPriority() {
    return emulateJMSPriority;
  }

  ConsumerConfiguration applyDefaults(ConsumerConfiguration defaultConsumerConfiguration) {
    Map<String, Object> mergedConsumerConfiguration = new HashMap<>();
    if (defaultConsumerConfiguration.consumerConfiguration != null) {
      mergedConsumerConfiguration.putAll(
          Utils.deepCopyMap(defaultConsumerConfiguration.consumerConfiguration));
    }
    if (consumerConfiguration != null) {
      mergedConsumerConfiguration.putAll(Utils.deepCopyMap(consumerConfiguration));
    }
    Schema<?> mergedConsumerSchema =
        consumerSchema != null ? consumerSchema : defaultConsumerConfiguration.consumerSchema;
    Boolean mergedEmulateJMSPriority =
        emulateJMSPriority != null
            ? emulateJMSPriority
            : defaultConsumerConfiguration.emulateJMSPriority;
    DeadLetterPolicy mergedDeadLetterPolicy =
        deadLetterPolicy != null ? deadLetterPolicy : defaultConsumerConfiguration.deadLetterPolicy;
    RedeliveryBackoff mergedNegativeAckRedeliveryBackoff =
        negativeAckRedeliveryBackoff != null
            ? negativeAckRedeliveryBackoff
            : defaultConsumerConfiguration.negativeAckRedeliveryBackoff;
    RedeliveryBackoff mergedAckTimeoutRedeliveryBackoff =
        ackTimeoutRedeliveryBackoff != null
            ? ackTimeoutRedeliveryBackoff
            : defaultConsumerConfiguration.ackTimeoutRedeliveryBackoff;

    return new ConsumerConfiguration(
        mergedConsumerConfiguration,
        mergedConsumerSchema,
        mergedEmulateJMSPriority,
        mergedDeadLetterPolicy,
        mergedNegativeAckRedeliveryBackoff,
        mergedAckTimeoutRedeliveryBackoff);
  }

  static ConsumerConfiguration buildConsumerConfiguration(
      Map<String, Object> consumerConfigurationM) {
    if (consumerConfigurationM == null || consumerConfigurationM.isEmpty()) {
      return DEFAULT;
    }
    consumerConfigurationM = Utils.deepCopyMap(consumerConfigurationM);

    boolean emulateJMSPriority = false;
    Schema<?> consumerSchema = null;
    Map<String, Object> consumerConfiguration = Collections.emptyMap();
    DeadLetterPolicy deadLetterPolicy = null;
    RedeliveryBackoff negativeAckRedeliveryBackoff = null;
    RedeliveryBackoff ackTimeoutRedeliveryBackoff = null;

    if (consumerConfigurationM != null) {
      consumerConfiguration = new HashMap(consumerConfigurationM);

      // remove values that cannot be accepted by loadConf()
      if (consumerConfiguration.containsKey("useSchema")) {
        boolean useSchema =
            Boolean.parseBoolean(getAndRemoveString("useSchema", "false", consumerConfiguration));
        if (useSchema) {
          consumerSchema = Schema.AUTO_CONSUME();
        } else {
          consumerSchema = Schema.BYTES;
        }
      }

      if (consumerConfiguration.containsKey("emulateJMSPriority")) {
        emulateJMSPriority =
            Boolean.parseBoolean(
                getAndRemoveString("emulateJMSPriority", "false", consumerConfiguration));
      }

      deadLetterPolicy = getAndRemoveDeadLetterPolicy(consumerConfiguration);
      negativeAckRedeliveryBackoff =
          getAndRemoveRedeliveryBackoff("negativeAckRedeliveryBackoff", consumerConfiguration);
      ackTimeoutRedeliveryBackoff =
          getAndRemoveRedeliveryBackoff("ackTimeoutRedeliveryBackoff", consumerConfiguration);
    }
    return new ConsumerConfiguration(
        consumerConfiguration,
        consumerSchema,
        emulateJMSPriority,
        deadLetterPolicy,
        negativeAckRedeliveryBackoff,
        ackTimeoutRedeliveryBackoff);
  }

  private static RedeliveryBackoff getAndRemoveRedeliveryBackoff(
      String baseName, Map<String, Object> consumerConfiguration) {
    Map<String, Object> config = (Map<String, Object>) consumerConfiguration.remove(baseName);
    if (config == null) {
      return null;
    }
    MultiplierRedeliveryBackoff.MultiplierRedeliveryBackoffBuilder builder =
        MultiplierRedeliveryBackoff.builder();
    long maxDelayMs = Long.parseLong(getAndRemoveString("maxDelayMs", "-1", config));
    if (maxDelayMs >= 0) {
      builder.maxDelayMs(maxDelayMs);
    }

    long minDelayMs = Long.parseLong(getAndRemoveString("minDelayMs", "-1", config));
    if (minDelayMs >= 0) {
      builder.minDelayMs(minDelayMs);
    }
    double multiplier = Double.parseDouble(getAndRemoveString("multiplier", "-1", config));
    if (multiplier >= 0) {
      builder.multiplier(multiplier);
    }
    if (!config.isEmpty()) {
      throw new IllegalArgumentException("Unhandled fields in " + baseName + ": " + config);
    }
    return builder.build();
  }

  private static DeadLetterPolicy getAndRemoveDeadLetterPolicy(
      Map<String, Object> consumerConfiguration) {
    Map<String, Object> deadLetterPolicyConfig =
        (Map<String, Object>) consumerConfiguration.remove("deadLetterPolicy");
    if (deadLetterPolicyConfig == null || deadLetterPolicyConfig.isEmpty()) {
      return null;
    }

    DeadLetterPolicy.DeadLetterPolicyBuilder deadLetterPolicyBuilder = DeadLetterPolicy.builder();
    String deadLetterTopic = getAndRemoveString("deadLetterTopic", "", deadLetterPolicyConfig);
    if (!deadLetterTopic.isEmpty()) {
      deadLetterPolicyBuilder.deadLetterTopic(deadLetterTopic);
    }
    String retryLetterTopic = getAndRemoveString("retryLetterTopic", "", deadLetterPolicyConfig);
    if (!deadLetterTopic.isEmpty()) {
      deadLetterPolicyBuilder.retryLetterTopic(retryLetterTopic);
    }
    String initialSubscriptionName =
        getAndRemoveString("initialSubscriptionName", "", deadLetterPolicyConfig);
    if (!initialSubscriptionName.isEmpty()) {
      deadLetterPolicyBuilder.initialSubscriptionName(initialSubscriptionName);
    }
    int maxRedeliverCount =
        Integer.parseInt(getAndRemoveString("maxRedeliverCount", "-1", deadLetterPolicyConfig));
    if (maxRedeliverCount > -1) {
      deadLetterPolicyBuilder.maxRedeliverCount(maxRedeliverCount);
    }
    if (!deadLetterPolicyConfig.isEmpty()) {
      throw new IllegalArgumentException(
          "Unhandled fields in deadLetterPolicy: " + deadLetterPolicyConfig);
    }

    return deadLetterPolicyBuilder.build();
  }
}
