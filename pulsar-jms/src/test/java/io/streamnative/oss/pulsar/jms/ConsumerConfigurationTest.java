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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Test;

final class ConsumerConfigurationTest {

  private void test(
      Map<String, Object> consumerConfiguration, Consumer<ConsumerConfiguration> test) {
    test(consumerConfiguration, ConsumerConfiguration.DEFAULT, test);
  }

  private void test(
      Map<String, Object> consumerConfiguration,
      ConsumerConfiguration defaultConfiguration,
      Consumer<ConsumerConfiguration> test) {
    ConsumerConfiguration result =
        ConsumerConfiguration.buildConsumerConfiguration(consumerConfiguration);

    // test that the ConsumerConfiguration matches the provided configuration
    test.accept(result);

    // test that the provided configuration applies also when applied to the default configuration
    test.accept(result.applyDefaults(defaultConfiguration));
  }

  @Test
  void testBuildEmptyConfiguration() {
    Map<String, Object> consumerConfiguration = new HashMap<>();
    test(
        consumerConfiguration,
        (result -> {
          assertTrue(result.getConsumerConfiguration().isEmpty());
          assertNull(result.getAckTimeoutRedeliveryBackoff());
          assertNull(result.getNegativeAckRedeliveryBackoff());
          assertNull(result.getDeadLetterPolicy());
          assertNull(result.getConsumerSchema());
        }));
  }

  @Test
  void testOverrideComplexConfiguration() {

    Map<String, Object> defaultConfiguration = new HashMap<>();
    defaultConfiguration.put("useSchema", true);
    defaultConfiguration.put("ackTimeoutMillis", 1234L);
    Map<String, Object> deadLetterPolicy = new HashMap<>();
    defaultConfiguration.put("deadLetterPolicy", deadLetterPolicy);
    deadLetterPolicy.put("maxRedeliverCount", 5);
    deadLetterPolicy.put("deadLetterTopic", "dql-topic-default");
    deadLetterPolicy.put("initialSubscriptionName", "dqlsub-default");

    Map<String, Object> negativeAckRedeliveryBackoff = new HashMap<>();
    defaultConfiguration.put("negativeAckRedeliveryBackoff", negativeAckRedeliveryBackoff);
    negativeAckRedeliveryBackoff.put("minDelayMs", 10);
    negativeAckRedeliveryBackoff.put("maxDelayMs", 100);
    negativeAckRedeliveryBackoff.put("multiplier", 2.0);

    Map<String, Object> ackTimeoutRedeliveryBackoff = new HashMap<>();
    defaultConfiguration.put("ackTimeoutRedeliveryBackoff", ackTimeoutRedeliveryBackoff);
    ackTimeoutRedeliveryBackoff.put("minDelayMs", 10);
    ackTimeoutRedeliveryBackoff.put("maxDelayMs", 100);
    ackTimeoutRedeliveryBackoff.put("multiplier", 2.0);

    test(
        defaultConfiguration,
        (result) -> {
          assertFalse(result.getConsumerConfiguration().isEmpty());
          assertNotNull(result.getAckTimeoutRedeliveryBackoff());
          assertNotNull(result.getNegativeAckRedeliveryBackoff());
          assertNotNull(result.getDeadLetterPolicy());
          assertNotNull(result.getConsumerSchema());
        });

    ConsumerConfiguration parsedDefault =
        ConsumerConfiguration.buildConsumerConfiguration(defaultConfiguration);
    assertEquals(parsedDefault.getDeadLetterPolicy().getDeadLetterTopic(), "dql-topic-default");
    assertEquals(parsedDefault.getDeadLetterPolicy().getMaxRedeliverCount(), 5);
    assertEquals(
        parsedDefault.getDeadLetterPolicy().getInitialSubscriptionName(), "dqlsub-default");
    assertEquals(parsedDefault.getConsumerSchema().getClass(), Schema.AUTO_CONSUME().getClass());
    assertEquals(20, parsedDefault.getNegativeAckRedeliveryBackoff().next(1));

    {
      // try to override with a empty config
      Map<String, Object> consumerConfiguration = new HashMap<>();

      ConsumerConfiguration parsedEmpty =
          ConsumerConfiguration.buildConsumerConfiguration(consumerConfiguration)
              .applyDefaults(parsedDefault);
      assertFalse(parsedEmpty.getConsumerConfiguration().isEmpty());
      assertNotNull(parsedEmpty.getAckTimeoutRedeliveryBackoff());
      assertNotNull(parsedEmpty.getNegativeAckRedeliveryBackoff());
      assertNotNull(parsedEmpty.getDeadLetterPolicy());
      assertEquals(parsedEmpty.getConsumerSchema().getClass(), Schema.AUTO_CONSUME().getClass());
    }

    {
      // disable schema
      Map<String, Object> consumerConfiguration = new HashMap<>();
      consumerConfiguration.put("useSchema", false);

      ConsumerConfiguration parsedEmpty =
          ConsumerConfiguration.buildConsumerConfiguration(consumerConfiguration)
              .applyDefaults(parsedDefault);
      assertFalse(parsedEmpty.getConsumerConfiguration().isEmpty());
      assertNotNull(parsedEmpty.getAckTimeoutRedeliveryBackoff());
      assertNotNull(parsedEmpty.getNegativeAckRedeliveryBackoff());
      assertNotNull(parsedEmpty.getDeadLetterPolicy());
      // no more schema
      assertEquals(parsedEmpty.getConsumerSchema().getSchemaInfo().getType(), SchemaType.BYTES);

      // test can enable schema again
      Map<String, Object> consumerConfigurationForceEnableSchema = new HashMap<>();
      consumerConfigurationForceEnableSchema.put("useSchema", true);
      ConsumerConfiguration enableSchemaAfterDisabled =
          ConsumerConfiguration.buildConsumerConfiguration(consumerConfigurationForceEnableSchema)
              .applyDefaults(parsedEmpty);
      assertEquals(
          enableSchemaAfterDisabled.getConsumerSchema().getClass(),
          Schema.AUTO_CONSUME().getClass());
    }

    {
      // set a different deadletter policy
      Map<String, Object> consumerConfiguration = new HashMap<>();
      Map<String, Object> newDeadLetterPolicy = new HashMap<>();
      consumerConfiguration.put("deadLetterPolicy", newDeadLetterPolicy);
      newDeadLetterPolicy.put("maxRedeliverCount", 6);
      newDeadLetterPolicy.put("deadLetterTopic", "dql-topic-non-default");
      newDeadLetterPolicy.put("initialSubscriptionName", "dqlsub-non-default");

      ConsumerConfiguration parsedEmpty =
          ConsumerConfiguration.buildConsumerConfiguration(consumerConfiguration)
              .applyDefaults(parsedDefault);
      assertFalse(parsedEmpty.getConsumerConfiguration().isEmpty());
      assertNotNull(parsedEmpty.getAckTimeoutRedeliveryBackoff());
      assertNotNull(parsedEmpty.getNegativeAckRedeliveryBackoff());
      assertEquals(parsedEmpty.getDeadLetterPolicy().getDeadLetterTopic(), "dql-topic-non-default");
      assertEquals(parsedEmpty.getDeadLetterPolicy().getMaxRedeliverCount(), 6);
      assertEquals(
          parsedEmpty.getDeadLetterPolicy().getInitialSubscriptionName(), "dqlsub-non-default");
      assertEquals(parsedEmpty.getConsumerSchema().getClass(), Schema.AUTO_CONSUME().getClass());
    }

    {
      // set a different negativeAckRedeliveryBackoff
      Map<String, Object> consumerConfiguration = new HashMap<>();
      Map<String, Object> newNegativeAckRedeliveryBackoff = new HashMap<>();
      consumerConfiguration.put("negativeAckRedeliveryBackoff", newNegativeAckRedeliveryBackoff);
      newNegativeAckRedeliveryBackoff.put("minDelayMs", 100);
      newNegativeAckRedeliveryBackoff.put("maxDelayMs", 100000);
      newNegativeAckRedeliveryBackoff.put("multiplier", 3.0);

      ConsumerConfiguration parsedEmpty =
          ConsumerConfiguration.buildConsumerConfiguration(consumerConfiguration)
              .applyDefaults(parsedDefault);
      assertFalse(parsedEmpty.getConsumerConfiguration().isEmpty());
      assertNotNull(parsedEmpty.getAckTimeoutRedeliveryBackoff());
      assertNotNull(parsedEmpty.getNegativeAckRedeliveryBackoff());
      assertEquals(300, parsedEmpty.getNegativeAckRedeliveryBackoff().next(1));
      assertEquals(parsedEmpty.getConsumerSchema().getClass(), Schema.AUTO_CONSUME().getClass());
    }

    {
      // set a different negativeAckRedeliveryBackoff
      Map<String, Object> consumerConfiguration = new HashMap<>();
      Map<String, Object> newAckTimeoutRedeliveryBackoff = new HashMap<>();
      consumerConfiguration.put("ackTimeoutRedeliveryBackoff", newAckTimeoutRedeliveryBackoff);
      newAckTimeoutRedeliveryBackoff.put("minDelayMs", 100);
      newAckTimeoutRedeliveryBackoff.put("maxDelayMs", 100000);
      newAckTimeoutRedeliveryBackoff.put("multiplier", 4.0);

      ConsumerConfiguration parsedEmpty =
          ConsumerConfiguration.buildConsumerConfiguration(consumerConfiguration)
              .applyDefaults(parsedDefault);
      assertFalse(parsedEmpty.getConsumerConfiguration().isEmpty());
      assertNotNull(parsedEmpty.getAckTimeoutRedeliveryBackoff());
      assertNotNull(parsedEmpty.getNegativeAckRedeliveryBackoff());
      assertEquals(400, parsedEmpty.getAckTimeoutRedeliveryBackoff().next(1));
      assertEquals(parsedEmpty.getConsumerSchema().getClass(), Schema.AUTO_CONSUME().getClass());
    }
  }

  @Test
  void testSimpleConfiguration() {
    Map<String, Object> consumerConfiguration = new HashMap<>();
    consumerConfiguration.put("ackTimeoutMillis", 100L);
    test(
        consumerConfiguration,
        (result -> {
          assertEquals(100L, result.getConsumerConfiguration().get("ackTimeoutMillis"));
          assertNull(result.getAckTimeoutRedeliveryBackoff());
          assertNull(result.getNegativeAckRedeliveryBackoff());
          assertNull(result.getDeadLetterPolicy());
          assertNull(result.getConsumerSchema());
        }));
  }

  @Test
  void testUseSchema() {
    Map<String, Object> consumerConfiguration = new HashMap<>();
    consumerConfiguration.put("useSchema", true);
    test(
        consumerConfiguration,
        (result -> {
          assertNull(result.getAckTimeoutRedeliveryBackoff());
          assertNull(result.getNegativeAckRedeliveryBackoff());
          assertNull(result.getDeadLetterPolicy());
          assertEquals(result.getConsumerSchema().getClass(), Schema.AUTO_CONSUME().getClass());
        }));
  }

  @Test
  void testDeadletterPolicy() {
    Map<String, Object> consumerConfiguration = new HashMap<>();
    Map<String, Object> deadLetterPolicy = new HashMap<>();
    consumerConfiguration.put("deadLetterPolicy", deadLetterPolicy);
    deadLetterPolicy.put("maxRedeliverCount", 1);
    deadLetterPolicy.put("deadLetterTopic", "dql-topic");
    deadLetterPolicy.put("initialSubscriptionName", "dqlsub");
    test(
        consumerConfiguration,
        (result -> {
          assertNull(result.getAckTimeoutRedeliveryBackoff());
          assertNull(result.getNegativeAckRedeliveryBackoff());
          assertNotNull(result.getDeadLetterPolicy());
          assertEquals(result.getDeadLetterPolicy().getDeadLetterTopic(), "dql-topic");
          assertEquals(result.getDeadLetterPolicy().getMaxRedeliverCount(), 1);
          assertEquals(result.getDeadLetterPolicy().getInitialSubscriptionName(), "dqlsub");
          assertNull(result.getConsumerSchema());
        }));
  }

  @Test
  void testNegativeAckRedeliveryBackoff() {
    Map<String, Object> consumerConfiguration = new HashMap<>();
    Map<String, Object> negativeAckRedeliveryBackoff = new HashMap<>();
    consumerConfiguration.put("negativeAckRedeliveryBackoff", negativeAckRedeliveryBackoff);
    negativeAckRedeliveryBackoff.put("minDelayMs", 10);
    negativeAckRedeliveryBackoff.put("maxDelayMs", 100);
    negativeAckRedeliveryBackoff.put("multiplier", 2.0);
    test(
        consumerConfiguration,
        (result -> {
          assertNull(result.getAckTimeoutRedeliveryBackoff());
          assertNotNull(result.getNegativeAckRedeliveryBackoff());
          assertNull(result.getDeadLetterPolicy());
          assertNull(result.getConsumerSchema());
        }));
  }

  @Test
  void testAckTimeoutRedeliveryBackoff() {
    Map<String, Object> consumerConfiguration = new HashMap<>();
    Map<String, Object> ackTimeoutRedeliveryBackoff = new HashMap<>();
    consumerConfiguration.put("ackTimeoutRedeliveryBackoff", ackTimeoutRedeliveryBackoff);
    ackTimeoutRedeliveryBackoff.put("minDelayMs", 10);
    ackTimeoutRedeliveryBackoff.put("maxDelayMs", 100);
    ackTimeoutRedeliveryBackoff.put("multiplier", 2.0);
    test(
        consumerConfiguration,
        (result -> {
          assertNotNull(result.getAckTimeoutRedeliveryBackoff());
          assertNull(result.getNegativeAckRedeliveryBackoff());
          assertNull(result.getDeadLetterPolicy());
          assertNull(result.getConsumerSchema());
        }));
  }
}
