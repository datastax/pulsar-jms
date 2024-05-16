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
package io.streamnative.oss.pulsar.jms.rar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import javax.resource.spi.InvalidPropertyException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class PulsarActivationSpecTest {

  @Test
  public void testDestinationType() throws Exception {

    validateDestinationType("queue", false);
    validateDestinationType("topic", false);
    validateDestinationType("Queue", false);
    validateDestinationType("Topic", false);
    validateDestinationType("javax.jms.Queue", false);
    validateDestinationType("javax.jms.Topic", false);

    validateDestinationType("", true);
    validateDestinationType(null, true);
    validateDestinationType("somethingelse", true);
  }

  private static void validateDestinationType(String type, boolean expectError) throws Exception {
    PulsarActivationSpec spec = new PulsarActivationSpec();
    String configuration = "{}";
    spec.setConfiguration(configuration);
    spec.setDestination("ttt");
    spec.setSubscriptionName("subname");
    spec.setDestinationType(type);

    if (expectError) {
      assertThrows(InvalidPropertyException.class, spec::validate);
    } else {
      spec.validate();
    }
  }

  @Test
  public void testSubscriptionType() throws Exception {

    validateSubscriptionType("Durable", false);
    validateSubscriptionType("NonDurable", false);
    validateSubscriptionType("", true);
    validateSubscriptionType(null, true);
    validateSubscriptionType("somethingelse", true);
  }

  private static void validateSubscriptionType(String type, boolean expectError) throws Exception {
    PulsarActivationSpec spec = new PulsarActivationSpec();
    String configuration = "{}";
    spec.setConfiguration(configuration);
    spec.setDestination("topicNames");
    spec.setDestinationType("queue");
    spec.setSubscriptionType(type);

    if (expectError) {
      assertThrows(InvalidPropertyException.class, spec::validate);
    } else {
      spec.validate();
    }
  }

  @Test
  public void testSubscriptionMode() throws Exception {

    validateSubscriptionMode("Exclusive", false);
    validateSubscriptionMode("Shared", false);
    validateSubscriptionMode("", true);
    validateSubscriptionMode(null, true);
    validateSubscriptionMode("somethingelse", true);
  }

  private static void validateSubscriptionMode(String type, boolean expectError) throws Exception {
    PulsarActivationSpec spec = new PulsarActivationSpec();
    String configuration = "{}";
    spec.setConfiguration(configuration);
    spec.setDestination("topicNames");
    spec.setDestinationType("queue");
    spec.setSubscriptionMode(type);

    if (expectError) {
      assertThrows(InvalidPropertyException.class, spec::validate);
    } else {
      spec.validate();
    }
  }

  @Test
  public void testDestination() throws Exception {
    PulsarActivationSpec spec = new PulsarActivationSpec();
    String configuration = "{}";
    spec.setConfiguration(configuration);
    spec.setDestination("queue");
    spec.validate();

    spec.setDestination(null);
    assertThrows(InvalidPropertyException.class, spec::validate);

    spec.setDestination("");
    assertThrows(InvalidPropertyException.class, spec::validate);

    spec.setDestination("topicNames");
    spec.validate();
  }

  @Test
  public void testMergeConfiguration() throws Exception {
    PulsarActivationSpec spec = new PulsarActivationSpec();
    String configuration = "foo";
    spec.setConfiguration(configuration);
    assertEquals("foo", spec.getMergedConfiguration("bar"));
    assertEquals("foo", spec.getConfiguration());

    spec.setConfiguration("");
    assertEquals("bar", spec.getMergedConfiguration("bar"));
    assertEquals("", spec.getConfiguration());

    spec.setConfiguration(null);
    assertEquals("bar", spec.getMergedConfiguration("bar"));
    assertEquals(null, spec.getConfiguration());

    spec.setConfiguration("{}");
    assertEquals("bar", spec.getMergedConfiguration("bar"));
    assertEquals("{}", spec.getConfiguration());

    spec.setConfiguration("{   }");
    assertEquals("bar", spec.getMergedConfiguration("bar"));
    assertEquals("{   }", spec.getConfiguration());
  }

  @Test
  public void testConsumerConfiguration() throws Exception {
    PulsarActivationSpec spec = new PulsarActivationSpec();
    String configuration = "{\"deadLetterPolicy\":{\"deadLetterTopic\":\"dlq-topic\"}}";
    spec.setConsumerConfig(configuration);
    Map<String, Object> parsed = spec.buildConsumerConfiguration();
    Map<String, Object> deadLetterPolicy = (Map<String, Object>) parsed.get("deadLetterPolicy");
    assertEquals("dlq-topic", deadLetterPolicy.get("deadLetterTopic"));

    spec.setConsumerConfig(null);
    assertTrue(spec.buildConsumerConfiguration().isEmpty());

    spec.setConsumerConfig("");
    assertTrue(spec.buildConsumerConfiguration().isEmpty());

    spec.setConsumerConfig("{}");
    assertTrue(spec.buildConsumerConfiguration().isEmpty());

    spec.setConsumerConfig("   {  }   ");
    assertTrue(spec.buildConsumerConfiguration().isEmpty());
  }
}
