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
package io.streamnative.oss.pulsar.jms.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamnative.oss.pulsar.jms.PulsarConnectionFactory;
import io.streamnative.oss.pulsar.jms.shaded.org.apache.pulsar.client.impl.auth.AuthenticationToken;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class RelocatedClassesTest {

  @Test
  public void test() throws Exception {
    Map<String, Object> properties = new HashMap<>();

    // here we are using the repackaged Pulsar client and actually the class name is
    assertTrue(
        AuthenticationToken.class.getName().startsWith("io.streamnative.oss.pulsar.jms.shaded"));

    properties.put("authPlugin", "org.apache.pulsar.client.impl.auth.AuthenticationToken");

    Map<String, Object> consumerProperties = new HashMap<>();
    consumerProperties.put("something", "org.apache.pulsar.client.something");

    Map<String, Object> producerProperties = new HashMap<>();
    producerProperties.put("somethingElse", "org.apache.pulsar.somethingElse");

    properties.put("consumerConfig", consumerProperties);
    properties.put("producerConfig", producerProperties);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      Map<String, Object> adjustedConfiguration = factory.getConfiguration();
      assertEquals(AuthenticationToken.class.getName(), adjustedConfiguration.get("authPlugin"));

      assertEquals(
          "io.streamnative.oss.pulsar.jms.shaded.org.apache.pulsar.client.something",
          ((Map) adjustedConfiguration.get("consumerConfig")).get("something"));

      assertEquals(
          "io.streamnative.oss.pulsar.jms.shaded.org.apache.pulsar.somethingElse",
          ((Map) adjustedConfiguration.get("producerConfig")).get("somethingElse"));
    }
  }
}
