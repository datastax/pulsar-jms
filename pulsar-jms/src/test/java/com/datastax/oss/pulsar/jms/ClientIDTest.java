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

import static org.junit.jupiter.api.Assertions.fail;

import jakarta.jms.Connection;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.InvalidClientIDRuntimeException;
import jakarta.jms.JMSContext;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ClientIDTest {

  @Test
  public void duplicateClientIdExceptions() throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", "http://localhost:8080");
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {

      try (Connection connection = factory.createConnection()) {
        connection.setClientID("a");
        try (Connection connection2 = factory.createConnection()) {
          connection2.setClientID("a");
          fail("cannot set a clientId twice");
        } catch (InvalidClientIDException ok) {
        }
      }
      try (Connection connection2 = factory.createConnection()) {
        connection2.setClientID("a");
      }

      try (JMSContext connection = factory.createContext()) {
        connection.setClientID("a");
        try (JMSContext connection2 = factory.createContext()) {
          connection2.setClientID("a");
          fail("cannot set a clientId twice");
        } catch (InvalidClientIDRuntimeException ok) {
        }
      }
      try (JMSContext connection2 = factory.createContext()) {
        connection2.setClientID("a");
      }
    }
  }
}
