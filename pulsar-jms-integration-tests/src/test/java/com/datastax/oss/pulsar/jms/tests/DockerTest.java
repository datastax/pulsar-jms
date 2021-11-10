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
package com.datastax.oss.pulsar.jms.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import javax.jms.Destination;
import javax.jms.JMSContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DockerTest {

  @TempDir public Path temporaryDir;

  @Test
  public void testPulsar272() throws Exception {
    test("apachepulsar/pulsar:2.7.2", false);
  }

  @Test
  public void testPulsar281() throws Exception {
    test("apachepulsar/pulsar:2.8.1", false);
  }

  @Test
  public void testPulsar281Transactions() throws Exception {
    test("apachepulsar/pulsar:2.8.1", true);
  }

  private void test(String image, boolean transactions) throws Exception {
    try (PulsarContainer pulsarContainer = new PulsarContainer(image, transactions); ) {
      pulsarContainer.start();
      Map<String, Object> properties = new HashMap<>();
      properties.put("brokerServiceUrl", pulsarContainer.getPulsarBrokerUrl());
      properties.put("webServiceUrl", pulsarContainer.getHttpServiceUrl());
      properties.put("enableTransaction", transactions);
      try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
          JMSContext context =
              factory.createContext(
                  transactions ? JMSContext.SESSION_TRANSACTED : JMSContext.CLIENT_ACKNOWLEDGE);
          JMSContext context2 =
              factory.createContext(
                  transactions ? JMSContext.SESSION_TRANSACTED : JMSContext.CLIENT_ACKNOWLEDGE)) {
        Destination queue = context.createQueue("test");
        context.createProducer().send(queue, "foo");
        if (transactions) {
          context.commit();
        }
        assertEquals("foo", context2.createConsumer(queue).receiveBody(String.class));
        if (transactions) {
          context2.commit();
        }
      }
    }
  }
}
