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
package io.streamnative.oss.pulsar.jms.selectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.jms.DeliveryMode;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.jupiter.api.Test;

class SelectorSupportTest {

  @Test
  public void test() throws Exception {
    match(true, "foo='bar' or foo='bar' or foo='bar'");
    match(true, "foo='bar' or foo='bar' or foo='other'");
    match(false, "foo='baz' or foo='other'");
    match(true, "foo='baz' or foo='bar'");

    match(true, "foo='bar' and foo='bar' and foo='bar'");
    match(false, "foo='bar' and foo='bar' and foo='other'");
    match(false, "foo='bar' and foo='other'");
    match(true, "foo='bar' and foo='bar'");
    match(false, "foo='other'");
    match(false, "foo is null");
    match(true, "foo is not null");
    match(true, "undefinedProperty is null");
    match(false, "not undefinedProperty");
    match(false, "undefinedProperty");
  }

  @Test
  public void testSpecialKeywords() throws Exception {
    match(true, "JMSMessageID = '0:1:9:-1'");
    match(true, "JMSMessageID is not null");
    match(false, "JMSMessageID is null");
    match(true, "JMSReplyTo = 'queue://persistent://public/default/testReply'");
    match(true, "JMSDestination = 'topic://persistent://public/default/test'");
    match(true, "JMSCorrelationID = '0:1:2:3'");
    match(true, "JMSDeliveryMode = 'PERSISTENT'");
    match(true, "JMSType = 'my-type'");
    match(true, "JMSExpiration = 1234");
    match(true, "JMSPriority = 5");
    match(true, "JMSTimestamp = 5234234");
  }

  private static void match(boolean expected, String selector) throws Exception {
    SelectorSupport build = SelectorSupport.build(selector, true);
    Map<String, AtomicInteger> propertyAccessCount = new HashMap<>();
    Map<String, Object> properties = new HashMap<>();
    properties.put("foo", "bar");
    properties.put("JMSMessageID", "0:1:9:-1");
    properties.put("JMSReplyTo", new ActiveMQQueue("persistent://public/default/testReply"));
    properties.put("JMSDestination", new ActiveMQTopic("persistent://public/default/test"));

    properties.put("JMSCorrelationID", "0:1:2:3");
    properties.put("JMSDeliveryMode", DeliveryMode.PERSISTENT);
    properties.put("JMSType", "my-type");
    properties.put("JMSExpiration", 1234L);
    properties.put("JMSPriority", 5);
    properties.put("JMSTimestamp", 5234234L);

    Function<String, Object> spy =
        (k) -> {
          propertyAccessCount.computeIfAbsent(k, v -> new AtomicInteger()).incrementAndGet();
          return properties.get(k);
        };
    assertEquals(expected, build.matches(spy));

    // SelectorSupport has a cache to prevent multiple requests for the same key
    propertyAccessCount.forEach(
        (property, count) -> {
          assertEquals(1, count.get());
        });
  }
}
