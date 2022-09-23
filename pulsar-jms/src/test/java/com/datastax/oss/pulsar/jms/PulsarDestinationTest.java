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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.jms.InvalidDestinationException;
import org.junit.jupiter.api.Test;

public class PulsarDestinationTest {

  @Test
  public void testExtractSubscriptionNameForTopic() throws Exception {
    PulsarTopic topic = new PulsarTopic("test");
    assertNull(topic.extractSubscriptionName(false));

    topic = new PulsarTopic("test:sub");
    assertNull(topic.extractSubscriptionName(false));
    assertNull(topic.extractSubscriptionName(true));
  }

  @Test
  public void testExtractSubscriptionNameForQueuePrependName() throws Exception {
    PulsarQueue topic = new PulsarQueue("test");
    assertNull(topic.extractSubscriptionName(true));

    topic = new PulsarQueue("test:sub");
    assertEquals("test:sub", topic.extractSubscriptionName(true));

    topic = new PulsarQueue("test:sub");
    assertEquals("test:sub", topic.extractSubscriptionName(true));

    topic = new PulsarQueue("persistent://public/default/test:sub");
    assertEquals("test:sub", topic.extractSubscriptionName(true));

    topic = new PulsarQueue("persistent://public/default/test");
    assertEquals(null, topic.extractSubscriptionName(true));

    topic = new PulsarQueue("regex:persistent://public/default/test");
    assertEquals(null, topic.extractSubscriptionName(true));

    topic = new PulsarQueue("regexp:persistent://public/default/test:sub");
    assertEquals("test:sub", topic.extractSubscriptionName(true));

    assertThrows(
        InvalidDestinationException.class,
        () -> {
          PulsarQueue topic2 = new PulsarQueue("persistent://public/default/test:");
          topic2.extractSubscriptionName(true);
        });
  }

  @Test
  public void testExtractSubscriptionNameForQueue() throws Exception {
    PulsarQueue topic = new PulsarQueue("test");
    assertNull(topic.extractSubscriptionName(false));

    topic = new PulsarQueue("test:sub");
    assertEquals("sub", topic.extractSubscriptionName(false));

    topic = new PulsarQueue("test:sub");
    assertEquals("sub", topic.extractSubscriptionName(false));

    topic = new PulsarQueue("persistent://public/default/test:sub");
    assertEquals("sub", topic.extractSubscriptionName(false));

    topic = new PulsarQueue("regex:persistent://public/default/test:sub");
    assertEquals("sub", topic.extractSubscriptionName(false));

    topic = new PulsarQueue("persistent://public/default/test");
    assertEquals(null, topic.extractSubscriptionName(false));

    topic = new PulsarQueue("regex:persistent://public/default/test");
    assertEquals(null, topic.extractSubscriptionName(false));

    assertThrows(
        InvalidDestinationException.class,
        () -> {
          PulsarQueue topic2 = new PulsarQueue("persistent://public/default/test:");
          topic2.extractSubscriptionName(false);
        });
  }
}
