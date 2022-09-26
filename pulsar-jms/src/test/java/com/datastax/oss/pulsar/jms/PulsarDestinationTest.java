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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.function.BiConsumer;
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

    topic = new PulsarQueue("multi:test:sub");
    assertEquals("test:sub", topic.extractSubscriptionName(true));

    topic = new PulsarQueue("multi:test:sub");
    assertEquals("test:sub", topic.extractSubscriptionName(true));

    topic = new PulsarQueue("multi:persistent://public/default/test:sub");
    assertEquals("test:sub", topic.extractSubscriptionName(true));

    topic = new PulsarQueue("multi:persistent://public/default/test");
    assertEquals(null, topic.extractSubscriptionName(true));

    topic = new PulsarQueue("multi:regex:persistent://public/default/test");
    assertEquals(null, topic.extractSubscriptionName(true));

    topic = new PulsarQueue("multi:regexp:persistent://public/default/test:sub");
    assertEquals("test:sub", topic.extractSubscriptionName(true));

    assertThrows(
        InvalidDestinationException.class,
        () -> {
          PulsarQueue topic2 = new PulsarQueue("multi:persistent://public/default/test:");
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

    topic = new PulsarQueue("multi:test:sub");
    assertEquals("sub", topic.extractSubscriptionName(false));

    topic = new PulsarQueue("multi:test:sub");
    assertEquals("sub", topic.extractSubscriptionName(false));

    topic = new PulsarQueue("multi:persistent://public/default/test:sub");
    assertEquals("sub", topic.extractSubscriptionName(false));

    topic = new PulsarQueue("multi:regex:persistent://public/default/test:sub");
    assertEquals("sub", topic.extractSubscriptionName(false));

    topic = new PulsarQueue("multi:persistent://public/default/test");
    assertEquals(null, topic.extractSubscriptionName(false));

    topic = new PulsarQueue("multi:regex:persistent://public/default/test");
    assertEquals(null, topic.extractSubscriptionName(false));

    assertThrows(
        InvalidDestinationException.class,
        () -> {
          PulsarQueue topic2 = new PulsarQueue("multi:persistent://public/default/test:");
          topic2.extractSubscriptionName(false);
        });
  }

  private void testMultiTopic(
      PulsarDestination destination,
      int expectedCount,
      BiConsumer<Integer, PulsarDestination> verifier)
      throws Exception {
    assertTrue(destination.isMultiTopic());
    List<PulsarDestination> destinationList = destination.getDestinations();
    assertEquals(expectedCount, destinationList.size());
    for (int i = 0; i < destinationList.size(); i++) {
      verifier.accept(i, destinationList.get(i));
      assertTrue(destination.getClass() == destinationList.get(i).getClass());
    }
  }

  @Test
  public void testMultiDestinations() throws Exception {
    testMultiTopic(
        new PulsarQueue("multi:test"),
        1,
        (i, d) -> {
          assertEquals(new PulsarQueue("test"), d);
        });
    testMultiTopic(
        new PulsarQueue("multi:test,test2"),
        2,
        (i, d) -> {
          switch (i) {
            case 0:
              assertEquals(new PulsarQueue("test"), d);
              break;
            case 1:
              assertEquals(new PulsarQueue("test2"), d);
              break;
            default:
              fail();
          }
        });
    testMultiTopic(
        new PulsarQueue("multi:test,test2:sub"),
        2,
        (i, d) -> {
          switch (i) {
            case 0:
              assertEquals(new PulsarQueue("test:sub"), d);
              break;
            case 1:
              assertEquals(new PulsarQueue("test2:sub"), d);
              break;
            default:
              fail();
          }
        });
    testMultiTopic(
        new PulsarQueue("multi:test:sub"),
        1,
        (i, d) -> {
          switch (i) {
            case 0:
              assertEquals(new PulsarQueue("test:sub"), d);
              break;
            default:
              fail();
          }
        });
    assertThrows(
        InvalidDestinationException.class,
        () -> {
          testMultiTopic(new PulsarQueue("multi:"), 0, (i, d) -> {});
        });
    assertThrows(
        InvalidDestinationException.class,
        () -> {
          testMultiTopic(new PulsarQueue("multi:,"), 0, (i, d) -> {});
        });
    assertThrows(
        InvalidDestinationException.class,
        () -> {
          testMultiTopic(new PulsarQueue("multi:,,"), 0, (i, d) -> {});
        });
    testMultiTopic(new PulsarQueue("multi:test,"), 1, (i, d) -> {});
    assertThrows(
        InvalidDestinationException.class,
        () -> {
          testMultiTopic(new PulsarQueue("multi:,test"), 1, (i, d) -> {});
        });
    assertThrows(
        InvalidDestinationException.class,
        () -> {
          testMultiTopic(new PulsarQueue("multi:test,,test"), 2, (i, d) -> {});
        });

    testMultiTopic(
        new PulsarTopic("multi:test"),
        1,
        (i, d) -> {
          assertEquals(new PulsarTopic("test"), d);
        });
    testMultiTopic(
        new PulsarTopic("multi:test,test2"),
        2,
        (i, d) -> {
          switch (i) {
            case 0:
              assertEquals(new PulsarTopic("test"), d);
              break;
            case 1:
              assertEquals(new PulsarTopic("test2"), d);
              break;
            default:
              fail();
          }
        });
    testMultiTopic(
        new PulsarTopic("multi:test,test2:sub"),
        2,
        (i, d) -> {
          switch (i) {
            case 0:
              assertEquals(new PulsarTopic("test"), d);
              break;
            case 1:
              assertEquals(new PulsarTopic("test2:sub"), d);
              break;
            default:
              fail();
          }
        });
    testMultiTopic(
        new PulsarTopic("multi:test:sub"),
        1,
        (i, d) -> {
          switch (i) {
            case 0:
              assertEquals(new PulsarTopic("test:sub"), d);
              break;
            default:
              fail();
          }
        });
    assertThrows(
        InvalidDestinationException.class,
        () -> {
          testMultiTopic(new PulsarTopic("multi:"), 0, (i, d) -> {});
        });
    assertThrows(
        InvalidDestinationException.class,
        () -> {
          testMultiTopic(new PulsarTopic("multi:,"), 0, (i, d) -> {});
        });
    assertThrows(
        InvalidDestinationException.class,
        () -> {
          testMultiTopic(new PulsarTopic("multi:,,"), 0, (i, d) -> {});
        });
    testMultiTopic(new PulsarTopic("multi:test,"), 1, (i, d) -> {});
    assertThrows(
        InvalidDestinationException.class,
        () -> {
          testMultiTopic(new PulsarTopic("multi:,test"), 1, (i, d) -> {});
        });
    assertThrows(
        InvalidDestinationException.class,
        () -> {
          testMultiTopic(new PulsarTopic("multi:test,,test"), 2, (i, d) -> {});
        });
  }
}
