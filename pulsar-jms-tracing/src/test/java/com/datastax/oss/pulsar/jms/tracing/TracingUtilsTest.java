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
package com.datastax.oss.pulsar.jms.tracing;

import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class TracingUtilsTest {

  List<String> traces = new ArrayList<>();
  private Tracer mockTracer =
      new Tracer() {
        @Override
        public void trace(EventCategory reason, String msg) {
          traces.add(msg);
        }
      };

  private static void trace(Tracer mockTracer, Map<String, Object> traceDetails) {
    TracingUtils.trace(mockTracer, EventCategory.MSG, EventSubCategory.PRODUCED, traceDetails);
  }

  @Test
  void traceTest() {
    traces.clear();
    trace(mockTracer, null);
    assertEquals(1, traces.size());
    assertEquals("{\"event\":\"MSG_PRODUCED\",\"traceDetails\":null}", traces.get(0));

    Map<String, Object> map = new TreeMap<>();

    traces.clear();
    trace(mockTracer, map);
    assertEquals(1, traces.size());
    assertEquals("{\"event\":\"MSG_PRODUCED\",\"traceDetails\":{}}", traces.get(0));

    map.put("key1", "value1");

    traces.clear();
    trace(mockTracer, map);
    assertEquals(1, traces.size());
    assertEquals(
        "{\"event\":\"MSG_PRODUCED\",\"traceDetails\":{\"key1\":\"value1\"}}", traces.get(0));
  }

  // todo:
  //    @Test
  //    void getConnectionDetailsTest() {
  //    }
  //
  //    @Test
  //    void getSubscriptionDetailsTest() {
  //    }
  //
  //    @Test
  //    void getConsumerDetailsTest() {
  //    }
  //
  //    @Test
  //    void getProducerDetails() {
  //    }
  //
  //    @Test
  //    void getMessageMetadataDetailsTest() {
  //    }
  //
  //    @Test
  //    void getEntryDetailsTest() {
  //    }
  //
  //    @Test
  //    void getPublishContextDetailsTest() {
  //    }

  @Test
  void traceByteBufTest() {
    Map<String, Object> traceDetails = new TreeMap<>();

    int maxBinaryDataLength = 100;

    traceByteBuf("key", null, traceDetails, maxBinaryDataLength);
    assertEquals(0, traceDetails.size());

    Random rand = new Random();

    ByteBuf small = Unpooled.buffer(20);
    for (int i = 0; i < 20; i++) {
      char randomChar = (char) (rand.nextInt(26) + 'a');
      small.writeByte(randomChar);
    }
    String smallStr = small.toString(StandardCharsets.UTF_8);
    assertEquals(1, small.refCnt());

    traceByteBuf("key", small, traceDetails, maxBinaryDataLength);
    assertEquals(1, traceDetails.size());
    assertEquals(20, ((String) traceDetails.get("key")).length());
    assertEquals(smallStr, traceDetails.get("key"));
    assertEquals(0, small.refCnt());

    ByteBuf big = Unpooled.buffer(maxBinaryDataLength + 100);
    for (int i = 0; i < maxBinaryDataLength + 100; i++) {
      char randomChar = (char) (rand.nextInt(26) + 'a');
      big.writeByte(randomChar);
    }
    assertEquals(1, big.refCnt());
    String bigStr = big.toString(StandardCharsets.UTF_8);

    traceDetails.clear();
    traceByteBuf("key", big, traceDetails, maxBinaryDataLength);
    assertEquals(1, traceDetails.size());
    assertEquals(maxBinaryDataLength + 3, ((String) traceDetails.get("key")).length());
    assertEquals(bigStr.substring(0, maxBinaryDataLength) + "...", traceDetails.get("key"));
    assertEquals(0, big.refCnt());
  }
}
