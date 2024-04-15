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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class TracingUtilsTest {

  List<String> traces = new ArrayList<>();
  private Tracer mockTracer =
      new Tracer() {
        @Override
        public void trace(String msg) {
          traces.add(msg);
        }
      };

  @Test
  void traceTest() {
    traces.clear();
    trace(mockTracer, "msg", null);
    assertEquals(1, traces.size());
    assertEquals("{\"message\":\"msg\",\"traceDetails\":null}", traces.get(0));

    Map<String, Object> map = new TreeMap<>();

    traces.clear();
    trace(mockTracer, "msg", map);
    assertEquals(1, traces.size());
    assertEquals("{\"message\":\"msg\",\"traceDetails\":{}}", traces.get(0));

    map.put("key1", "value1");

    traces.clear();
    trace(mockTracer, "msg", map);
    assertEquals(1, traces.size());
    assertEquals("{\"message\":\"msg\",\"traceDetails\":{\"key1\":\"value1\"}}", traces.get(0));
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

    traceByteBuf("key", null, traceDetails);
    assertEquals(0, traceDetails.size());

    ByteBuf small = Unpooled.buffer(20);
    for (int i = 0; i < 20; i++) {
      small.writeByte(i);
    }
    traceByteBuf("key", small, traceDetails);
    assertEquals(1, traceDetails.size());
    assertEquals(42, ((String) traceDetails.get("key")).length());
    assertEquals("0x000102030405060708090a0b0c0d0e0f10111213", traceDetails.get("key"));

    ByteBuf big = Unpooled.buffer(MAX_DATA_LENGTH + 100);
    for (int i = 0; i < MAX_DATA_LENGTH + 100; i++) {
      big.writeByte(i);
    }

    traceDetails.clear();
    traceByteBuf("key", big, traceDetails);
    assertEquals(1, traceDetails.size());
    assertEquals(2 + 2 * MAX_DATA_LENGTH, ((String) traceDetails.get("keySlice")).length());
    assertTrue(((String) traceDetails.get("keySlice")).startsWith("0x000102"));
  }
}