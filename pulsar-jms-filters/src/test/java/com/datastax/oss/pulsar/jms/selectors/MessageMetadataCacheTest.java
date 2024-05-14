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
package com.datastax.oss.pulsar.jms.selectors;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.pulsar.common.api.proto.MessageMetadata;

class MessageMetadataCacheTest {

  @org.junit.jupiter.api.Test
  void testGetProperty() {
    MessageMetadata metadata = new MessageMetadata();
    metadata.addProperty().setKey("foo").setValue("bar");
    metadata.addProperty().setKey("i_jsmtype").setValue("int");
    metadata.addProperty().setKey("i").setValue("5");
    MessageMetadataCache cache = new MessageMetadataCache(metadata);
    assertNull(cache.getProperty("key"));
    assertEquals("bar", cache.getProperty("foo"));
    assertEquals(5, cache.getProperty("i"));
  }
}
