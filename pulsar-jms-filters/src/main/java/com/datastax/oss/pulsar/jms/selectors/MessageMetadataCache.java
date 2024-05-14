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

import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.common.api.proto.MessageMetadata;

class MessageMetadataCache {
  final Map<String, String> rawProperties = new HashMap<>();
  final Map<String, Object> properties = new HashMap<>();
  private static final Object CACHED_NULL = new Object();

  MessageMetadataCache(MessageMetadata metadata) {
    // computing this is expensive because it involves a lot of string manipulation
    // protobuf has to parse the bytes and then convert them to Strings
    // so we want to do it only once
    // please note that when a selector references a property that is not
    // in the message we would end up in scanning the whole list
    metadata.getPropertiesList().forEach(p -> rawProperties.put(p.getKey(), p.getValue()));
  }

  Object getProperty(String key) {
    Object cached = properties.get(key);
    if (cached == CACHED_NULL) {
      return null;
    }
    if (cached != null) {
      return cached;
    }
    Object result = JMSFilter.getProperty(rawProperties, key);
    if (result == null) {
      properties.put(key, CACHED_NULL);
    } else {
      properties.put(key, result);
    }
    return result;
  }
}
