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

import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.jms.JMSException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;

@Slf4j
public class JMSFilter implements EntryFilter {

  private final ConcurrentHashMap<String, SelectorSupport> selectors = new ConcurrentHashMap<>();

  @Override
  public FilterResult filterEntry(Entry entry, FilterContext context) {
    Subscription subscription = context.getSubscription();
    if (!(subscription instanceof PersistentSubscription)) {
      return FilterResult.ACCEPT;
    }
    PersistentSubscription persistentSubscription = (PersistentSubscription) subscription;
    Map<String, String> subscriptionProperties = persistentSubscription.getSubscriptionProperties();
    String jmsSelector = subscriptionProperties.get("jms.selector");
    if (jmsSelector == null || jmsSelector.isEmpty()) {
      return FilterResult.ACCEPT;
    }

    MessageMetadata metadata = context.getMsgMetadata();
    if (metadata.hasMarkerType()) {
      // special messages...ignore
      return FilterResult.ACCEPT;
    }
    SelectorSupport selector =
        selectors.computeIfAbsent(
            jmsSelector,
            s -> {
              try {
                return SelectorSupport.build(s, true);
              } catch (JMSException err) {
                log.error("Cannot build selector from '{}'", jmsSelector, err);
                return null;
              }
            });
    if (selector == null) {
      return FilterResult.REJECT;
    }
    try {
      if (metadata.hasNumMessagesInBatch()) {
        ByteBuf payload = entry.getDataBuffer().slice();
        Commands.skipMessageMetadata(payload);
        final int uncompressedSize = metadata.getUncompressedSize();
        final CompressionCodec codec =
            CompressionCodecProvider.getCompressionCodec(metadata.getCompression());
        final ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);
        try {
          int numMessages = metadata.getNumMessagesInBatch();
          // we cannot do much here.
          // because we can accept or reject only the whole batch.
          boolean oneAccepted = false;
          for (int i = 0; i < numMessages; i++) {
            final SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
            final ByteBuf singleMessagePayload =
                Commands.deSerializeSingleMessageInBatch(
                    uncompressedPayload, singleMessageMetadata, i, numMessages);
            final long timestamp =
                (metadata.getEventTime() > 0) ? metadata.getEventTime() : metadata.getPublishTime();

            String key = metadata.hasPartitionKey() ? metadata.getPartitionKey() : null;
            Map<String, Object> typedProperties =
                buildProperties(
                    singleMessageMetadata.getPropertiesCount(),
                    singleMessageMetadata.getPropertiesList());
            boolean matches = selector.matches(typedProperties);
            log.info(
                "filterBatchEntry {} " + "{} {} {} {} matches {}",
                key,
                entry,
                subscriptionProperties,
                typedProperties,
                matches);
            oneAccepted = oneAccepted || matches;
            singleMessagePayload.release();
          }
          return oneAccepted ? FilterResult.ACCEPT : FilterResult.REJECT;
        } finally {
          uncompressedPayload.release();
        }
      } else {
        String key = metadata.hasPartitionKey() ? metadata.getPartitionKey() : null;
        Map<String, Object> typedProperties =
            buildProperties(metadata.getPropertiesCount(), metadata.getPropertiesList());
        boolean matches = selector.matches(typedProperties);
        log.info(
            "filterEntry {} {} {} {} matches {}",
            key,
            entry,
            subscriptionProperties,
            typedProperties,
            matches);
        return matches ? FilterResult.ACCEPT : FilterResult.REJECT;
      }

    } catch (Throwable err) {
      log.error("Error while decoding batch entry", err);
      return FilterResult.REJECT;
    }
  }

  private Map<String, Object> buildProperties(int propertiesCount, List<KeyValue> propertiesList) {
    Map<String, String> properties = new HashMap<>();
    if (propertiesCount > 0) {
      propertiesList.forEach(
          kv -> {
            properties.put(kv.getKey(), kv.getValue());
          });
    }
    Map<String, Object> typedProperties = new HashMap<>();
    properties.forEach(
        (k, v) -> {
          if (!k.equals("_jsmtype")) {
            typedProperties.put(k, getObjectProperty(k, properties));
          }
        });
    return typedProperties;
  }

  @Override
  public void close() {
    log.info("closing {}", this);
  }

  private static String propertyType(String name) {
    return name + "_jsmtype";
  }

  public static Object getObjectProperty(String name, Map<String, String> properties) {

    String value = properties.getOrDefault(name, null);
    if (value == null) {
      return null;
    }
    String type = properties.getOrDefault(propertyType(name), "string");
    switch (type) {
      case "string":
        return value;
      case "boolean":
        return Boolean.parseBoolean(value);
      case "float":
        return Float.parseFloat(value);
      case "double":
        return Double.parseDouble(value);
      case "int":
        return Integer.parseInt(value);
      case "short":
        return Short.parseShort(value);
      case "byte":
        return Byte.parseByte(value);
      case "long":
        return Long.parseLong(value);
      default:
        // string
        return value;
    }
  }
}
