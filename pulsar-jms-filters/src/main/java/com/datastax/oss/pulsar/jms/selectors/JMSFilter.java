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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
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
    Consumer consumer = context.getConsumer();
    Map<String, String> consumerMetadata = consumer.getMetadata();
    boolean jmsFiltering = "true".equals(consumerMetadata.get("jms.filtering"));
    if (!jmsFiltering) {
      return FilterResult.ACCEPT;
    }
    CommandSubscribe.SubType subType = consumer.subType();
    final boolean isExclusive;
    switch (subType) {
      case Exclusive:
      case Failover:
        isExclusive = true;
        break;
      default:
        isExclusive = false;
        break;
    }
    String topicName = context.getSubscription().getTopicName();
    String jmsSelector = consumerMetadata.getOrDefault("jms.selector", "");
    String destinationTypeForTheClient = consumerMetadata.get("jms.destination.type");
    String jmsSelectorRejectAction = consumerMetadata.get("jms.selector.reject.action");
    // noLocal filter
    String filterJMSConnectionID = consumerMetadata.getOrDefault("jms.filter.JMSConnectionID", "");
    boolean forceDropRejected =
        "true".equals(consumerMetadata.getOrDefault("jms.force.drop.rejected", "false"));
    final FilterResult rejectResultForSelector;
    if ("drop".equals(jmsSelectorRejectAction)) {
      // this is the common behaviour for a Topics
      rejectResultForSelector = FilterResult.REJECT;
    } else {
      // this is the common behaviour for a Queue
      rejectResultForSelector = FilterResult.RESCHEDULE;
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
                return SelectorSupport.build(s, !jmsSelector.isEmpty());
              } catch (JMSException err) {
                log.error("Cannot build selector from '{}'", jmsSelector, err);
                return null;
              }
            });
    if (selector == null && !jmsSelector.isEmpty()) {
      // error creating the selector. try again later
      return FilterResult.RESCHEDULE;
    }

    try {
      if (metadata.hasNumMessagesInBatch()) {

        // this is batch message
        // we can reject/reschedule it only if all the messages are to be rejects
        // we must accept it if at least one message passes the filters

        ByteBuf payload = entry.getDataBuffer().slice();
        Commands.skipMessageMetadata(payload);
        final int uncompressedSize = metadata.getUncompressedSize();
        final CompressionCodec codec =
            CompressionCodecProvider.getCompressionCodec(metadata.getCompression());
        final ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);
        try {
          int numMessages = metadata.getNumMessagesInBatch();
          boolean oneAccepted = false;
          boolean allExpired = true;
          for (int i = 0; i < numMessages; i++) {
            final SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
            final ByteBuf singleMessagePayload =
                Commands.deSerializeSingleMessageInBatch(
                    uncompressedPayload, singleMessageMetadata, i, numMessages);
            try {
              Map<String, Object> typedProperties =
                  buildProperties(
                      singleMessageMetadata.getPropertiesCount(),
                      singleMessageMetadata.getPropertiesList());

              // noLocal filter
              // all the messages in the batch come from the Producer/Connection
              // so we can reject the whole batch immediately at the first entry
              if (!filterJMSConnectionID.isEmpty()
                  && filterJMSConnectionID.equals(typedProperties.get("JMSConnectionID"))) {
                if (isExclusive || forceDropRejected) {
                  return FilterResult.REJECT;
                } else {
                  return FilterResult.RESCHEDULE;
                }
              }

              // timeToLive filter
              long jmsExpiration = getJMSExpiration(typedProperties);
              if (jmsExpiration > 0 && System.currentTimeMillis() > jmsExpiration) {
                // we are going to send the batch to the client
                // in this case, this way the client
                // can discard it
              } else {
                allExpired = false;
              }

              boolean matches = true;

              if (selector != null) {
                matches =
                    matches(
                        typedProperties,
                        singleMessageMetadata,
                        null,
                        topicName,
                        selector,
                        destinationTypeForTheClient,
                        jmsExpiration);
              }

              oneAccepted = oneAccepted || matches;
            } finally {
              singleMessagePayload.release();
            }
          }
          if (allExpired) {
            return FilterResult.REJECT;
          }
          if (oneAccepted) {
            return FilterResult.ACCEPT;
          }
          return rejectResultForSelector;
        } finally {
          uncompressedPayload.release();
        }
      } else {

        // here we are dealing with a single message,
        // so we can reject the message more easily

        Map<String, Object> typedProperties =
            buildProperties(metadata.getPropertiesCount(), metadata.getPropertiesList());

        // timetoLive filter
        long jmsExpiration = getJMSExpiration(typedProperties);
        if (jmsExpiration > 0 && System.currentTimeMillis() > jmsExpiration) {
          // message expired, this does not depend on the Consumer
          // we can to REJECT it immediately
          return FilterResult.REJECT;
        }

        boolean matches = true;
        if (selector != null) {
          matches =
              matches(
                  typedProperties,
                  null,
                  metadata,
                  topicName,
                  selector,
                  destinationTypeForTheClient,
                  jmsExpiration);
        }

        if (!filterJMSConnectionID.isEmpty()
            && filterJMSConnectionID.equals(typedProperties.get("JMSConnectionID"))) {
          if (isExclusive || forceDropRejected) {
            return FilterResult.REJECT;
          } else {
            return FilterResult.RESCHEDULE;
          }
        }

        if (matches) {
          log.info("result {} {} {}", entry.getPosition(), FilterResult.ACCEPT, typedProperties);
          return FilterResult.ACCEPT;
        }
        log.info("result {} {} {}", entry.getPosition(), rejectResultForSelector, typedProperties);
        return rejectResultForSelector;
      }

    } catch (Throwable err) {
      log.error("Error while decoding batch entry", err);
      return FilterResult.REJECT;
    }
  }

  private static long getJMSExpiration(Map<String, Object> typedProperties) {
    long jmsExpiration = 0;
    if (typedProperties.containsKey("JMSExpiration")) {
      try {
        jmsExpiration = Long.parseLong(typedProperties.get("JMSExpiration") + "");
      } catch (NumberFormatException err) {
        // cannot decode JMSExpiration
      }
    }
    return jmsExpiration;
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
    selectors.clear();
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

  private static String safeString(Object value) {
    return value == null ? null : value.toString();
  }

  private static boolean matches(
      Map<String, Object> typedProperties,
      SingleMessageMetadata singleMessageMetadata,
      MessageMetadata metadata,
      String topicName,
      SelectorSupport selector,
      String destinationTypeForTheClient,
      long jmsExpiration)
      throws JMSException {

    String _jmsReplyTo = safeString(typedProperties.get("JMSReplyTo"));
    Destination jmsReplyTo = null;
    if (_jmsReplyTo != null) {
      String jmsReplyToType = typedProperties.get("JMSReplyToType") + "";
      switch (jmsReplyToType) {
        case "topic":
          jmsReplyTo = new ActiveMQTopic(_jmsReplyTo);
          break;
        default:
          jmsReplyTo = new ActiveMQQueue(_jmsReplyTo);
      }
    }

    Destination destination =
        "queue".equalsIgnoreCase(destinationTypeForTheClient)
            ? new ActiveMQQueue(topicName)
            : new ActiveMQTopic(topicName);

    String jmsType = safeString(typedProperties.get("JMSType"));
    String messageId = safeString(typedProperties.get("JMSMessageId"));
    String correlationId = null;
    String _correlationId = safeString(typedProperties.get("JMSCorrelationID"));
    if (_correlationId != null) {
      correlationId = new String(Base64.getDecoder().decode(correlationId), StandardCharsets.UTF_8);
    }
    int jmsPriority = Message.DEFAULT_PRIORITY;
    if (typedProperties.containsKey("JMSPriority")) {
      try {
        jmsPriority = Integer.parseInt(typedProperties.get("JMSPriority") + "");
      } catch (NumberFormatException err) {
        // cannot decode priority, not a big deal as it is not supported in Pulsar
      }
    }
    int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
    if (typedProperties.containsKey("JMSDeliveryMode")) {
      try {
        deliveryMode = Integer.parseInt(typedProperties.get("JMSDeliveryMode") + "");
      } catch (NumberFormatException err) {
        // cannot decode deliveryMode, not a big deal as it is not supported in Pulsar
      }
    }

    // this is optional
    long jmsTimestamp = 0;

    if (singleMessageMetadata != null) {

      if (singleMessageMetadata.hasEventTime()) {
        jmsTimestamp = singleMessageMetadata.getEventTime();
      }

      // JMSXDeliveryCount not supported here
      // typedProperties.put("JMSXDeliveryCount", (singleMessageMetadata.getRedeliveryCount() + 1) +
      // "");

      if (singleMessageMetadata.hasPartitionKey()) {
        typedProperties.put("JMSXGroupID", singleMessageMetadata.getPartitionKey());
      } else {
        typedProperties.put("JMSXGroupID", "");
      }

      if (!typedProperties.containsKey("JMSXGroupSeq")) {
        if (singleMessageMetadata.hasSequenceId()) {
          typedProperties.put("JMSXGroupSeq", singleMessageMetadata.getSequenceId() + "");
        } else {
          typedProperties.put("JMSXGroupSeq", "0");
        }
      }

    } else {
      if (metadata.hasEventTime()) {
        jmsTimestamp = metadata.getEventTime();
      }

      // JMSXDeliveryCount not supported here
      // typedProperties.put("JMSXDeliveryCount", (metadata.getRedeliveryCount() + 1) + "");

      if (metadata.hasPartitionKey()) {
        typedProperties.put("JMSXGroupID", metadata.getPartitionKey());
      } else {
        typedProperties.put("JMSXGroupID", "");
      }

      if (!typedProperties.containsKey("JMSXGroupSeq")) {
        if (metadata.hasSequenceId()) {
          typedProperties.put("JMSXGroupSeq", metadata.getSequenceId() + "");
        } else {
          typedProperties.put("JMSXGroupSeq", "0");
        }
      }
    }
    return selector.matches(
        typedProperties,
        messageId,
        correlationId,
        jmsReplyTo,
        destination,
        deliveryMode,
        jmsType,
        jmsExpiration,
        jmsPriority,
        jmsTimestamp);
  }
}
