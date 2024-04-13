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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.commons.codec.binary.Hex;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class TracingUtils {
  private static final org.slf4j.Logger traceLogger = org.slf4j.LoggerFactory.getLogger("jms-tracing");

  @FunctionalInterface
  public interface Tracer {
    void trace(String message);
  }

  public static final Tracer SLF4J_TRACER = traceLogger::info;

  private static final ObjectMapper mapper =
      new ObjectMapper()
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .enable(SerializationFeature.WRITE_NULL_MAP_VALUES);

  public static final int MAX_DATA_LENGTH = 1024;

  public enum TraceLevel {
    NONE,
    MINIMAL,
    BASIC,
    FULL
  }


  public static void trace(String message, Map<String, Object> traceDetails) {
    trace(SLF4J_TRACER, message, traceDetails);
  }

  public static void trace(Tracer tracer, String message, Map<String, Object> traceDetails) {
    Map<String, Object> trace = new TreeMap<>();
    trace.put("message", message);
    trace.put("traceDetails", traceDetails);

    try {
      String loggableJsonString = mapper.writeValueAsString(trace);
      tracer.trace(loggableJsonString);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Map<String, Object> getConnectionDetails(TraceLevel level, ServerCnx cnx) {
    if (cnx == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateConnectionDetails(level, cnx, details);
    return details;
  }

  private static void populateConnectionDetails(
      TraceLevel level, ServerCnx cnx, Map<String, Object> traceDetails) {
    if (cnx == null) {
      return;
    }

    switch (level) {
      case MINIMAL:
        traceDetails.put("clientAddress", cnx.clientAddress().toString());
        break;
      case BASIC:
        populateConnectionDetails(TraceLevel.MINIMAL, cnx, traceDetails);

        traceDetails.put("clientVersion", cnx.getClientVersion());
        traceDetails.put("clientSourceAddressAndPort", cnx.clientSourceAddressAndPort());
        break;
      case FULL:
        populateConnectionDetails(TraceLevel.MINIMAL, cnx, traceDetails);
        populateConnectionDetails(TraceLevel.BASIC, cnx, traceDetails);

        traceDetails.put("authRole", cnx.getAuthRole());
        traceDetails.put("authMethod", cnx.getAuthMethod());
        traceDetails.put("authMethodName", cnx.getAuthenticationProvider().getAuthMethodName());
        break;
      default:
        log.warn("Unknown tracing level: {}", level);
        break;
    }
  }

  public static Map<String, Object> getSubscriptionDetails(TraceLevel level, Subscription sub) {
    if (sub == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateSubscriptionDetails(level, sub, details);
    return details;
  }

  private static void populateSubscriptionDetails(
      TraceLevel level, Subscription sub, Map<String, Object> traceDetails) {
    if (sub == null) {
      return;
    }

    switch (level) {
      case MINIMAL:
        traceDetails.put("name", sub.getName());
        traceDetails.put("topicName", TopicName.get(sub.getTopicName()).getPartitionedTopicName());
        traceDetails.put("type", sub.getType().name());
        break;
      case BASIC:
        populateSubscriptionDetails(TraceLevel.MINIMAL, sub, traceDetails);

        if (sub.getConsumers() != null) {
          traceDetails.put("numberOfConsumers", sub.getConsumers().size());
          traceDetails.put(
              "namesOfConsumers",
              sub.getConsumers().stream().map(Consumer::consumerName).collect(Collectors.toList()));
        }

        break;
      case FULL:
        populateSubscriptionDetails(TraceLevel.MINIMAL, sub, traceDetails);
        populateSubscriptionDetails(TraceLevel.BASIC, sub, traceDetails);

        traceDetails.put("subscriptionProperties", sub.getSubscriptionProperties());
        break;
      default:
        log.warn("Unknown tracing level: {}", level);
        break;
    }
  }

  public static Map<String, Object> getConsumerDetails(TraceLevel level, Consumer consumer) {
    if (consumer == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateConsumerDetails(level, consumer, details);
    return details;
  }

  private static void populateConsumerDetails(
      TraceLevel level, Consumer consumer, Map<String, Object> traceDetails) {
    if (consumer == null) {
      return;
    }

    switch (level) {
      case MINIMAL:
        traceDetails.put("name", consumer.consumerName());
        traceDetails.put("consumerId", consumer.consumerId());
        if (consumer.getSubscription() != null) {
          traceDetails.put("subscriptionName", consumer.getSubscription().getName());
          traceDetails.put(
              "topicName",
              TopicName.get(consumer.getSubscription().getTopicName()).getPartitionedTopicName());
        }
        break;
      case BASIC:
        populateConsumerDetails(TraceLevel.MINIMAL, consumer, traceDetails);

        traceDetails.put("priorityLevel", consumer.getPriorityLevel());
        traceDetails.put("subType", consumer.subType().name());
        traceDetails.put("clientAddress", consumer.getClientAddress());
        break;
      case FULL:
        populateConsumerDetails(TraceLevel.MINIMAL, consumer, traceDetails);
        populateConsumerDetails(TraceLevel.BASIC, consumer, traceDetails);

        traceDetails.put("metadata", consumer.getMetadata());
        break;
      default:
        log.warn("Unknown tracing level: {}", level);
        break;
    }
  }

  public static Map<String, Object> getProducerDetails(TraceLevel level, Producer producer) {
    if (producer == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateProducerDetails(level, producer, details);
    return details;
  }

  private static void populateProducerDetails(
      TraceLevel level, Producer producer, Map<String, Object> traceDetails) {
    if (producer == null) {
      return;
    }

    switch (level) {
      case MINIMAL:
        traceDetails.put("producerId", producer.getProducerId());
        traceDetails.put("producerName", producer.getProducerName());
        traceDetails.put("accessMode", producer.getAccessMode().name());
        if (producer.getTopic() != null) {
          traceDetails.put(
              "topicName", TopicName.get(producer.getTopic().getName()).getPartitionedTopicName());
        }
        break;
      case BASIC:
        populateProducerDetails(TraceLevel.MINIMAL, producer, traceDetails);

        traceDetails.put("clientAddress", producer.getClientAddress());
        break;
      case FULL:
        populateProducerDetails(TraceLevel.MINIMAL, producer, traceDetails);
        populateProducerDetails(TraceLevel.BASIC, producer, traceDetails);

        traceDetails.put("metadata", producer.getMetadata());
        if (producer.getSchemaVersion() != null) {
          traceDetails.put("schemaVersion", producer.getSchemaVersion().toString());
        }
        traceDetails.put("remoteCluster", producer.getRemoteCluster());
        break;
      default:
        log.warn("Unknown tracing level: {}", level);
        break;
    }
  }

  public static Map<String, Object> getMessageMetadataDetails(
      TraceLevel level, MessageMetadata msgMetadata) {
    if (msgMetadata == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateMessageMetadataDetails(level, msgMetadata, details);
    return details;
  }

  private static void populateMessageMetadataDetails(
      TraceLevel level, MessageMetadata msgMetadata, Map<String, Object> traceDetails) {
    if (msgMetadata == null) {
      return;
    }

    switch (level) {
      case MINIMAL:
        traceDetails.put("sequenceId", msgMetadata.getSequenceId());
        traceDetails.put("producerName", msgMetadata.getProducerName());
        traceDetails.put("partitionKey", msgMetadata.getPartitionKey());
        break;
      case BASIC:
        populateMessageMetadataDetails(TraceLevel.MINIMAL, msgMetadata, traceDetails);

        traceDetails.put("uncompressedSize", msgMetadata.getUncompressedSize());
        traceDetails.put("serializedSize", msgMetadata.getSerializedSize());
        traceDetails.put("numMessagesInBatch", msgMetadata.getNumMessagesInBatch());
        break;
      case FULL:
        populateMessageMetadataDetails(TraceLevel.MINIMAL, msgMetadata, traceDetails);
        populateMessageMetadataDetails(TraceLevel.BASIC, msgMetadata, traceDetails);

        traceDetails.put("publishTime", msgMetadata.getPublishTime());
        traceDetails.put("eventTime", msgMetadata.getEventTime());
        traceDetails.put("replicatedFrom", msgMetadata.getReplicatedFrom());
        traceDetails.put("uuid", msgMetadata.getUuid());
        break;
      default:
        log.warn("Unknown tracing level: {}", level);
        break;
    }
  }

  public static Map<String, Object> getEntryDetails(TraceLevel level, Entry entry) {
    if (entry == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateEntryDetails(level, entry, details);
    return details;
  }

  private static void populateEntryDetails(
      TraceLevel level, Entry entry, Map<String, Object> traceDetails) {
    if (entry == null) {
      return;
    }

    switch (level) {
      case MINIMAL:
        traceDetails.put("ledgerId", entry.getLedgerId());
        traceDetails.put("entryId", entry.getEntryId());
        break;
      case BASIC:
        populateEntryDetails(TraceLevel.MINIMAL, entry, traceDetails);

        traceDetails.put("length", entry.getLength());
        break;
      case FULL:
        populateEntryDetails(TraceLevel.MINIMAL, entry, traceDetails);
        populateEntryDetails(TraceLevel.BASIC, entry, traceDetails);

        traceByteBuf("data", entry.getDataBuffer(), traceDetails);
        break;
      default:
        log.warn("Unknown tracing level: {}", level);
        break;
    }
  }

  public static Map<String, Object> getPublishContextDetails(Topic.PublishContext publishContext) {
    if (publishContext == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populatePublishContext(publishContext, details);
    return details;
  }

  private static void populatePublishContext(
      Topic.PublishContext publishContext, Map<String, Object> traceDetails) {
    traceDetails.put("isMarkerMessage", publishContext.isMarkerMessage());
    traceDetails.put("isChunked", publishContext.isChunked());
    traceDetails.put("numberOfMessages", publishContext.getNumberOfMessages());

    traceDetails.put("entryTimestamp", publishContext.getEntryTimestamp());
    traceDetails.put("msgSize", publishContext.getMsgSize());
    traceDetails.put("producerName", publishContext.getProducerName());
    traceDetails.put("originalProducerName", publishContext.getOriginalProducerName());
    traceDetails.put("originalSequenceId", publishContext.getOriginalSequenceId());
    traceDetails.put("sequenceId", publishContext.getSequenceId());
  }

  public static void traceByteBuf(String key, ByteBuf buf, Map<String, Object> traceDetails) {
    if (buf == null) return;

    if (buf.readableBytes() < MAX_DATA_LENGTH) {
      traceDetails.put(key, "0x" + Hex.encodeHexString(buf.nioBuffer()));
    } else {
      traceDetails.put(key + "Slice", "0x" + Hex.encodeHexString(buf.slice(0, MAX_DATA_LENGTH).nioBuffer()));
    }
  }
}
