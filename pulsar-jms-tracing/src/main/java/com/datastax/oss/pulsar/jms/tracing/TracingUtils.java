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
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
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
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class TracingUtils {
  private static final org.slf4j.Logger traceLogger =
      org.slf4j.LoggerFactory.getLogger("jms-tracing");

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
    trace.put("eventType", message);
    trace.put("traceDetails", traceDetails);

    try {
      String loggableJsonString = mapper.writeValueAsString(trace);
      tracer.trace(loggableJsonString);
    } catch (JsonProcessingException e) {
      log.error(
          "Failed to serialize trace event type '{}' as json, traceDetails: {}",
          message,
          traceDetails,
          e);
    }
  }

  public static Map<String, Object> getCommandDetails(TraceLevel level, BaseCommand command) {
    if (command == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateCommandDetails(level, command, details);
    return details;
  }

  private static void populateCommandDetails(
      TraceLevel level, BaseCommand command, Map<String, Object> traceDetails) {
    if (command == null) {
      return;
    }
    if (level == TraceLevel.NONE) {
      return;
    }

    if (!command.hasType()) {
      return;
    }

    // trace all params otherwise

    switch (command.getType()) {
      case CONNECT:
        populateByReflection(command.getConnect(), traceDetails);
        break;
      case CONNECTED:
        populateByReflection(command.getConnected(), traceDetails);
        break;
      case SUBSCRIBE:
        populateByReflection(command.getSubscribe(), traceDetails);
        break;
      case PRODUCER:
        populateByReflection(command.getProducer(), traceDetails);
        break;
      case SEND:
        populateByReflection(command.getSend(), traceDetails);
        break;
      case SEND_RECEIPT:
        populateByReflection(command.getSendReceipt(), traceDetails);
        break;
      case SEND_ERROR:
        populateByReflection(command.getSendError(), traceDetails);
        break;
      case MESSAGE:
        populateByReflection(command.getMessage(), traceDetails);
        break;
      case ACK:
        populateByReflection(command.getAck(), traceDetails);
        break;
      case FLOW:
        populateByReflection(command.getFlow(), traceDetails);
        break;
      case UNSUBSCRIBE:
        populateByReflection(command.getUnsubscribe(), traceDetails);
        break;
      case SUCCESS:
        populateByReflection(command.getSuccess(), traceDetails);
        break;
      case ERROR:
        populateByReflection(command.getError(), traceDetails);
        break;
      case CLOSE_PRODUCER:
        populateByReflection(command.getCloseProducer(), traceDetails);
        break;
      case CLOSE_CONSUMER:
        populateByReflection(command.getCloseConsumer(), traceDetails);
        break;
      case PRODUCER_SUCCESS:
        populateByReflection(command.getProducerSuccess(), traceDetails);
        break;
      case PING:
        populateByReflection(command.getPing(), traceDetails);
        break;
      case PONG:
        populateByReflection(command.getPong(), traceDetails);
        break;
      case REDELIVER_UNACKNOWLEDGED_MESSAGES:
        populateByReflection(command.getRedeliverUnacknowledgedMessages(), traceDetails);
        break;
      case PARTITIONED_METADATA:
        populateByReflection(command.getPartitionMetadata(), traceDetails);
        break;
      case PARTITIONED_METADATA_RESPONSE:
        populateByReflection(command.getPartitionMetadataResponse(), traceDetails);
        break;
      case LOOKUP:
        populateByReflection(command.getLookupTopic(), traceDetails);
        break;
      case LOOKUP_RESPONSE:
        populateByReflection(command.getLookupTopicResponse(), traceDetails);
        break;
      case CONSUMER_STATS:
        populateByReflection(command.getConsumerStats(), traceDetails);
        break;
      case CONSUMER_STATS_RESPONSE:
        populateByReflection(command.getConsumerStatsResponse(), traceDetails);
        break;
      case REACHED_END_OF_TOPIC:
        populateByReflection(command.getReachedEndOfTopic(), traceDetails);
        break;
      case SEEK:
        populateByReflection(command.getSeek(), traceDetails);
        break;
      case GET_LAST_MESSAGE_ID:
        populateByReflection(command.getGetLastMessageId(), traceDetails);
        break;
      case GET_LAST_MESSAGE_ID_RESPONSE:
        populateByReflection(command.getGetLastMessageIdResponse(), traceDetails);
        break;
      case ACTIVE_CONSUMER_CHANGE:
        populateByReflection(command.getActiveConsumerChange(), traceDetails);
        break;
      case GET_TOPICS_OF_NAMESPACE:
        populateByReflection(command.getGetTopicsOfNamespace(), traceDetails);
        break;
      case GET_TOPICS_OF_NAMESPACE_RESPONSE:
        populateByReflection(command.getGetTopicsOfNamespaceResponse(), traceDetails);
        break;
      case GET_SCHEMA:
        populateByReflection(command.getGetSchema(), traceDetails);
        break;
      case GET_SCHEMA_RESPONSE:
        populateByReflection(command.getGetSchemaResponse(), traceDetails);
        break;
      case AUTH_CHALLENGE:
        populateByReflection(command.getAuthChallenge(), traceDetails);
        break;
      case AUTH_RESPONSE:
        populateByReflection(command.getAuthResponse(), traceDetails);
        break;
      case ACK_RESPONSE:
        populateByReflection(command.getAckResponse(), traceDetails);
        break;
      case GET_OR_CREATE_SCHEMA:
        populateByReflection(command.getGetOrCreateSchema(), traceDetails);
        break;
      case GET_OR_CREATE_SCHEMA_RESPONSE:
        populateByReflection(command.getGetOrCreateSchemaResponse(), traceDetails);
        break;
      case NEW_TXN:
        populateByReflection(command.getNewTxn(), traceDetails);
        break;
      case NEW_TXN_RESPONSE:
        populateByReflection(command.getNewTxnResponse(), traceDetails);
        break;
      case ADD_PARTITION_TO_TXN:
        populateByReflection(command.getAddPartitionToTxn(), traceDetails);
        break;
      case ADD_PARTITION_TO_TXN_RESPONSE:
        populateByReflection(command.getAddPartitionToTxnResponse(), traceDetails);
        break;
      case ADD_SUBSCRIPTION_TO_TXN:
        populateByReflection(command.getAddSubscriptionToTxn(), traceDetails);
        break;
      case ADD_SUBSCRIPTION_TO_TXN_RESPONSE:
        populateByReflection(command.getAddSubscriptionToTxnResponse(), traceDetails);
        break;
      case END_TXN:
        populateByReflection(command.getEndTxn(), traceDetails);
        break;
      case END_TXN_RESPONSE:
        populateByReflection(command.getEndTxnResponse(), traceDetails);
        break;
      case END_TXN_ON_PARTITION:
        populateByReflection(command.getEndTxnOnPartition(), traceDetails);
        break;
      case END_TXN_ON_PARTITION_RESPONSE:
        populateByReflection(command.getEndTxnOnPartitionResponse(), traceDetails);
        break;
      case END_TXN_ON_SUBSCRIPTION:
        populateByReflection(command.getEndTxnOnSubscription(), traceDetails);
        break;
      case END_TXN_ON_SUBSCRIPTION_RESPONSE:
        populateByReflection(command.getEndTxnOnSubscriptionResponse(), traceDetails);
        break;
      case TC_CLIENT_CONNECT_REQUEST:
        populateByReflection(command.getTcClientConnectRequest(), traceDetails);
        break;
      case TC_CLIENT_CONNECT_RESPONSE:
        populateByReflection(command.getTcClientConnectResponse(), traceDetails);
        break;
      case WATCH_TOPIC_LIST:
        populateByReflection(command.getWatchTopicList(), traceDetails);
        break;
      case WATCH_TOPIC_LIST_SUCCESS:
        populateByReflection(command.getWatchTopicListSuccess(), traceDetails);
        break;
      case WATCH_TOPIC_UPDATE:
        populateByReflection(command.getWatchTopicUpdate(), traceDetails);
        break;
      case WATCH_TOPIC_LIST_CLOSE:
        populateByReflection(command.getWatchTopicListClose(), traceDetails);
        break;
      case TOPIC_MIGRATED:
        populateByReflection(command.getTopicMigrated(), traceDetails);
        break;
      default:
        log.error("Unknown command type: {}", command.getType());
        traceDetails.put("error", "unknownCommandType " + command.getType());
    }
  }

  private static void populateByReflection(Object command, Map<String, Object> traceDetails) {
    if (command == null) {
      return;
    }
    if (!command.getClass().getCanonicalName().contains("org.apache.pulsar.common.api.proto")) {
      return;
    }

    Method[] allMethods = command.getClass().getMethods();

    Arrays.stream(allMethods)
        .filter(method -> method.getName().startsWith("has"))
        .filter(
            method -> {
              try {
                return (boolean) method.invoke(command);
              } catch (Exception e) {
                return false;
              }
            })
        .forEach(
            method -> {
              String fieldName = method.getName().substring(3);
              try {
                Optional<Method> accessor =
                    Arrays.stream(allMethods)
                        .filter(
                            m ->
                                m.getName().equals("get" + fieldName)
                                    || m.getName().equals("is" + fieldName))
                        .findFirst();
                if (!accessor.isPresent()) {
                  log.warn(
                      "No accessor found for field (but has.. counterpart was found): {} of {}",
                      fieldName,
                      command.getClass().getCanonicalName());
                  return;
                }
                Object value = accessor.get().invoke(command);

                if (value == null) return;

                // skip logging of binary data
                if (value instanceof byte[]
                    || value instanceof ByteBuf
                    || value instanceof ByteBuffer) {
                  int size = 0;
                  if (value instanceof byte[]) {
                    size = ((byte[]) value).length;
                  } else if (value instanceof ByteBuf) {
                    size = ((ByteBuf) value).readableBytes();
                  } else if (value instanceof ByteBuffer) {
                    size = ((ByteBuffer) value).remaining();
                  }
                  traceDetails.put(fieldName + "_size", size);
                  return;
                }

                if (value
                    .getClass()
                    .getCanonicalName()
                    .contains("org.apache.pulsar.common.api.proto")) {
                  Map<String, Object> details = new TreeMap<>();
                  populateByReflection(value, details);
                  traceDetails.put(fieldName, details);
                } else {
                  traceDetails.put(fieldName, value);
                }
              } catch (Exception e) {
                log.error("Failed to access field: {}", fieldName, e);
              }
            });
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
        populateConnectionDetails(TraceLevel.BASIC, cnx, traceDetails);

        traceDetails.put("authRole", cnx.getAuthRole());
        traceDetails.put("authMethod", cnx.getAuthMethod());
        traceDetails.put(
            "authMethodName",
            cnx.getAuthenticationProvider() == null
                ? "no provider"
                : cnx.getAuthenticationProvider().getAuthMethodName());
        break;
      case NONE:
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
        populateSubscriptionDetails(TraceLevel.BASIC, sub, traceDetails);

        traceDetails.put("subscriptionProperties", sub.getSubscriptionProperties());
        break;
      case NONE:
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
        Subscription sub = consumer.getSubscription();
        if (sub != null) {
          traceDetails.put("subscriptionName", sub.getName());
          traceDetails.put(
              "topicName", TopicName.get(sub.getTopicName()).getPartitionedTopicName());
        }
        break;
      case BASIC:
        populateConsumerDetails(TraceLevel.MINIMAL, consumer, traceDetails);

        traceDetails.put("priorityLevel", consumer.getPriorityLevel());
        traceDetails.put("subType", consumer.subType() == null ? null : consumer.subType().name());
        traceDetails.put("clientAddress", consumer.getClientAddress());
        break;
      case FULL:
        populateConsumerDetails(TraceLevel.BASIC, consumer, traceDetails);

        traceDetails.put("metadata", consumer.getMetadata());
        break;
      case NONE:
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
        traceDetails.put(
            "accessMode",
            producer.getAccessMode() == null ? null : producer.getAccessMode().name());
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
        populateProducerDetails(TraceLevel.BASIC, producer, traceDetails);

        traceDetails.put("metadata", producer.getMetadata());
        if (producer.getSchemaVersion() != null) {
          traceDetails.put("schemaVersion", producer.getSchemaVersion().toString());
        }
        traceDetails.put("remoteCluster", producer.getRemoteCluster());
        break;
      case NONE:
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
        if (msgMetadata.hasPartitionKey()) {
          traceDetails.put("partitionKey", msgMetadata.getPartitionKey());
        }
        if (msgMetadata.hasSequenceId()) {
          traceDetails.put("sequenceId", msgMetadata.getSequenceId());
        }
        if (msgMetadata.hasProducerName()) {
          traceDetails.put("producerName", msgMetadata.getProducerName());
        }
        break;
      case BASIC:
        populateMessageMetadataDetails(TraceLevel.MINIMAL, msgMetadata, traceDetails);

        if (msgMetadata.hasUncompressedSize()) {
          traceDetails.put("uncompressedSize", msgMetadata.getUncompressedSize());
        }
        if (msgMetadata.hasNumMessagesInBatch()) {
          traceDetails.put("numMessagesInBatch", msgMetadata.getNumMessagesInBatch());
        }
        traceDetails.put("serializedSize", msgMetadata.getSerializedSize());
        break;
      case FULL:
        populateMessageMetadataDetails(TraceLevel.BASIC, msgMetadata, traceDetails);

        if (msgMetadata.hasPublishTime()) {
          traceDetails.put("publishTime", msgMetadata.getPublishTime());
        }
        if (msgMetadata.hasEventTime()) {
          traceDetails.put("eventTime", msgMetadata.getEventTime());
        }
        if (msgMetadata.hasReplicatedFrom()) {
          traceDetails.put("replicatedFrom", msgMetadata.getReplicatedFrom());
        }
        if (msgMetadata.hasUuid()) {
          traceDetails.put("uuid", msgMetadata.getUuid());
        }
        break;
      case NONE:
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
        populateEntryDetails(TraceLevel.BASIC, entry, traceDetails);

        traceByteBuf("data", entry.getDataBuffer(), traceDetails);
        break;
      case NONE:
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
      traceDetails.put(
          key + "Slice", "0x" + Hex.encodeHexString(buf.slice(0, MAX_DATA_LENGTH).nioBuffer()));
    }
  }
}
