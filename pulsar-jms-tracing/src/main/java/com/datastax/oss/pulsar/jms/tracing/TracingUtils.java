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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.commons.codec.binary.Hex;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;

@Slf4j
public class TracingUtils {

  public enum EventReasons {
    ADMINISTRATIVE,
    COMMANDS,
    MESSAGE,
    TRANSACTION,
    SERVLET,
  }

  @FunctionalInterface
  public interface Tracer {
    void trace(EventReasons reason, String message);
  }

  public static class Slf4jTracer implements Tracer {
    private static final Map<EventReasons, org.slf4j.Logger> traceLoggers = new HashMap<>();

    static {
      for (EventReasons reason : EventReasons.values()) {
        traceLoggers.put(
            reason,
            org.slf4j.LoggerFactory.getLogger("jms-tracing-" + reason.name().toLowerCase()));
      }
    }

    @Override
    public void trace(EventReasons reason, String message) {
      traceLoggers.get(reason).info(message);
    }
  }

  public static final Tracer SLF4J_TRACER = new Slf4jTracer();

  private static final ObjectMapper mapper =
      new ObjectMapper()
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .enable(SerializationFeature.WRITE_NULL_MAP_VALUES);

  public enum TraceLevel {
    OFF,
    ON
  }

  private static final LoadingCache<String, String> ipResolverCache =
      CacheBuilder.newBuilder()
          .maximumSize(10_000L)
          .concurrencyLevel(Runtime.getRuntime().availableProcessors())
          .build(
              new CacheLoader<String, String>() {
                public String load(String clientAddress) {
                  // Dn resolution can be slow in some cases
                  // and we do not want to create too many requests to DNS,
                  // so we cache the result
                  log.info("resolving DNS for {}", clientAddress);
                  try {
                    InetAddress address = InetAddress.getByName(clientAddress);
                    String hostName = address.getCanonicalHostName();
                    if (log.isDebugEnabled()) {
                      log.debug("Resolved DNS for {} to {}", clientAddress, hostName);
                    }
                    return hostName;
                  } catch (UnknownHostException e) {
                    log.error("Failed to resolve DNS for {}", clientAddress, e);
                    return clientAddress;
                  }
                }
              });

  public static String hostNameOf(String clientAddress) {
    if (clientAddress == null || clientAddress.isEmpty()) {
      return "unknown/null";
    }

    try {
      return ipResolverCache.get(clientAddress);
    } catch (Throwable t) {
      log.error("Failed to resolve DNS for {}", clientAddress, t);
      return clientAddress;
    }
  }

  public static void trace(EventReasons reason, String message, Map<String, Object> traceDetails) {
    trace(SLF4J_TRACER, reason, message, traceDetails);
  }

  public static void trace(
      Tracer tracer, EventReasons reason, String message, Map<String, Object> traceDetails) {
    Map<String, Object> trace = new TreeMap<>();
    trace.put("eventType", message);
    trace.put("traceDetails", traceDetails);

    try {
      String loggableJsonString = mapper.writeValueAsString(trace);
      tracer.trace(reason, loggableJsonString);
    } catch (JsonProcessingException e) {
      log.error(
          "Failed to serialize trace event type '{}' as json, traceDetails: {}",
          message,
          traceDetails,
          e);
    }
  }

  public static Map<String, Object> getCommandDetails(BaseCommand command) {
    if (command == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateCommandDetails(command, details);
    return details;
  }

  private static void populateCommandDetails(
      BaseCommand command, Map<String, Object> traceDetails) {
    if (command == null) {
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

  private static final Set<String> skipTraceFields =
      Sets.newHashSet(
          "authdata",
          "authmethod",
          "authmethodname",
          "originalauthdata",
          "orginalauthmethod",
          "originalprincipal",
          "schema");

  private static void populateByReflection(Object command, Map<String, Object> traceDetails) {
    if (command == null) {
      return;
    }
    if (!command.getClass().getCanonicalName().contains("org.apache.pulsar.common.api.proto")) {
      return;
    }

    Method[] allMethods = command.getClass().getMethods();

    Arrays.stream(allMethods)
        .filter(
            method -> {
              if (!method.getName().startsWith("has")) {
                return false;
              }
              String fieldName = method.getName().substring(3);
              return !skipTraceFields.contains(fieldName.toLowerCase());
            })
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
                  final int size;
                  if (value instanceof byte[]) {
                    size = ((byte[]) value).length;
                  } else if (value instanceof ByteBuf) {
                    size = ((ByteBuf) value).readableBytes();
                  } else {
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

  public static Map<String, Object> getConnectionDetails(ServerCnx cnx) {
    if (cnx == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateConnectionDetails(cnx, details);
    return details;
  }

  private static void populateConnectionDetails(ServerCnx cnx, Map<String, Object> traceDetails) {
    if (cnx == null) {
      return;
    }

    traceDetails.put("clientAddress", hostNameOf(cnx.clientSourceAddress()));
    traceDetails.put("clientSocket", cnx.clientAddress());
    traceDetails.put("authRole", cnx.getAuthRole());
    traceDetails.put("clientVersion", cnx.getClientVersion());
    traceDetails.put("clientSourceAddressAndPort", cnx.clientSourceAddressAndPort());
    traceDetails.put("authMethod", cnx.getAuthMethod());
    traceDetails.put(
        "authMethodName",
        cnx.getAuthenticationProvider() == null
            ? "no provider"
            : cnx.getAuthenticationProvider().getAuthMethodName());

    AuthenticationDataSource authData = cnx.getAuthenticationData();
    if (authData != null) {
      traceDetails.put("authData", getAuthDataDetails(authData));
    }
  }

  private static Object getAuthDataDetails(AuthenticationDataSource authData) {
    if (authData == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateAuthDataDetails(authData, details);
    return details;
  }

  private static void populateAuthDataDetails(
      AuthenticationDataSource authData, Map<String, Object> details) {
    if (authData == null) {
      return;
    }

    details.put("peerAddress", authData.getPeerAddress());
    details.put("commandData", authData.getCommandData());
    details.put("httpAuthType", authData.getHttpAuthType());
    details.put("subscription", authData.getSubscription());
    if (authData.getTlsCertificates() != null) {
      details.put(
          "tlsCertificates",
          Arrays.stream(authData.getTlsCertificates())
              .map(Object::toString)
              .collect(Collectors.toList()));
    }
  }

  public static Map<String, Object> getSubscriptionDetails(Subscription sub) {
    if (sub == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateSubscriptionDetails(sub, details);
    return details;
  }

  private static void populateSubscriptionDetails(
      Subscription sub, Map<String, Object> traceDetails) {
    if (sub == null) {
      return;
    }

    traceDetails.put("name", sub.getName());
    traceDetails.put("topicName", TopicName.get(sub.getTopicName()).getPartitionedTopicName());
    traceDetails.put("type", sub.getType().name());

    if (sub.getConsumers() != null) {
      traceDetails.put("numberOfConsumers", sub.getConsumers().size());
      traceDetails.put(
          "namesOfConsumers",
          sub.getConsumers().stream().map(Consumer::consumerName).collect(Collectors.toList()));
    }

    traceDetails.put("subscriptionProperties", sub.getSubscriptionProperties());
  }

  public static Map<String, Object> getConsumerDetails(Consumer consumer) {
    if (consumer == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateConsumerDetails(consumer, details);
    return details;
  }

  private static void populateConsumerDetails(Consumer consumer, Map<String, Object> traceDetails) {
    if (consumer == null) {
      return;
    }

    traceDetails.put("name", consumer.consumerName());
    traceDetails.put("consumerId", consumer.consumerId());
    Subscription sub = consumer.getSubscription();
    if (sub != null) {
      traceDetails.put("subscriptionName", sub.getName());
      traceDetails.put("topicName", TopicName.get(sub.getTopicName()).getPartitionedTopicName());
    }

    traceDetails.put("priorityLevel", consumer.getPriorityLevel());
    traceDetails.put("subType", consumer.subType() == null ? null : consumer.subType().name());
    traceDetails.put("clientAddress", hostNameOf(consumer.getClientAddress()));

    traceDetails.put("metadata", consumer.getMetadata());
  }

  public static Map<String, Object> getProducerDetails(Producer producer, boolean traceSchema) {
    if (producer == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateProducerDetails(producer, details, traceSchema);
    return details;
  }

  private static void populateProducerDetails(
      Producer producer, Map<String, Object> traceDetails, boolean traceSchema) {
    if (producer == null) {
      return;
    }

    traceDetails.put("producerId", producer.getProducerId());
    traceDetails.put("producerName", producer.getProducerName());
    traceDetails.put(
        "accessMode", producer.getAccessMode() == null ? null : producer.getAccessMode().name());
    if (producer.getTopic() != null) {
      traceDetails.put(
          "topicName", TopicName.get(producer.getTopic().getName()).getPartitionedTopicName());
    }

    traceDetails.put("clientAddress", hostNameOf(producer.getClientAddress()));

    traceDetails.put("metadata", producer.getMetadata());

    if (traceSchema && producer.getSchemaVersion() != null) {
      final String schemaVersion;
      if (producer.getSchemaVersion() == SchemaVersion.Empty) {
        schemaVersion = "Empty";
      } else if (producer.getSchemaVersion() == SchemaVersion.Latest) {
        schemaVersion = "Latest";
      } else {
        schemaVersion = "0x" + Hex.encodeHexString(producer.getSchemaVersion().bytes());
      }
      traceDetails.put("schemaVersion", schemaVersion);
    }
    traceDetails.put("remoteCluster", producer.getRemoteCluster());
  }

  public static Map<String, Object> getMessageMetadataDetails(MessageMetadata msgMetadata) {
    if (msgMetadata == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateMessageMetadataDetails(msgMetadata, details);
    return details;
  }

  private static void populateMessageMetadataDetails(
      MessageMetadata msgMetadata, Map<String, Object> traceDetails) {
    if (msgMetadata == null) {
      return;
    }

    if (msgMetadata.hasPartitionKey()) {
      traceDetails.put("partitionKey", msgMetadata.getPartitionKey());
    }
    if (msgMetadata.hasSequenceId()) {
      traceDetails.put("sequenceId", msgMetadata.getSequenceId());
    }
    if (msgMetadata.hasProducerName()) {
      traceDetails.put("producerName", msgMetadata.getProducerName());
    }

    if (msgMetadata.hasUncompressedSize()) {
      traceDetails.put("uncompressedSize", msgMetadata.getUncompressedSize());
    }
    if (msgMetadata.hasNumMessagesInBatch()) {
      traceDetails.put("numMessagesInBatch", msgMetadata.getNumMessagesInBatch());
    }
    traceDetails.put("serializedSize", msgMetadata.getSerializedSize());

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
  }

  public static Map<String, Object> getEntryDetails(Entry entry, int maxBinaryDataLength) {
    if (entry == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateEntryDetails(entry, details, maxBinaryDataLength);
    return details;
  }

  private static void populateEntryDetails(
      Entry entry, Map<String, Object> traceDetails, int maxBinaryDataLength) {
    if (entry == null) {
      return;
    }

    traceDetails.put("messageId", entry.getLedgerId() + ":" + entry.getEntryId());

    traceDetails.put("length", entry.getLength());

    traceByteBuf("data", entry.getDataBuffer(), traceDetails, maxBinaryDataLength);
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

  public static void traceByteBuf(
      String key, ByteBuf buf, Map<String, Object> traceDetails, int maxBinaryDataLength) {
    if (buf == null || maxBinaryDataLength <= 0) return;

    if (buf.readableBytes() < maxBinaryDataLength) {
      traceDetails.put(key, "0x" + Hex.encodeHexString(buf.nioBuffer()));
    } else {
      traceDetails.put(
          key + "Slice", "0x" + Hex.encodeHexString(buf.slice(0, maxBinaryDataLength).nioBuffer()));
    }
  }
}
