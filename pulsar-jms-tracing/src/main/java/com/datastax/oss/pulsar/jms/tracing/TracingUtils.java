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
import io.netty.buffer.ByteBuf;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
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
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;

@Slf4j
public class TracingUtils {

  public enum EventCategory {
    CONN, // connection creation, closure,
    PROD, // producer creation, closure,
    CONS, // consumer creation, closure,
    TX, // (transaction creation, commit, rollback,etc),
    MSG, // (message level send,dispatch,ack,expire,acktimeout, negative ack, etc),
    REST, // (rest api calls),
  }

  public enum EventSubCategory {
    CREATED,
    CLOSED,

    PRODUCED,
    STORED,

    READ,
    DISPATCHED,
    ACKED,
    FILTERED,

    OPENED,
    COMMITTED,
    ABORTED,

    CALLED,
  }

  @FunctionalInterface
  public interface Tracer {
    void trace(EventCategory category, String message);
  }

  public static class Slf4jTracer implements Tracer {
    private static final Map<EventCategory, org.slf4j.Logger> traceLoggers = new HashMap<>();

    static {
      for (EventCategory category : EventCategory.values()) {
        traceLoggers.put(
            category,
            org.slf4j.LoggerFactory.getLogger("jms-tracing-" + category.name().toLowerCase()));
      }
    }

    @Override
    public void trace(EventCategory category, String message) {
      traceLoggers.get(category).info(message);
    }
  }

  public static final Tracer SLF4J_TRACER = new Slf4jTracer();

  private static final ObjectMapper mapper =
      new ObjectMapper()
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .enable(SerializationFeature.WRITE_NULL_MAP_VALUES);

  public enum TraceLevel {
    OFF, // disabled
    ON, // enabled without payload tracing
    PAYLOAD, // enabled with payload tracing
  }

  private static final LoadingCache<String, String> ipResolverCache =
      CacheBuilder.newBuilder()
          .maximumSize(10_000L)
          .expireAfterWrite(4, TimeUnit.HOURS)
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

  public static String hostNameOf(String clientAddress, String clientSourceAddressAndPort) {
    if (clientAddress == null || clientAddress.isEmpty()
            || clientSourceAddressAndPort == null || !clientSourceAddressAndPort.contains(":")) {
      return "unknown/null";
    }

    try {
      String port = clientSourceAddressAndPort.split(":")[1];
      return ipResolverCache.get(clientAddress) + ":" + port;
    } catch (Throwable t) {
      log.error("Failed to resolve DNS for {}", clientAddress, t);
      return clientAddress;
    }
  }

  public static String hostNameOf(String clientAddress, int remotePort) {
    if (clientAddress == null || clientAddress.isEmpty()) {
      return "unknown/null";
    }

    try {
      return ipResolverCache.get(clientAddress) + ":" + remotePort;
    } catch (Throwable t) {
      log.error("Failed to resolve DNS for {}", clientAddress, t);
      return clientAddress;
    }
  }

  public static void trace(
      EventCategory category, EventSubCategory subCategory, Map<String, Object> traceDetails) {
    trace(SLF4J_TRACER, category, subCategory, traceDetails);
  }

  public static void trace(
      Tracer tracer,
      EventCategory category,
      EventSubCategory subCategory,
      Map<String, Object> traceDetails) {
    Map<String, Object> trace = new TreeMap<>();
    trace.put("event", category + "_" + subCategory);
    trace.put("traceDetails", traceDetails);

    try {
      String loggableJsonString = mapper.writeValueAsString(trace);
      tracer.trace(category, loggableJsonString);
    } catch (JsonProcessingException e) {
      log.error(
          "Failed to serialize trace event '{}_{}' as json, traceDetails: {}",
          category,
          subCategory,
          traceDetails,
          e);
    }
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
    traceDetails.put("clientHost", hostNameOf(cnx.clientSourceAddress(), cnx.clientSourceAddressAndPort()));
    traceDetails.put("authRole", cnx.getAuthRole());
    traceDetails.put("clientVersion", cnx.getClientVersion());
    traceDetails.put("clientSourceAddressAndPort", cnx.clientSourceAddressAndPort());
    traceDetails.put("authMethod", cnx.getAuthMethod());
    if (cnx.getAuthenticationProvider() != null) {
      traceDetails.put("authMethodName", cnx.getAuthenticationProvider().getAuthMethodName());
    }

    traceDetails.put("state", cnx.getState());

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
    }
    traceDetails.put("isReplicated", sub.isReplicated());
    traceDetails.put("numberOfEntriesDelayed", sub.getNumberOfEntriesDelayed());
    traceDetails.put("numberOfEntriesInBacklog", sub.getNumberOfEntriesInBacklog(false));

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
    traceDetails.put("clientHost", hostNameOf(consumer.getClientAddress(), consumer.cnx().clientSourceAddressAndPort()));

    traceDetails.put("metadata", consumer.getMetadata());
    traceDetails.put("unackedMessages", consumer.getUnackedMessages());
    traceDetails.put("authRole", consumer.cnx().getAuthRole());
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

    traceDetails.put("clientHost", hostNameOf(producer.getClientAddress(), producer.getCnx().clientSourceAddressAndPort()));

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

    traceDetails.put("authRole", producer.getCnx().getAuthRole());
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

  public static Map<String, Object> getEntryDetails(
      TraceLevel level, Entry entry, int maxBinaryDataLength) {
    if (entry == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populateEntryDetails(level, entry, details, maxBinaryDataLength);
    return details;
  }

  private static void populateEntryDetails(
      TraceLevel level, Entry entry, Map<String, Object> traceDetails, int maxBinaryDataLength) {
    if (entry == null) {
      return;
    }

    traceDetails.put("messageId", entry.getLedgerId() + ":" + entry.getEntryId());

    traceDetails.put("length", entry.getLength());

    if (TraceLevel.PAYLOAD == level && entry.getDataBuffer() != null) {
      traceMetadataAndPayload(
          "payload", entry.getDataBuffer().slice(), traceDetails, maxBinaryDataLength);
    }
  }

  public static Map<String, Object> getPublishContextDetails(TraceLevel level, Topic.PublishContext publishContext) {
    if (publishContext == null) {
      return null;
    }

    Map<String, Object> details = new TreeMap<>();
    populatePublishContext(level, publishContext, details);
    return details;
  }

  private static void populatePublishContext(TraceLevel level,
      Topic.PublishContext publishContext, Map<String, Object> traceDetails) {
    traceDetails.put("sequenceId", publishContext.getSequenceId());
    traceDetails.put("entryTimestamp", publishContext.getEntryTimestamp());
    traceDetails.put("msgSize", publishContext.getMsgSize());

    if (TraceLevel.PAYLOAD == level) {
      traceDetails.put("numberOfMessages", publishContext.getNumberOfMessages());
      traceDetails.put("isMarkerMessage", publishContext.isMarkerMessage());
      traceDetails.put("isChunked", publishContext.isChunked());
      if (publishContext.getOriginalProducerName() != null) {
        traceDetails.put("originalProducerName", publishContext.getOriginalProducerName());
        traceDetails.put("originalSequenceId", publishContext.getOriginalSequenceId());
      }
    }
  }

  /** this will release metadataAndPayload */
  public static void traceMetadataAndPayload(
      String key,
      ByteBuf metadataAndPayload,
      Map<String, Object> traceDetails,
      int maxPayloadLength) {
    if (metadataAndPayload == null) return;
    if (maxPayloadLength <= 0) {
      metadataAndPayload.release();
      return;
    }
    try {
      // advance readerIndex
      MessageMetadata metadata = Commands.parseMessageMetadata(metadataAndPayload);

      // todo: do we need to trace this metadata?
      populateMessageMetadataDetails(metadata, traceDetails);

      // Decode if needed
      CompressionCodec codec =
          CompressionCodecProvider.getCompressionCodec(metadata.getCompression());
      ByteBuf uncompressedPayload =
          codec.decode(metadataAndPayload, metadata.getUncompressedSize());
      traceByteBuf(key, uncompressedPayload, traceDetails, maxPayloadLength);
    } catch (Throwable t) {
      log.error("Failed to trace metadataAndPayload", t);
    } finally {
      metadataAndPayload.release();
    }
  }

  /** this will release payload */
  public static void traceByteBuf(
      String key, ByteBuf payload, Map<String, Object> traceDetails, int maxPayloadLength) {
    if (payload == null) return;

    if (maxPayloadLength <= 0) {
      payload.release();
      return;
    }

    try {
      // todo: does this require additional steps if messages are batched?
      String dataAsString = payload.toString(StandardCharsets.UTF_8);
      if (dataAsString.length() > maxPayloadLength + 3) {
        dataAsString = dataAsString.substring(0, maxPayloadLength) + "...";
      }
      traceDetails.put(key, dataAsString);
    } catch (Throwable t) {
      log.error("Failed to convert ByteBuf to string", t);
      if (payload.readableBytes() < maxPayloadLength) {
        traceDetails.put(key, "0x" + Hex.encodeHexString(payload.nioBuffer()));
      } else {
        ByteBuf buf = payload.slice(0, maxPayloadLength / 2);
        traceDetails.put(key, "0x" + Hex.encodeHexString(buf.nioBuffer()) + "...");
        buf.release();
      }
    } finally {
      payload.release();
    }
  }
}
