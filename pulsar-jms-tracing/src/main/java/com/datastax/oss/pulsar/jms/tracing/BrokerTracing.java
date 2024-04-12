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
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.commons.codec.binary.Hex;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.intercept.InterceptException;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class BrokerTracing implements BrokerInterceptor {

  private static final org.slf4j.Logger tracer = org.slf4j.LoggerFactory.getLogger("jms-tracing");

  private static final ObjectMapper mapper =
      new ObjectMapper()
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .enable(SerializationFeature.WRITE_NULL_MAP_VALUES);

  static final int MAX_DATA_LENGTH = 1024;

  public enum EventReasons {
    ADMINISTRATIVE,
    MESSAGE,
    TRANSACTION,
    SERVLET,
  }

  public enum TraceLevel {
    NONE,
    MINIMAL,
    BASIC,
    FULL
  }

  //  private final Set<EventReasons> defaultEnabledEvents = new HashSet<>();
  private static final TraceLevel defaultTraceLevel = TraceLevel.BASIC;

  public static void trace(String message, Map<String, Object> traceDetails) {
    Map<String, Object> trace = new TreeMap<>();
    trace.put("message", message);
    trace.put("details", traceDetails);

    try {
      String loggableJsonString = mapper.writeValueAsString(trace);
      tracer.info(loggableJsonString);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public void initialize(PulsarService pulsarService) {
    //    defaultEnabledEvents.add(EventReasons.ADMINISTRATIVE);
    //    defaultEnabledEvents.add(EventReasons.MESSAGE);
    //    defaultEnabledEvents.add(EventReasons.TRANSACTION);
  }

  @Override
  public void close() {}

  private Set<EventReasons> getEnabledEvents(ServerCnx cnx) {
    return getEnabledEvents(cnx.getBrokerService().getPulsar());
  }

  private Set<EventReasons> getEnabledEvents(PulsarService pulsar) {
    // todo: do this less frequently

    String events =
        pulsar.getConfiguration().getProperties().getProperty("jmsTracingEventList", "");

    Set<EventReasons> enabledEvents = new HashSet<>();

    for (String event : events.split(",")) {
      try {
        enabledEvents.add(EventReasons.valueOf(event.trim()));
      } catch (IllegalArgumentException e) {
        log.warn("Invalid event: {}. Skipping", event);
      }
    }

    return enabledEvents;
  }

  private static TraceLevel getTracingLevel(ServerCnx cnx) {
    // todo: do this less frequently
    String level =
        cnx.getBrokerService()
            .getPulsar()
            .getConfiguration()
            .getProperties()
            .getProperty("jmsTracingLevel", defaultTraceLevel.toString());
    try {
      return TraceLevel.valueOf(level);
    } catch (IllegalArgumentException e) {
      log.warn("Invalid tracing level: {}. Using default: {}", level, defaultTraceLevel);
      return defaultTraceLevel;
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

  private static void traceByteBuf(String key, ByteBuf buf, Map<String, Object> traceDetails) {
    if (buf == null) return;

    if (buf.readableBytes() < MAX_DATA_LENGTH) {
      traceDetails.put(key, Hex.encodeHex(buf.nioBuffer()));
    } else {
      traceDetails.put(key + "Slice", Hex.encodeHex(buf.slice(0, MAX_DATA_LENGTH).nioBuffer()));
    }
  }

  private static TraceLevel getTracingLevel(Subscription sub) {
    if (sub == null
        || sub.getSubscriptionProperties() == null
        || !sub.getSubscriptionProperties().containsKey("trace")) {
      return TraceLevel.NONE;
    }
    try {
      return TraceLevel.valueOf(sub.getSubscriptionProperties().get("trace"));
    } catch (IllegalArgumentException e) {
      log.warn(
          "Invalid tracing level: {}. Setting to NONE for subscription {}",
          sub.getSubscriptionProperties().get("trace"),
          sub);
      return TraceLevel.NONE;
    }
  }

  private static TraceLevel getTracingLevel(Producer producer) {
    if (producer == null
        || producer.getMetadata() == null
        || !producer.getMetadata().containsKey("trace")) {
      return TraceLevel.NONE;
    }
    try {
      return TraceLevel.valueOf(producer.getMetadata().get("trace"));
    } catch (IllegalArgumentException e) {
      log.warn(
          "Invalid tracing level: {}. Setting to NONE for producer {}",
          producer.getMetadata().get("trace"),
          producer);
      return TraceLevel.NONE;
    }
  }

  //    private boolean needToTraceTopic(Topic topic) {
  //        if (topic == null) return false;
  //
  //        // todo: how to read topic props
  //        return true;
  //    }

  /* ***************************
   **  Administrative events
   ******************************/

  public void onConnectionCreated(ServerCnx cnx) {
    if (!getEnabledEvents(cnx).contains(EventReasons.ADMINISTRATIVE)) return;

    TraceLevel level = getTracingLevel(cnx);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(level, cnx));
    trace("Connection created", traceDetails);
  }

  public void producerCreated(ServerCnx cnx, Producer producer, Map<String, String> metadata) {
    if (!getEnabledEvents(cnx).contains(EventReasons.ADMINISTRATIVE)) return;

    TraceLevel level = getTracingLevel(cnx);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(level, cnx));
    traceDetails.put("producer", getProducerDetails(level, producer));
    traceDetails.put("metadata", metadata);

    trace("Producer created", traceDetails);
  }

  public void producerClosed(ServerCnx cnx, Producer producer, Map<String, String> metadata) {
    if (!getEnabledEvents(cnx).contains(EventReasons.ADMINISTRATIVE)) return;

    TraceLevel level = getTracingLevel(cnx);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(level, cnx));
    traceDetails.put("producer", getProducerDetails(level, producer));
    traceDetails.put("metadata", metadata);

    trace("Producer closed", traceDetails);
  }

  public void consumerCreated(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
    if (!getEnabledEvents(cnx).contains(EventReasons.ADMINISTRATIVE)) return;

    TraceLevel level = getTracingLevel(cnx);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(level, cnx));
    traceDetails.put("consumer", getConsumerDetails(level, consumer));
    traceDetails.put("subscription", getSubscriptionDetails(level, consumer.getSubscription()));
    traceDetails.put("metadata", metadata);

    trace("Consumer created", traceDetails);
  }

  public void consumerClosed(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
    if (!getEnabledEvents(cnx).contains(EventReasons.ADMINISTRATIVE)) return;

    TraceLevel level = getTracingLevel(cnx);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(level, cnx));
    traceDetails.put("consumer", getConsumerDetails(level, consumer));
    traceDetails.put("subscription", getSubscriptionDetails(level, consumer.getSubscription()));
    traceDetails.put("metadata", metadata);

    trace("Consumer closed", traceDetails);
  }

  public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {
    if (!getEnabledEvents(cnx).contains(EventReasons.ADMINISTRATIVE)) return;

    TraceLevel level = getTracingLevel(cnx);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(level, cnx));
    traceDetails.put("command", command.toString());

    trace("Pulsar command called", traceDetails);
  }

  public void onConnectionClosed(ServerCnx cnx) {
    if (!getEnabledEvents(cnx).contains(EventReasons.ADMINISTRATIVE)) return;

    TraceLevel level = getTracingLevel(cnx);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(level, cnx));

    trace("Connection closed", traceDetails);
  }

  /* ***************************
   **  Message events
   ******************************/

  public void beforeSendMessage(
      Subscription subscription,
      Entry entry,
      long[] ackSet,
      MessageMetadata msgMetadata,
      Consumer consumer) {
    if (!getEnabledEvents(consumer.cnx().getBrokerService().getPulsar())
        .contains(EventReasons.MESSAGE)) return;

    TraceLevel level = getTracingLevel(subscription);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("subscription", getSubscriptionDetails(level, subscription));
    traceDetails.put("consumer", getConsumerDetails(level, consumer));
    traceDetails.put("entry", getEntryDetails(level, entry));
    traceDetails.put("messageMetadata", getMessageMetadataDetails(level, msgMetadata));

    trace("Before sending message", traceDetails);
  }

  public void onMessagePublish(
      Producer producer, ByteBuf headersAndPayload, Topic.PublishContext publishContext) {

    if (!getEnabledEvents(producer.getCnx().getBrokerService().getPulsar())
        .contains(EventReasons.MESSAGE)) return;

    TraceLevel level = getTracingLevel(producer);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("producer", getProducerDetails(level, producer));
    traceDetails.put("publishContext", getPublishContextDetails(publishContext));
    traceByteBuf("headersAndPayload", headersAndPayload, traceDetails);

    trace("Message publish", traceDetails);
  }

  public void messageProduced(
      ServerCnx cnx,
      Producer producer,
      long startTimeNs,
      long ledgerId,
      long entryId,
      Topic.PublishContext publishContext) {
    if (!getEnabledEvents(cnx).contains(EventReasons.MESSAGE)) return;

    TraceLevel level = getTracingLevel(producer);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(level, cnx));
    traceDetails.put("producer", getProducerDetails(level, producer));
    traceDetails.put("publishContext", getPublishContextDetails(publishContext));
    traceDetails.put("ledgerId", ledgerId);
    traceDetails.put("entryId", entryId);
    traceDetails.put("startTimeNs", startTimeNs);

    trace("Message produced", traceDetails);
  }

  public void messageDispatched(
      ServerCnx cnx, Consumer consumer, long ledgerId, long entryId, ByteBuf headersAndPayload) {
    if (!getEnabledEvents(cnx).contains(EventReasons.MESSAGE)) return;

    TraceLevel level = getTracingLevel(consumer.getSubscription());
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(level, cnx));
    traceDetails.put("consumer", getConsumerDetails(level, consumer));
    traceDetails.put("subscription", getSubscriptionDetails(level, consumer.getSubscription()));
    traceDetails.put("ledgerId", ledgerId);
    traceDetails.put("entryId", entryId);
    traceByteBuf("headersAndPayload", headersAndPayload, traceDetails);

    trace("After dispatching message", traceDetails);
  }

  public void messageAcked(ServerCnx cnx, Consumer consumer, CommandAck ackCmd) {
    if (!getEnabledEvents(cnx).contains(EventReasons.MESSAGE)) return;

    TraceLevel level = getTracingLevel(consumer.getSubscription());
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(level, cnx));
    traceDetails.put("consumer", getConsumerDetails(level, consumer));
    traceDetails.put("subscription", getSubscriptionDetails(level, consumer.getSubscription()));

    Map<String, Object> ackDetails = new TreeMap<>();
    ackDetails.put("type", ackCmd.getAckType().name());
    ackDetails.put("consumerId", ackCmd.getConsumerId());
    ackDetails.put(
        "messageIds",
        ackCmd
            .getMessageIdsList()
            .stream()
            .map(x -> x.getLedgerId() + ":" + x.getEntryId())
            .collect(Collectors.toList()));

    traceDetails.put("ack", ackDetails);

    trace("Message acked", traceDetails);
  }

  /* ***************************
   **  Transaction events
   ******************************/

  public void txnOpened(long tcId, String txnID) {
    //    if (getEnabledEvents(???).contains(EventReasons.TRANSACTION)) {
    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("tcId", tcId);
    traceDetails.put("txnID", txnID);

    trace("Transaction opened", traceDetails);
    //    }
  }

  public void txnEnded(String txnID, long txnAction) {
    //    if (getEnabledEvents(???).contains(EventReasons.TRANSACTION)) {
    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("txnID", txnID);
    traceDetails.put("txnAction", txnAction);

    trace("Transaction closed", traceDetails);
    //    }
  }

  /* ***************************
   **  Servlet events
   ******************************/

  public void onWebserviceRequest(ServletRequest request)
      throws IOException, ServletException, InterceptException {
    //    if (getEnabledEvents(???).contains(EventReasons.SERVLET)) {
    //      log.info("onWebserviceRequest: Tracing servlet requests not supported");
    //    }
  }

  public void onWebserviceResponse(ServletRequest request, ServletResponse response)
      throws IOException, ServletException {
    //    if (getEnabledEvents(???).contains(EventReasons.SERVLET)) {
    //      log.info("onWebserviceResponse: Tracing servlet requests not supported");
    //    }
  }

  // not needed
  // public void onFilter(ServletRequest request, ServletResponse response, FilterChain chain)
}
