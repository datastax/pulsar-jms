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

  public static final org.slf4j.Logger tracer = org.slf4j.LoggerFactory.getLogger("jms-tracing");

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

  private static void populateConnectionDetails(
      TraceLevel level, ServerCnx cnx, Map<String, Object> traceDetails) {
    if (cnx == null) {
      traceDetails.put("ServerCnx", "null");
      return;
    }

    switch (level) {
      case MINIMAL:
        traceDetails.put("ServerCnx.clientAddress", cnx.clientAddress().toString());
        break;
      case BASIC:
        populateConnectionDetails(TraceLevel.MINIMAL, cnx, traceDetails);

        traceDetails.put("ServerCnx.clientVersion", cnx.getClientVersion());
        traceDetails.put("ServerCnx.clientSourceAddressAndPort", cnx.clientSourceAddressAndPort());
        break;
      case FULL:
        populateConnectionDetails(TraceLevel.MINIMAL, cnx, traceDetails);
        populateConnectionDetails(TraceLevel.BASIC, cnx, traceDetails);

        traceDetails.put("ServerCnx.authRole", cnx.getAuthRole());
        traceDetails.put("ServerCnx.authMethod", cnx.getAuthMethod());
        traceDetails.put(
            "ServerCnx.authMethodName", cnx.getAuthenticationProvider().getAuthMethodName());
        break;
      default:
        log.warn("Unknown tracing level: {}", level);
        break;
    }
  }

  private static void populateSubscriptionDetails(
      TraceLevel level, Subscription sub, Map<String, Object> traceDetails) {
    if (sub == null) {
      traceDetails.put("Subscription", "null");
      return;
    }

    switch (level) {
      case MINIMAL:
        traceDetails.put("Subscription.name", sub.getName());
        traceDetails.put(
            "Subscription.topicName", TopicName.get(sub.getTopicName()).getPartitionedTopicName());
        traceDetails.put("Subscription.type", sub.getType().name());
        break;
      case BASIC:
        populateSubscriptionDetails(TraceLevel.MINIMAL, sub, traceDetails);

        if (sub.getConsumers() != null) {
          traceDetails.put("Subscription.numberOfConsumers", sub.getConsumers().size());
          traceDetails.put(
              "Subscription.namesOfConsumers",
              sub.getConsumers().stream().map(Consumer::consumerName).collect(Collectors.toList()));
        }

        break;
      case FULL:
        populateSubscriptionDetails(TraceLevel.MINIMAL, sub, traceDetails);
        populateSubscriptionDetails(TraceLevel.BASIC, sub, traceDetails);

        traceDetails.put("Subscription.subscriptionProperties", sub.getSubscriptionProperties());
        break;
      default:
        log.warn("Unknown tracing level: {}", level);
        break;
    }
  }

  private static void populateConsumerDetails(
      TraceLevel level, Consumer consumer, Map<String, Object> traceDetails) {
    if (consumer == null) {
      traceDetails.put("Consumer", "null");
      return;
    }

    switch (level) {
      case MINIMAL:
        traceDetails.put("Consumer.name", consumer.consumerName());
        traceDetails.put("Consumer.consumerId", consumer.consumerId());
        if (consumer.getSubscription() != null) {
          traceDetails.put("Consumer.subscriptionName", consumer.getSubscription().getName());
          traceDetails.put(
              "Consumer.topicName",
              TopicName.get(consumer.getSubscription().getTopicName()).getPartitionedTopicName());
        }
        break;
      case BASIC:
        populateConsumerDetails(TraceLevel.MINIMAL, consumer, traceDetails);

        traceDetails.put("Consumer.priorityLevel", consumer.getPriorityLevel());
        traceDetails.put("Consumer.subType", consumer.subType().name());
        traceDetails.put("Consumer.clientAddress", consumer.getClientAddress());
        break;
      case FULL:
        populateConsumerDetails(TraceLevel.MINIMAL, consumer, traceDetails);
        populateConsumerDetails(TraceLevel.BASIC, consumer, traceDetails);

        traceDetails.put("Consumer.metadata", consumer.getMetadata());
        break;
      default:
        log.warn("Unknown tracing level: {}", level);
        break;
    }
  }

  private static void populateProducerDetails(
      TraceLevel level, Producer producer, Map<String, Object> traceDetails) {
    if (producer == null) {
      traceDetails.put("Producer", "null");
      return;
    }

    switch (level) {
      case MINIMAL:
        traceDetails.put("Producer.producerId", producer.getProducerId());
        traceDetails.put("Producer.producerName", producer.getProducerName());
        traceDetails.put("Producer.accessMode", producer.getAccessMode().name());
        if (producer.getTopic() != null) {
          traceDetails.put(
              "Producer.topicName",
              TopicName.get(producer.getTopic().getName()).getPartitionedTopicName());
        }
        break;
      case BASIC:
        populateProducerDetails(TraceLevel.MINIMAL, producer, traceDetails);

        traceDetails.put("Producer.clientAddress", producer.getClientAddress());
        break;
      case FULL:
        populateProducerDetails(TraceLevel.MINIMAL, producer, traceDetails);
        populateProducerDetails(TraceLevel.BASIC, producer, traceDetails);

        traceDetails.put("Producer.metadata", producer.getMetadata());
        if (producer.getSchemaVersion() != null) {
          traceDetails.put("Producer.schemaVersion", producer.getSchemaVersion().toString());
        }
        traceDetails.put("producer.remoteCluster", producer.getRemoteCluster());
        break;
      default:
        log.warn("Unknown tracing level: {}", level);
        break;
    }
  }

  private static void populateMessageMetadataDetails(
      TraceLevel level, MessageMetadata msgMetadata, Map<String, Object> traceDetails) {
    if (msgMetadata == null) {
      traceDetails.put("MessageMetadata", "null");
      return;
    }

    switch (level) {
      case MINIMAL:
        traceDetails.put("MessageMetadata.sequenceId", msgMetadata.getSequenceId());
        traceDetails.put("MessageMetadata.producerName", msgMetadata.getProducerName());
        traceDetails.put("MessageMetadata.partitionKey", msgMetadata.getPartitionKey());
        break;
      case BASIC:
        populateMessageMetadataDetails(TraceLevel.MINIMAL, msgMetadata, traceDetails);

        traceDetails.put("MessageMetadata.uncompressedSize", msgMetadata.getUncompressedSize());
        traceDetails.put("MessageMetadata.serializedSize", msgMetadata.getSerializedSize());
        traceDetails.put("MessageMetadata.numMessagesInBatch", msgMetadata.getNumMessagesInBatch());
        break;
      case FULL:
        populateMessageMetadataDetails(TraceLevel.MINIMAL, msgMetadata, traceDetails);
        populateMessageMetadataDetails(TraceLevel.BASIC, msgMetadata, traceDetails);

        traceDetails.put("MessageMetadata.publishTime", msgMetadata.getPublishTime());
        traceDetails.put("MessageMetadata.eventTime", msgMetadata.getEventTime());
        traceDetails.put("MessageMetadata.replicatedFrom", msgMetadata.getReplicatedFrom());
        traceDetails.put("MessageMetadata.uuid", msgMetadata.getUuid());
        break;
      default:
        log.warn("Unknown tracing level: {}", level);
        break;
    }
  }

  private static void populateEntryDetails(
      TraceLevel level, Entry entry, Map<String, Object> traceDetails) {
    if (entry == null) {
      traceDetails.put("Entry", "null");
      return;
    }

    switch (level) {
      case MINIMAL:
        traceDetails.put("Entry.ledgerId", entry.getLedgerId());
        traceDetails.put("Entry.entryId", entry.getEntryId());
        break;
      case BASIC:
        populateEntryDetails(TraceLevel.MINIMAL, entry, traceDetails);

        traceDetails.put("Entry.length", entry.getLength());
        break;
      case FULL:
        populateEntryDetails(TraceLevel.MINIMAL, entry, traceDetails);
        populateEntryDetails(TraceLevel.BASIC, entry, traceDetails);

        traceByteBuf("Entry.data", entry.getDataBuffer(), traceDetails);
        break;
      default:
        log.warn("Unknown tracing level: {}", level);
        break;
    }
  }

  private static void populatePublishContext(
      Topic.PublishContext publishContext, Map<String, Object> traceDetails) {
    traceDetails.put("publishContext.isMarkerMessage", publishContext.isMarkerMessage());
    traceDetails.put("publishContext.isChunked", publishContext.isChunked());
    traceDetails.put("publishContext.numberOfMessages", publishContext.getNumberOfMessages());

    traceDetails.put("publishContext.entryTimestamp", publishContext.getEntryTimestamp());
    traceDetails.put("publishContext.msgSize", publishContext.getMsgSize());
    traceDetails.put("publishContext.producerName", publishContext.getProducerName());
    traceDetails.put(
        "publishContext.originalProducerName", publishContext.getOriginalProducerName());
    traceDetails.put("publishContext.originalSequenceId", publishContext.getOriginalSequenceId());
    traceDetails.put("publishContext.sequenceId", publishContext.getSequenceId());
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
    populateConnectionDetails(level, cnx, traceDetails);
    tracer.info("Connection created: {}", traceDetails);
  }

  public void producerCreated(ServerCnx cnx, Producer producer, Map<String, String> metadata) {
    if (!getEnabledEvents(cnx).contains(EventReasons.ADMINISTRATIVE)) return;

    TraceLevel level = getTracingLevel(cnx);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    populateConnectionDetails(level, cnx, traceDetails);
    populateProducerDetails(level, producer, traceDetails);

    traceDetails.put("metadata", metadata);

    tracer.info("Producer created: {}", traceDetails);
  }

  public void producerClosed(ServerCnx cnx, Producer producer, Map<String, String> metadata) {
    if (!getEnabledEvents(cnx).contains(EventReasons.ADMINISTRATIVE)) return;

    TraceLevel level = getTracingLevel(cnx);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    populateConnectionDetails(level, cnx, traceDetails);
    populateProducerDetails(level, producer, traceDetails);

    traceDetails.put("metadata", metadata);

    tracer.info("Producer closed: {}", traceDetails);
  }

  public void consumerCreated(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
    if (!getEnabledEvents(cnx).contains(EventReasons.ADMINISTRATIVE)) return;

    TraceLevel level = getTracingLevel(cnx);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    populateConnectionDetails(level, cnx, traceDetails);
    populateConsumerDetails(level, consumer, traceDetails);
    populateSubscriptionDetails(level, consumer.getSubscription(), traceDetails);

    traceDetails.put("metadata", metadata);

    tracer.info("Consumer created: {}", traceDetails);
  }

  public void consumerClosed(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
    if (!getEnabledEvents(cnx).contains(EventReasons.ADMINISTRATIVE)) return;

    TraceLevel level = getTracingLevel(cnx);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    populateConnectionDetails(level, cnx, traceDetails);
    populateConsumerDetails(level, consumer, traceDetails);
    populateSubscriptionDetails(level, consumer.getSubscription(), traceDetails);

    traceDetails.put("metadata", metadata);

    tracer.info("Consumer closed: {}", traceDetails);
  }

  public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {
    if (!getEnabledEvents(cnx).contains(EventReasons.ADMINISTRATIVE)) return;

    TraceLevel level = getTracingLevel(cnx);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    populateConnectionDetails(level, cnx, traceDetails);
    traceDetails.put("command", command.toString());
    tracer.info("Pulsar command called: {}", traceDetails);
  }

  public void onConnectionClosed(ServerCnx cnx) {
    if (!getEnabledEvents(cnx).contains(EventReasons.ADMINISTRATIVE)) return;

    TraceLevel level = getTracingLevel(cnx);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    populateConnectionDetails(level, cnx, traceDetails);
    tracer.info("Connection closed: {}", traceDetails);
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
    populateConsumerDetails(level, consumer, traceDetails);
    populateSubscriptionDetails(level, subscription, traceDetails);
    populateEntryDetails(level, entry, traceDetails);
    populateMessageMetadataDetails(level, msgMetadata, traceDetails);

    tracer.info("Before sending message: {}", traceDetails);
  }

  public void onMessagePublish(
      Producer producer, ByteBuf headersAndPayload, Topic.PublishContext publishContext) {

    if (!getEnabledEvents(producer.getCnx().getBrokerService().getPulsar())
        .contains(EventReasons.MESSAGE)) return;

    TraceLevel level = getTracingLevel(producer);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    populateProducerDetails(level, producer, traceDetails);

    traceByteBuf("headersAndPayload", headersAndPayload, traceDetails);

    populatePublishContext(publishContext, traceDetails);

    tracer.info("Message publish: {}", traceDetails);
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

    populateConnectionDetails(level, cnx, traceDetails);
    populateProducerDetails(level, producer, traceDetails);
    populatePublishContext(publishContext, traceDetails);

    traceDetails.put("ledgerId", ledgerId);
    traceDetails.put("entryId", entryId);
    traceDetails.put("startTimeNs", startTimeNs);

    tracer.info("Message produced: {}", traceDetails);
  }

  public void messageDispatched(
      ServerCnx cnx, Consumer consumer, long ledgerId, long entryId, ByteBuf headersAndPayload) {
    if (!getEnabledEvents(cnx).contains(EventReasons.MESSAGE)) return;

    TraceLevel level = getTracingLevel(consumer.getSubscription());
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    populateConnectionDetails(level, cnx, traceDetails);
    populateConsumerDetails(level, consumer, traceDetails);
    populateSubscriptionDetails(level, consumer.getSubscription(), traceDetails);

    traceDetails.put("ledgerId", ledgerId);
    traceDetails.put("entryId", entryId);

    traceByteBuf("headersAndPayload", headersAndPayload, traceDetails);

    tracer.info("After dispatching message: {}", traceDetails);
  }

  public void messageAcked(ServerCnx cnx, Consumer consumer, CommandAck ackCmd) {
    if (!getEnabledEvents(cnx).contains(EventReasons.MESSAGE)) return;

    TraceLevel level = getTracingLevel(consumer.getSubscription());
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    populateConnectionDetails(level, cnx, traceDetails);
    populateConsumerDetails(level, consumer, traceDetails);
    populateSubscriptionDetails(level, consumer.getSubscription(), traceDetails);

    traceDetails.put("ack.type", ackCmd.getAckType().name());
    traceDetails.put("ack.consumerId", ackCmd.getConsumerId());
    traceDetails.put(
        "ack.messageIds",
        ackCmd
            .getMessageIdsList()
            .stream()
            .map(x -> x.getLedgerId() + ":" + x.getEntryId())
            .collect(Collectors.toList()));

    tracer.info("Message acked: {}", traceDetails);
  }

  /* ***************************
   **  Transaction events
   ******************************/

  public void txnOpened(long tcId, String txnID) {
    //    if (getEnabledEvents(???).contains(EventReasons.TRANSACTION)) {
    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("tcId", tcId);
    traceDetails.put("txnID", txnID);

    tracer.info("Transaction opened: {}", traceDetails);
    //    }
  }

  public void txnEnded(String txnID, long txnAction) {
    //    if (getEnabledEvents(???).contains(EventReasons.TRANSACTION)) {
    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("txnID", txnID);
    traceDetails.put("txnAction", txnAction);

    tracer.info("Transaction closed: {}", traceDetails);
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
