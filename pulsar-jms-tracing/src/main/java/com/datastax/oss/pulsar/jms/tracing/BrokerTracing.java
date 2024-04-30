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

import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.TraceLevel;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.getCommandDetails;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.getConnectionDetails;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.getConsumerDetails;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.getEntryDetails;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.getMessageMetadataDetails;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.getProducerDetails;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.getPublishContextDetails;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.getSubscriptionDetails;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.trace;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.traceByteBuf;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.intercept.InterceptException;
import org.apache.pulsar.common.naming.TopicName;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class BrokerTracing implements BrokerInterceptor {

  public enum EventReasons {
    ADMINISTRATIVE,
    COMMANDS,
    MESSAGE,
    TRANSACTION,
    SERVLET,
  }

  private static final TraceLevel defaultTraceLevel = TraceLevel.BASIC;

  private final Set<EventReasons> jmsTracingEventList = new HashSet<>();
  private TraceLevel traceLevel = defaultTraceLevel;
  private int maxBinaryDataLength = 256;
  private int cacheTraceLevelsDurationSec = 10;
  private boolean traceSystemTopics = false;
  private boolean traceSchema = false;
  private boolean reduceLevelForNestedComponents = true;

  private static Set<EventReasons> loadEnabledEvents(
      PulsarService pulsarService, Set<EventReasons> enabledEvents) {
    String events =
        pulsarService.getConfiguration().getProperties().getProperty("jmsTracingEventList", "");
    log.debug("read jmsTracingEventList: {}", events);

    for (String event : events.split(",")) {
      try {
        enabledEvents.add(EventReasons.valueOf(event.trim().toUpperCase()));
      } catch (IllegalArgumentException e) {
        log.error("Invalid event: {}. Skipping", event);
      }
    }

    return enabledEvents;
  }

  @NotNull
  private static TraceLevel getTraceLevel(PulsarService pulsar) {
    String level =
        pulsar
            .getConfiguration()
            .getProperties()
            .getProperty("jmsTracingLevel", defaultTraceLevel.toString());
    try {
      return TraceLevel.valueOf(level.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      log.warn("Invalid tracing level: {}. Using default: {}", level, defaultTraceLevel);
      return defaultTraceLevel;
    }
  }

  private final LoadingCache<Subscription, TraceLevel> traceLevelForSubscription =
      CacheBuilder.newBuilder()
          .expireAfterWrite(cacheTraceLevelsDurationSec, TimeUnit.SECONDS)
          .build(
              new CacheLoader<Subscription, TraceLevel>() {
                public TraceLevel load(Subscription sub) {
                  Map<String, String> subProps = sub.getSubscriptionProperties();
                  try {
                    if (subProps == null || !subProps.containsKey("trace")) {
                      PulsarAdmin admin =
                          sub.getTopic().getBrokerService().getPulsar().getAdminClient();
                      return BrokerTracing.getTracingLevel(admin, sub.getTopic());
                    }

                    return TraceLevel.valueOf(subProps.get("trace").trim().toUpperCase());
                  } catch (IllegalArgumentException e) {
                    log.warn(
                        "Invalid tracing level: {}. Setting to NONE for subscription {}",
                        subProps.get("trace"),
                        sub);
                    return TraceLevel.NONE;
                  } catch (Throwable t) {
                    log.error("Error getting tracing level", t);
                    return TraceLevel.NONE;
                  }
                }
              });

  private final LoadingCache<Producer, TraceLevel> traceLevelForProducer =
      CacheBuilder.newBuilder()
          .expireAfterWrite(cacheTraceLevelsDurationSec, TimeUnit.SECONDS)
          .build(
              new CacheLoader<Producer, TraceLevel>() {
                public TraceLevel load(Producer producer) {
                  try {
                    PulsarAdmin admin =
                        producer.getCnx().getBrokerService().getPulsar().getAdminClient();
                    Topic topic = producer.getTopic();

                    return BrokerTracing.getTracingLevel(admin, topic);
                  } catch (Throwable t) {
                    log.error("Error getting tracing level", t);
                    return TraceLevel.NONE;
                  }
                }
              });

  @NotNull
  private static TraceLevel getTracingLevel(PulsarAdmin admin, Topic topic) {
    Map<String, String> props = null;
    try {
      props =
          admin.topics().getProperties(TopicName.get(topic.getName()).getPartitionedTopicName());

      if (props == null || !props.containsKey("trace")) {
        return TraceLevel.NONE;
      }

      return TraceLevel.valueOf(props.get("trace").trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      log.warn(
          "Invalid tracing level: {}. Setting to NONE",
          props == null ? "no props" : props.get("trace"));
      return TraceLevel.NONE;
    } catch (Throwable t) {
      log.error("Error getting tracing level", t);
      return TraceLevel.NONE;
    }
  }

  public void initialize(PulsarService pulsarService) {
    log.info("Initializing BrokerTracing");

    loadEnabledEvents(pulsarService, jmsTracingEventList);
    traceLevel = getTraceLevel(pulsarService);

    Properties props = pulsarService.getConfiguration().getProperties();
    if (props.containsKey("jmsTracingMaxBinaryDataLength")) {
      maxBinaryDataLength = Integer.parseInt(props.getProperty("jmsTracingMaxBinaryDataLength"));
    }
    if (props.containsKey("jmsTracingTraceSystemTopics")) {
      traceSystemTopics = Boolean.parseBoolean(props.getProperty("jmsTracingTraceSystemTopics"));
    }
    if (props.containsKey("jmsTracingTraceSchema")) {
      traceSchema = Boolean.parseBoolean(props.getProperty("jmsTracingTraceSchema"));
    }
    if (props.containsKey("jmsTracingReduceLevelForNestedComponents")) {
      reduceLevelForNestedComponents =
          Boolean.parseBoolean(props.getProperty("jmsTracingReduceLevelForNestedComponents"));
    }
    if (props.containsKey("jmsTracingCacheTraceLevelsDurationSec")) {
      cacheTraceLevelsDurationSec =
          Integer.parseInt(props.getProperty("jmsTracingCacheTraceLevelsDurationSec"));
      if (cacheTraceLevelsDurationSec <= 0) {
        log.warn(
            "Invalid cache duration: {}. Setting to default: {}", cacheTraceLevelsDurationSec, 10);
        cacheTraceLevelsDurationSec = 10;
      }
    }
  }

  @Override
  public void close() {
    log.info("Closing BrokerTracing");
  }

  private TraceLevel getTracingLevel(Consumer consumer) {
    if (consumer == null) return TraceLevel.NONE;

    return getTracingLevel(consumer.getSubscription());
  }

  private TraceLevel getTracingLevel(Subscription sub) {
    if (sub == null) return TraceLevel.NONE;

    if (!traceSystemTopics && sub.getTopic().isSystemTopic()) return TraceLevel.NONE;

    try {
      return traceLevelForSubscription.get(sub);
    } catch (ExecutionException e) {
      log.error("Error getting tracing level", e);
      return TraceLevel.NONE;
    }
  }

  private TraceLevel getTracingLevel(Producer producer) {
    if (producer == null) return TraceLevel.NONE;

    if (!traceSystemTopics && producer.getTopic().isSystemTopic()) return TraceLevel.NONE;

    try {
      return traceLevelForProducer.get(producer);
    } catch (ExecutionException e) {
      log.error("Error getting tracing level", e);
      return TraceLevel.NONE;
    }
  }

  private TraceLevel getTraceLevelForComponent(TraceLevel current) {
    if (current == TraceLevel.NONE) return TraceLevel.NONE;
    if (reduceLevelForNestedComponents) return TraceLevel.BASIC;

    return current;
  }

  /* ***************************
   **  Administrative events
   ******************************/

  public void onConnectionCreated(ServerCnx cnx) {
    if (!jmsTracingEventList.contains(EventReasons.ADMINISTRATIVE)) return;

    if (traceLevel == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(traceLevel, cnx));
    trace("Connection created", traceDetails);
  }

  public void producerCreated(ServerCnx cnx, Producer producer, Map<String, String> metadata) {
    if (!jmsTracingEventList.contains(EventReasons.ADMINISTRATIVE)) return;
    if (!traceSystemTopics && producer.getTopic().isSystemTopic()) return;

    if (traceLevel == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(getTraceLevelForComponent(traceLevel), cnx));
    traceDetails.put("producer", getProducerDetails(traceLevel, producer, traceSchema));
    traceDetails.put("metadata", metadata);

    trace("Producer created", traceDetails);
  }

  public void producerClosed(ServerCnx cnx, Producer producer, Map<String, String> metadata) {
    if (!jmsTracingEventList.contains(EventReasons.ADMINISTRATIVE)) return;
    if (!traceSystemTopics && producer.getTopic().isSystemTopic()) return;

    if (traceLevel == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(getTraceLevelForComponent(traceLevel), cnx));
    traceDetails.put("producer", getProducerDetails(traceLevel, producer, traceSchema));
    traceDetails.put("metadata", metadata);

    trace("Producer closed", traceDetails);
  }

  public void consumerCreated(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
    if (!jmsTracingEventList.contains(EventReasons.ADMINISTRATIVE)) return;
    if (!traceSystemTopics && consumer.getSubscription().getTopic().isSystemTopic()) return;

    if (traceLevel == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(getTraceLevelForComponent(traceLevel), cnx));
    traceDetails.put("consumer", getConsumerDetails(traceLevel, consumer));
    traceDetails.put(
        "subscription", getSubscriptionDetails(traceLevel, consumer.getSubscription()));
    traceDetails.put("metadata", metadata);

    trace("Consumer created", traceDetails);
  }

  public void consumerClosed(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
    if (!jmsTracingEventList.contains(EventReasons.ADMINISTRATIVE)) return;
    if (!traceSystemTopics && consumer.getSubscription().getTopic().isSystemTopic()) return;

    if (traceLevel == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(getTraceLevelForComponent(traceLevel), cnx));
    traceDetails.put("consumer", getConsumerDetails(traceLevel, consumer));
    traceDetails.put(
        "subscription", getSubscriptionDetails(traceLevel, consumer.getSubscription()));
    traceDetails.put("metadata", metadata);

    trace("Consumer closed", traceDetails);
  }

  public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {
    if (!jmsTracingEventList.contains(EventReasons.COMMANDS)) return;

    if (traceLevel == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(getTraceLevelForComponent(traceLevel), cnx));

    if (command.hasType()) {
      traceDetails.put("type", command.getType().name());
      if (traceLevel != TraceLevel.MINIMAL) {
        traceDetails.put("command", getCommandDetails(traceLevel, command));
      }
    } else {
      traceDetails.put("type", "unknown/null");
    }

    trace("Pulsar command called", traceDetails);
  }

  public void onConnectionClosed(ServerCnx cnx) {
    if (!jmsTracingEventList.contains(EventReasons.ADMINISTRATIVE)) return;

    if (traceLevel == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(traceLevel, cnx));

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
    if (!jmsTracingEventList.contains(EventReasons.MESSAGE)) return;

    TraceLevel level = getTracingLevel(subscription);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put(
        "subscription", getSubscriptionDetails(getTraceLevelForComponent(level), subscription));
    traceDetails.put("consumer", getConsumerDetails(getTraceLevelForComponent(level), consumer));
    traceDetails.put("entry", getEntryDetails(level, entry, maxBinaryDataLength));
    traceDetails.put("messageMetadata", getMessageMetadataDetails(level, msgMetadata));

    trace("Before sending message", traceDetails);
  }

  public void onMessagePublish(
      Producer producer, ByteBuf headersAndPayload, Topic.PublishContext publishContext) {

    if (!jmsTracingEventList.contains(EventReasons.MESSAGE)) return;

    TraceLevel level = getTracingLevel(producer);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put(
        "producer", getProducerDetails(getTraceLevelForComponent(level), producer, traceSchema));
    traceDetails.put("publishContext", getPublishContextDetails(publishContext));
    traceByteBuf("headersAndPayload", headersAndPayload, traceDetails, maxBinaryDataLength);

    trace("Message publish", traceDetails);
  }

  public void messageProduced(
      ServerCnx cnx,
      Producer producer,
      long startTimeNs,
      long ledgerId,
      long entryId,
      Topic.PublishContext publishContext) {
    if (!jmsTracingEventList.contains(EventReasons.MESSAGE)) return;

    TraceLevel level = getTracingLevel(producer);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(getTraceLevelForComponent(level), cnx));
    traceDetails.put(
        "producer", getProducerDetails(getTraceLevelForComponent(level), producer, traceSchema));
    traceDetails.put("publishContext", getPublishContextDetails(publishContext));
    traceDetails.put("messageId", ledgerId + ":" + entryId);
    traceDetails.put("startTimeNs", startTimeNs);
    trace("Message produced", traceDetails);
  }

  public void messageDispatched(
      ServerCnx cnx, Consumer consumer, long ledgerId, long entryId, ByteBuf headersAndPayload) {
    if (!jmsTracingEventList.contains(EventReasons.MESSAGE)) return;

    TraceLevel level = getTracingLevel(consumer);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(getTraceLevelForComponent(level), cnx));
    traceDetails.put("consumer", getConsumerDetails(getTraceLevelForComponent(level), consumer));
    traceDetails.put(
        "subscription",
        getSubscriptionDetails(getTraceLevelForComponent(level), consumer.getSubscription()));
    traceDetails.put("messageId", ledgerId + ":" + entryId);
    traceByteBuf("headersAndPayload", headersAndPayload, traceDetails, maxBinaryDataLength);

    trace("After dispatching message", traceDetails);
  }

  public void messageAcked(ServerCnx cnx, Consumer consumer, CommandAck ackCmd) {
    if (!jmsTracingEventList.contains(EventReasons.MESSAGE)) return;

    TraceLevel level = getTracingLevel(consumer);
    if (level == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(getTraceLevelForComponent(level), cnx));
    traceDetails.put("consumer", getConsumerDetails(getTraceLevelForComponent(level), consumer));
    traceDetails.put(
        "subscription",
        getSubscriptionDetails(getTraceLevelForComponent(level), consumer.getSubscription()));

    Map<String, Object> ackDetails = new TreeMap<>();
    if (ackCmd.hasAckType()) {
      ackDetails.put("type", ackCmd.getAckType().name());
    } else {
      ackDetails.put("type", "NOT SET");
    }
    if (ackCmd.hasConsumerId()) {
      ackDetails.put("consumerId", ackCmd.getConsumerId());
    } else {
      ackDetails.put("consumerId", "NOT SET");
    }
    ackDetails.put("numAckedMessages", ackCmd.getMessageIdsCount());
    ackDetails.put(
        "messageIds",
        ackCmd
            .getMessageIdsList()
            .stream()
            .map(BrokerTracing::formatMessageId)
            .collect(Collectors.toList()));

    if (ackCmd.hasTxnidLeastBits() && ackCmd.hasTxnidMostBits()) {
      ackDetails.put(
          "txnID", "(" + ackCmd.getTxnidMostBits() + "," + ackCmd.getTxnidLeastBits() + ")");
    }
    if (ackCmd.hasRequestId()) {
      ackDetails.put("requestId", ackCmd.getRequestId());
    }

    traceDetails.put("ack", ackDetails);

    trace("Message acked", traceDetails);
  }

  @NotNull
  private static String formatMessageId(MessageIdData x) {
    String msgId = x.getLedgerId() + ":" + x.getEntryId();
    if (x.hasBatchIndex()) {
      msgId += " (batchSize: " + x.getBatchSize() + "|ackSetCnt: " + x.getAckSetsCount() + ")";
    } else if (x.getAckSetsCount() > 0) {
      msgId += " (ackSetCnt " + x.getAckSetsCount() + ")";
    }
    return msgId;
  }

  /* ***************************
   **  Transaction events
   ******************************/

  public void txnOpened(long tcId, String txnID) {
    if (!jmsTracingEventList.contains(EventReasons.TRANSACTION)) return;
    if (traceLevel == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("tcId", tcId);
    traceDetails.put("txnID", txnID);

    trace("Transaction opened", traceDetails);
  }

  public void txnEnded(String txnID, long txnAction) {
    if (!jmsTracingEventList.contains(EventReasons.TRANSACTION)) return;
    if (traceLevel == TraceLevel.NONE) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("txnID", txnID);
    traceDetails.put("txnAction", txnAction);

    trace("Transaction closed", traceDetails);
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
