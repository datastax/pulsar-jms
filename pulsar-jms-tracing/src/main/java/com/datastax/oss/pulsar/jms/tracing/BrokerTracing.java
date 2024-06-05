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

import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.EventCategory;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.EventSubCategory;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.TraceLevel;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.getConnectionDetails;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.getConsumerDetails;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.getEntryDetails;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.getProducerDetails;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.getPublishContextDetails;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.getSubscriptionDetails;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.hostNameOf;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.trace;
import static com.datastax.oss.pulsar.jms.tracing.TracingUtils.traceMetadataAndPayload;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.ByteBuf;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.commons.lang3.reflect.FieldUtils;
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
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.PublisherStatsImpl;
import org.apache.pulsar.common.util.DateFormatter;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class BrokerTracing implements BrokerInterceptor {

  private static final TraceLevel defaultTraceLevel = TraceLevel.OFF;

  private final Set<EventCategory> jmsTracingEventCategory = new HashSet<>();
  private TraceLevel traceLevel = defaultTraceLevel;
  private int maxPayloadLength = 256;
  private int cacheTraceLevelsDurationSec = 10;
  private boolean traceSystemTopics = false;
  private boolean traceSchema = false;

  private static void loadEnabledEvents(
      PulsarService pulsarService, Set<EventCategory> enabledEvents, TraceLevel traceLevel) {

    if (traceLevel == TraceLevel.OFF) {
      log.info("Tracing is disabled. jmsTracingEventCategory is ignored.");
      enabledEvents.clear();
      return;
    }

    Properties props = pulsarService.getConfiguration().getProperties();
    if (props.containsKey("jmsTracingEventCategory")) {
      String events = props.getProperty("jmsTracingEventCategory", "");
      log.info("read jmsTracingEventCategory: {}", events);

      enabledEvents.clear();
      for (String event : events.split(",")) {
        try {
          enabledEvents.add(EventCategory.valueOf(event.trim().toUpperCase()));
        } catch (IllegalArgumentException e) {
          log.error("Invalid event: {}. Skipping", event);
        }
      }
    } else {
      log.warn("jmsTracingEventCategory is not set. Using ADMIN, CONN.");
      enabledEvents.add(EventCategory.CONN);
    }
  }

  @NotNull
  private static TraceLevel getTraceLevel(PulsarService pulsar) {
    String level =
        pulsar
            .getConfiguration()
            .getProperties()
            .getProperty("jmsTracingLevel", defaultTraceLevel.toString());
    try {
      TraceLevel traceLevel = TraceLevel.valueOf(level.trim().toUpperCase());
      log.info("Using tracing level: {}", traceLevel);
      return traceLevel;
    } catch (IllegalArgumentException e) {
      log.warn("Invalid tracing level: {}. Using default: {}", level, defaultTraceLevel);
      return defaultTraceLevel;
    }
  }

  private final LoadingCache<Subscription, TraceLevel> traceLevelForSubscription =
      CacheBuilder.newBuilder()
          .maximumSize(10_000L)
          .concurrencyLevel(Runtime.getRuntime().availableProcessors())
          .expireAfterWrite(10L * cacheTraceLevelsDurationSec, TimeUnit.SECONDS)
          .refreshAfterWrite(cacheTraceLevelsDurationSec, TimeUnit.SECONDS)
          .build(
              new CacheLoader<Subscription, TraceLevel>() {
                public TraceLevel load(Subscription sub) {
                  log.info("Loading trace level for subscription {}", sub);
                  return BrokerTracing.readTraceLevelForSubscription(sub);
                }

                public ListenableFuture<TraceLevel> reload(Subscription sub, TraceLevel oldValue)
                    throws Exception {
                  SettableFuture<TraceLevel> future = SettableFuture.create();
                  BrokerTracing.readTraceLevelForSubscriptionAsync(sub)
                      .whenComplete(
                          (level, ex) -> {
                            if (ex != null) {
                              future.setException(ex);
                            } else {
                              future.set(level);
                            }
                          });
                  return future;
                }
              });
  private final LoadingCache<Producer, TraceLevel> traceLevelForProducer =
      CacheBuilder.newBuilder()
          .maximumSize(10_000L)
          .concurrencyLevel(Runtime.getRuntime().availableProcessors())
          .expireAfterWrite(10L * cacheTraceLevelsDurationSec, TimeUnit.SECONDS)
          .refreshAfterWrite(cacheTraceLevelsDurationSec, TimeUnit.SECONDS)
          .build(
              new CacheLoader<Producer, TraceLevel>() {
                public TraceLevel load(Producer producer) {
                  try {
                    log.info("Loading trace level for producer {}", producer);
                    PulsarAdmin admin =
                        producer.getCnx().getBrokerService().getPulsar().getAdminClient();
                    Topic topic = producer.getTopic();

                    return BrokerTracing.readTraceLevelForTopic(admin, topic);
                  } catch (Throwable t) {
                    log.error("Error getting tracing level", t);
                    return TraceLevel.OFF;
                  }
                }

                public ListenableFuture<TraceLevel> reload(Producer producer, TraceLevel oldValue)
                    throws Exception {
                  SettableFuture<TraceLevel> future = SettableFuture.create();

                  PulsarAdmin admin =
                      producer.getCnx().getBrokerService().getPulsar().getAdminClient();
                  Topic topic = producer.getTopic();

                  BrokerTracing.readTraceLevelForTopicAsync(admin, topic)
                      .whenComplete(
                          (level, ex) -> {
                            if (ex != null) {
                              future.setException(ex);
                            } else {
                              future.set(level);
                            }
                          });

                  return future;
                }
              });

  @NotNull
  private static TraceLevel readTraceLevelForSubscription(Subscription sub) {
    try {
      return readTraceLevelForSubscriptionAsync(sub).get();
    } catch (InterruptedException | ExecutionException e) {
      log.error("Interrupted while getting subscription tracing level for {}", sub, e);
      Thread.currentThread().interrupt();
      return TraceLevel.OFF;
    } catch (Throwable t) {
      log.error("Error getting subscription tracing level for {}", sub, t);
      return TraceLevel.OFF;
    }
  }

  @NotNull
  private static CompletableFuture<TraceLevel> readTraceLevelForSubscriptionAsync(
      Subscription sub) {
    Map<String, String> subProps = sub.getSubscriptionProperties();
    try {
      if (subProps == null || !subProps.containsKey("trace")) {
        PulsarAdmin admin = sub.getTopic().getBrokerService().getPulsar().getAdminClient();
        return BrokerTracing.readTraceLevelForTopicAsync(admin, sub.getTopic());
      }

      return CompletableFuture.completedFuture(
          TraceLevel.valueOf(subProps.get("trace").trim().toUpperCase()));
    } catch (IllegalArgumentException e) {
      log.warn(
          "Invalid tracing level: {}. Setting to NONE for subscription {}",
          subProps.get("trace"),
          sub);
      return CompletableFuture.completedFuture(TraceLevel.OFF);
    } catch (Throwable t) {
      log.error("Error getting tracing level. Setting to NONE for subscription {}", sub, t);
      return CompletableFuture.completedFuture(TraceLevel.OFF);
    }
  }

  @NotNull
  private static TraceLevel readTraceLevelForTopic(PulsarAdmin admin, Topic topic) {
    try {
      return readTraceLevelForTopicAsync(admin, topic).get();
    } catch (InterruptedException | ExecutionException e) {
      log.error("Interrupted while getting tracing level for topic {}", topic.getName(), e);
      Thread.currentThread().interrupt();
      return TraceLevel.OFF;
    } catch (Throwable t) {
      log.error("Error getting tracing level for topic {}", topic.getName(), t);
      return TraceLevel.OFF;
    }
  }

  @NotNull
  private static CompletableFuture<TraceLevel> readTraceLevelForTopicAsync(
      PulsarAdmin admin, Topic topic) {
    CompletableFuture<Map<String, String>> propsFuture =
        admin.topics().getPropertiesAsync(TopicName.get(topic.getName()).getPartitionedTopicName());
    return propsFuture.handle(
        (props, ex) -> {
          if (ex != null) {
            log.error("Error getting tracing level for topic {}", topic.getName(), ex);
            return TraceLevel.OFF;
          }

          try {
            if (props == null || !props.containsKey("trace")) {
              return TraceLevel.OFF;
            }

            return TraceLevel.valueOf(props.get("trace").trim().toUpperCase());
          } catch (IllegalArgumentException e) {
            log.warn(
                "Invalid tracing level for topic {}: {}. Setting to NONE",
                topic.getName(),
                props.get("trace"));
            return TraceLevel.OFF;
          }
        });
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

  public void initialize(PulsarService pulsarService) {
    log.info("Initializing BrokerTracing");

    traceLevel = getTraceLevel(pulsarService);

    loadEnabledEvents(pulsarService, jmsTracingEventCategory, traceLevel);

    Properties props = pulsarService.getConfiguration().getProperties();
    if (props.containsKey("jmsTracingMaxPayloadLength")) {
      maxPayloadLength = Integer.parseInt(props.getProperty("jmsTracingMaxPayloadLength"));
      log.info("Setting maxPayloadLength to {}", maxPayloadLength);
    }
    if (props.containsKey("jmsTracingTraceSystemTopics")) {
      traceSystemTopics = Boolean.parseBoolean(props.getProperty("jmsTracingTraceSystemTopics"));
      log.info("Setting traceSystemTopics to {}", traceSystemTopics);
    }
    if (props.containsKey("jmsTracingTraceSchema")) {
      traceSchema = Boolean.parseBoolean(props.getProperty("jmsTracingTraceSchema"));
      log.info("Setting traceSchema to {}", traceSchema);
    }
    if (props.containsKey("jmsTracingCacheTraceLevelsDurationSec")) {
      cacheTraceLevelsDurationSec =
          Integer.parseInt(props.getProperty("jmsTracingCacheTraceLevelsDurationSec"));
      if (cacheTraceLevelsDurationSec <= 0) {
        log.warn(
            "Invalid cache duration: {}. Setting to default: {}", cacheTraceLevelsDurationSec, 10);
        cacheTraceLevelsDurationSec = 10;
      }
      log.info("Setting cacheTraceLevelsDurationSec to {}", cacheTraceLevelsDurationSec);
    }
  }

  @Override
  public void close() {
    log.info("Closing BrokerTracing");
  }

  private TraceLevel getTracingLevel(Consumer consumer) {
    if (consumer == null) return TraceLevel.OFF;

    return getTracingLevel(consumer.getSubscription());
  }

  private TraceLevel getTracingLevel(Subscription sub) {
    if (sub == null) return TraceLevel.OFF;

    if (!traceSystemTopics && sub.getTopic().isSystemTopic()) return TraceLevel.OFF;

    try {
      return traceLevelForSubscription.get(sub);
    } catch (ExecutionException e) {
      log.error("Error getting tracing level", e);
      return TraceLevel.OFF;
    }
  }

  private TraceLevel getTracingLevel(Producer producer) {
    if (producer == null) return TraceLevel.OFF;

    if (!traceSystemTopics && producer.getTopic().isSystemTopic()) return TraceLevel.OFF;

    try {
      return traceLevelForProducer.get(producer);
    } catch (ExecutionException e) {
      log.error("Error getting tracing level", e);
      return TraceLevel.OFF;
    }
  }

  private static void addMinimumProducerDetails(
      Producer producer, Map<String, Object> traceDetails) {
    if (producer == null) return;

    traceDetails.put("producerId", producer.getProducerId());
    traceDetails.put("producerName", producer.getProducerName());
    if (producer.getAccessMode() != null) {
      traceDetails.put("accessMode", producer.getAccessMode().name());
    }
    traceDetails.put(
        "clientHost",
        TracingUtils.hostNameOf(
            producer.getClientAddress(), producer.getCnx().clientSourceAddressAndPort()));

    if (producer.getTopic() != null) {
      traceDetails.put(
          "topicName", TopicName.get(producer.getTopic().getName()).getPartitionedTopicName());
    }
    traceDetails.put("authRole", producer.getCnx().getAuthRole());
  }

  private static void addMinimumConsumerSubscriptionDetails(
      Consumer consumer, Map<String, Object> traceDetails) {
    if (consumer == null) return;

    addMinimumConsumerSubscriptionDetails(consumer, consumer.getSubscription(), traceDetails);
  }

  private static void addMinimumConsumerSubscriptionDetails(
      Consumer consumer, Subscription subscription, Map<String, Object> traceDetails) {
    if (consumer != null) {
      traceDetails.put("consumerName", consumer.consumerName());
      traceDetails.put("consumerId", consumer.consumerId());
      traceDetails.put(
          "clientHost",
          TracingUtils.hostNameOf(
              consumer.getClientAddress(), consumer.cnx().clientSourceAddressAndPort()));
      traceDetails.put("authRole", consumer.cnx().getAuthRole());
    }

    if (subscription != null) {
      traceDetails.put("subscriptionName", subscription.getName());
      traceDetails.put(
          "topicName", TopicName.get(subscription.getTopicName()).getPartitionedTopicName());
      traceDetails.put("subscriptionType", subscription.getType().name());
    }
  }

  /* ***************************
   **  Connection events
   ******************************/

  public void onConnectionCreated(ServerCnx cnx) {
    if (!jmsTracingEventCategory.contains(EventCategory.CONN)) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(cnx));
    traceDetails.put("brokerUrl", cnx.getBrokerService().getPulsar().getBrokerServiceUrl());

    trace(EventCategory.CONN, EventSubCategory.CREATED, traceDetails);
  }

  public void onConnectionClosed(ServerCnx cnx) {
    if (!jmsTracingEventCategory.contains(EventCategory.CONN)) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("serverCnx", getConnectionDetails(cnx));
    traceDetails.put("brokerUrl", cnx.getBrokerService().getPulsar().getBrokerServiceUrl());

    trace(EventCategory.CONN, EventSubCategory.CLOSED, traceDetails);
  }

  /* **************************
   * Producer connection events
   *****************************/

  public void producerCreated(ServerCnx cnx, Producer producer, Map<String, String> metadata) {
    if (!jmsTracingEventCategory.contains(EventCategory.PROD)) return;
    if (!traceSystemTopics && producer.getTopic().isSystemTopic()) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("producer", getProducerDetails(producer, traceSchema));
    traceDetails.put("metadata", metadata);
    traceDetails.put("brokerUrl", cnx.getBrokerService().getPulsar().getBrokerServiceUrl());

    trace(EventCategory.PROD, EventSubCategory.CREATED, traceDetails);
  }

  public void producerClosed(ServerCnx cnx, Producer producer, Map<String, String> metadata) {
    if (!jmsTracingEventCategory.contains(EventCategory.PROD)) return;
    if (!traceSystemTopics && producer.getTopic().isSystemTopic()) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("producer", getProducerDetails(producer, traceSchema));
    traceDetails.put("metadata", metadata);
    traceDetails.put("brokerUrl", cnx.getBrokerService().getPulsar().getBrokerServiceUrl());

    Map<String, Object> statsTrace = new TreeMap<>();
    PublisherStatsImpl stats = producer.getStats();

    statsTrace.put("connectedSince", stats.getConnectedSince());
    statsTrace.put("closedAt", DateFormatter.now());
    statsTrace.put("averageMsgSize", stats.getAverageMsgSize());
    statsTrace.put("msgRateIn", stats.getMsgRateIn());
    statsTrace.put("msgThroughputIn", stats.getMsgThroughputIn());
    // no message count in stats? stats.getCount() is not it
    traceDetails.put("stats", statsTrace);

    trace(EventCategory.PROD, EventSubCategory.CLOSED, traceDetails);
  }

  /* **************************
   * Consumer connection events
   *****************************/

  public void consumerCreated(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
    if (!jmsTracingEventCategory.contains(EventCategory.CONS)) return;
    if (!traceSystemTopics && consumer.getSubscription().getTopic().isSystemTopic()) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("consumer", getConsumerDetails(consumer));
    traceDetails.put("subscription", getSubscriptionDetails(consumer.getSubscription()));
    traceDetails.put("metadata", metadata);
    traceDetails.put("brokerUrl", cnx.getBrokerService().getPulsar().getBrokerServiceUrl());

    trace(EventCategory.CONS, EventSubCategory.CREATED, traceDetails);
  }

  public void consumerClosed(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
    if (!jmsTracingEventCategory.contains(EventCategory.CONS)) return;
    if (!traceSystemTopics && consumer.getSubscription().getTopic().isSystemTopic()) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("consumer", getConsumerDetails(consumer));
    traceDetails.put("subscription", getSubscriptionDetails(consumer.getSubscription()));
    traceDetails.put("metadata", metadata);
    traceDetails.put("brokerUrl", cnx.getBrokerService().getPulsar().getBrokerServiceUrl());

    ConsumerStatsImpl stats = consumer.getStats();
    Map<String, Object> statsTrace = new TreeMap<>();
    statsTrace.put("connectedSince", stats.getConnectedSince());
    statsTrace.put("closedAt", DateFormatter.now());
    statsTrace.put("averageMsgSize", stats.getAvgMessagesPerEntry());
    statsTrace.put("msgRateOut", stats.getMsgRateOut());
    statsTrace.put("msgThroughputOut", stats.getMsgThroughputOut());
    statsTrace.put("msgOutCounter", stats.getMsgOutCounter());
    statsTrace.put("bytesOutCounter", stats.getBytesOutCounter());
    statsTrace.put("unackedMessages", stats.getUnackedMessages());
    statsTrace.put("messageAckRate", stats.getMessageAckRate());
    statsTrace.put("msgRateRedeliver", stats.getMsgRateRedeliver());
    statsTrace.put("readPositionWhenJoining", stats.getReadPositionWhenJoining());
    Subscription sub = consumer.getSubscription();
    if (sub != null) {
      statsTrace.put("subscriptionApproxBacklog", sub.getNumberOfEntriesInBacklog(false));
      statsTrace.put("subscriptionMsgRateExpired", sub.getExpiredMessageRate());
    }
    traceDetails.put("stats", statsTrace);

    trace(EventCategory.CONS, EventSubCategory.CLOSED, traceDetails);
  }

  /* ***************************
   **  Message events
   ******************************/

  public void onMessagePublish(
      Producer producer, ByteBuf headersAndPayload, Topic.PublishContext publishContext) {
    if (!jmsTracingEventCategory.contains(EventCategory.MSG)) return;

    TraceLevel level = getTracingLevel(producer);
    if (level == TraceLevel.OFF) return;

    Map<String, Object> traceDetails = new TreeMap<>();

    addMinimumProducerDetails(producer, traceDetails);

    traceDetails.put("publishContext", getPublishContextDetails(level, publishContext));

    if (TraceLevel.PAYLOAD == level && headersAndPayload != null) {
      Map<String, Object> headersAndPayloadDetails = new TreeMap<>();
      traceMetadataAndPayload(
          "payload",
          headersAndPayload.retainedDuplicate(),
          headersAndPayloadDetails,
          maxPayloadLength);
      traceDetails.put("headersAndPayload", headersAndPayloadDetails);
    }

    trace(EventCategory.MSG, EventSubCategory.PRODUCED, traceDetails);
  }

  public void messageProduced(
      ServerCnx cnx,
      Producer producer,
      long startTimeNs,
      long ledgerId,
      long entryId,
      Topic.PublishContext publishContext) {
    if (!jmsTracingEventCategory.contains(EventCategory.MSG)) return;

    TraceLevel level = getTracingLevel(producer);
    if (level == TraceLevel.OFF) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    addMinimumProducerDetails(producer, traceDetails);

    traceDetails.put("publishContext", getPublishContextDetails(level, publishContext));
    traceDetails.put("messageId", ledgerId + ":" + entryId);
    trace(EventCategory.MSG, EventSubCategory.STORED, traceDetails);
  }

  public void beforeSendMessage(
      Subscription subscription,
      Entry entry,
      long[] ackSet,
      MessageMetadata msgMetadata,
      Consumer consumer) {
    if (!jmsTracingEventCategory.contains(EventCategory.MSG)) return;

    TraceLevel level = getTracingLevel(subscription);
    if (level == TraceLevel.OFF) return;

    Map<String, Object> traceDetails = new TreeMap<>();

    addMinimumConsumerSubscriptionDetails(consumer, subscription, traceDetails);

    traceDetails.put("messageId", entry.getLedgerId() + ":" + entry.getEntryId());

    traceDetails.put("headersAndPayload", getEntryDetails(level, entry, maxPayloadLength));

    trace(EventCategory.MSG, EventSubCategory.READ, traceDetails);
  }

  public void messageDispatched(
      ServerCnx cnx, Consumer consumer, long ledgerId, long entryId, ByteBuf headersAndPayload) {
    if (!jmsTracingEventCategory.contains(EventCategory.MSG)) return;

    TraceLevel level = getTracingLevel(consumer);
    if (level == TraceLevel.OFF) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    addMinimumConsumerSubscriptionDetails(consumer, traceDetails);
    traceDetails.put("messageId", ledgerId + ":" + entryId);

    if (TraceLevel.PAYLOAD == level && headersAndPayload != null) {
      Map<String, Object> headersAndPayloadDetails = new TreeMap<>();
      traceMetadataAndPayload(
          "payload",
          headersAndPayload.retainedDuplicate(),
          headersAndPayloadDetails,
          maxPayloadLength);
      traceDetails.put("headersAndPayload", headersAndPayloadDetails);
    }

    trace(EventCategory.MSG, EventSubCategory.DISPATCHED, traceDetails);
  }

  public void messageAcked(ServerCnx cnx, Consumer consumer, CommandAck ackCmd) {
    if (!jmsTracingEventCategory.contains(EventCategory.MSG)) return;

    TraceLevel level = getTracingLevel(consumer);
    if (consumer != null && level == TraceLevel.OFF) return;

    Map<String, Object> traceDetails = new TreeMap<>();

    addMinimumConsumerSubscriptionDetails(consumer, traceDetails);

    Map<String, Object> ackDetails = new TreeMap<>();
    if (ackCmd.hasAckType()) {
      ackDetails.put("type", ackCmd.getAckType().name());
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

    EventSubCategory subcategory =
        consumer == null ? EventSubCategory.FILTERED : EventSubCategory.ACKED;

    trace(EventCategory.MSG, subcategory, traceDetails);
  }

  /* ***************************
   **  Transaction events
   ******************************/

  public void txnOpened(long tcId, String txnID) {
    if (!jmsTracingEventCategory.contains(EventCategory.TX)) return;

    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("tcId", tcId); // transaction coordinator id
    traceDetails.put("txnID", txnID);

    trace(EventCategory.TX, EventSubCategory.OPENED, traceDetails);
  }

  public void txnEnded(String txnID, long txnAction) {
    if (!jmsTracingEventCategory.contains(EventCategory.TX)) return;

    final EventSubCategory subcategory;
    Map<String, Object> traceDetails = new TreeMap<>();
    traceDetails.put("txnID", txnID);

    TxnAction action = TxnAction.valueOf((int) txnAction);
    if (action == null) {
      subcategory = EventSubCategory.CLOSED;
      traceDetails.put("txnAction", "unknown action code " + txnAction);
    } else {
      traceDetails.put("txnAction", action.name());
      switch (action) {
        case COMMIT:
          subcategory = EventSubCategory.COMMITTED;
          break;
        case ABORT:
          subcategory = EventSubCategory.ABORTED;
          break;
        default:
          subcategory = EventSubCategory.CLOSED;
          traceDetails.put("txnAction", "unknown action code " + txnAction + " " + action.name());
          break;
      }
    }

    trace(EventCategory.TX, subcategory, traceDetails);
  }

  /* ***************************
   **  Servlet events
   ******************************/

  public void onWebserviceResponse(ServletRequest request, ServletResponse response) {
    if (!jmsTracingEventCategory.contains(EventCategory.REST)) return;

    Map<String, Object> traceDetails = new TreeMap<>();

    traceDetails.put("host", hostNameOf(request.getRemoteHost(), request.getRemotePort()));
    traceDetails.put("protocol", request.getProtocol());
    traceDetails.put("scheme", request.getScheme());

    // todo: log POST payload?
    try {
      HttpServletRequest req = (HttpServletRequest) FieldUtils.readField(request, "request", true);
      traceDetails.put("method", req.getMethod());
      traceDetails.put("uri", req.getRequestURI());
      if (req.getQueryString() != null) {
        traceDetails.put("queryString", req.getQueryString());
      }
      traceDetails.put("authType", req.getAuthType());
      traceDetails.put("remoteUser", req.getRemoteUser());

      HttpServletResponse resp = (HttpServletResponse) response;
      traceDetails.put("status", resp.getStatus());
    } catch (Throwable t) {
      log.error("Error getting request details", t);
    }

    trace(EventCategory.REST, EventSubCategory.CALLED, traceDetails);
  }

  /* ***************************
   **  Skipped
   ******************************/

  public void onPulsarCommand(BaseCommand command, ServerCnx cnx) {
    // skipping, output is not useful
  }

  public void onWebserviceRequest(ServletRequest request) {
    // skipping, it is the same as onWebserviceResponse
    // but without response status.
  }

  // public void onFilter(ServletRequest request, ServletResponse response, FilterChain chain)
}
