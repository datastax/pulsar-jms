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
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
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
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.intercept.InterceptException;

@Slf4j
public class BrokerTracing implements BrokerInterceptor {

  private static final LoadingCache<PulsarService, Set<EventReasons>> jmsTracingEventList =
      CacheBuilder.newBuilder()
          .maximumSize(10)
          .expireAfterWrite(1, java.util.concurrent.TimeUnit.MINUTES)
          .build(
              new CacheLoader<PulsarService, Set<EventReasons>>() {
                public Set<EventReasons> load(PulsarService pulsar) {
                  // dynamic config can get reloaded without service restart
                  String events =
                      pulsar
                          .getConfiguration()
                          .getProperties()
                          .getProperty("jmsTracingEventList", "");
                  log.debug("read jmsTracingEventList: {}", events);

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
              });

  private static final LoadingCache<PulsarService, TraceLevel> traceLevelForService =
      CacheBuilder.newBuilder()
          .maximumSize(10)
          .expireAfterWrite(1, java.util.concurrent.TimeUnit.MINUTES)
          .build(
              new CacheLoader<PulsarService, TraceLevel>() {
                public TraceLevel load(PulsarService pulsar) {
                  String level =
                      pulsar
                          .getConfiguration()
                          .getProperties()
                          .getProperty("jmsTracingLevel", defaultTraceLevel.toString());
                  try {
                    return TraceLevel.valueOf(level);
                  } catch (IllegalArgumentException e) {
                    log.warn(
                        "Invalid tracing level: {}. Using default: {}", level, defaultTraceLevel);
                    return defaultTraceLevel;
                  }
                }
              });

  public enum EventReasons {
    ADMINISTRATIVE,
    MESSAGE,
    TRANSACTION,
    SERVLET,
  }

  private static final TraceLevel defaultTraceLevel = TraceLevel.BASIC;

  public void initialize(PulsarService pulsarService) {
    log.info("Initializing BrokerTracing");
  }

  @Override
  public void close() {
    log.info("Closing BrokerTracing");
  }

  private Set<EventReasons> getEnabledEvents(ServerCnx cnx) {
    return getEnabledEvents(cnx.getBrokerService().getPulsar());
  }

  private Set<EventReasons> getEnabledEvents(PulsarService pulsar) {
    try {
      return jmsTracingEventList.get(pulsar);
    } catch (ExecutionException e) {
      log.error("Error getting enabled events", e);
      return new HashSet<>();
    }
  }

  private static TraceLevel getTracingLevel(ServerCnx cnx) {

    try {
      return traceLevelForService.get(cnx.getBrokerService().getPulsar());
    } catch (ExecutionException e) {
      log.error("Error getting tracing level", e);
      return defaultTraceLevel;
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