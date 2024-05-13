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

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.jms.JMSException;
import javax.jms.Message;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
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
  private static final double MS_IN_NANOS = 1_000_000;
  static final double[] BUCKETS =
      new double[] {
        1 * MS_IN_NANOS,
        3 * MS_IN_NANOS,
        5 * MS_IN_NANOS,
        10 * MS_IN_NANOS,
        20 * MS_IN_NANOS,
        50 * MS_IN_NANOS,
        100 * MS_IN_NANOS,
        200 * MS_IN_NANOS,
        500 * MS_IN_NANOS,
        1000 * MS_IN_NANOS,
        2000 * MS_IN_NANOS,
        5000 * MS_IN_NANOS,
        10000 * MS_IN_NANOS
      };

  // for these properties the type is not written
  private static final Map<String, String> SYSTEM_PROPERTIES_TYPES =
      ImmutableMap.of(
          "JMSPriority",
          "int",
          "JMSExpiration",
          "long",
          "JMSXDeliveryCount",
          "int",
          "JMSDeliveryTime",
          "long",
          "JMSType",
          "string",
          "JMSDeliveryMode",
          "int",
          "JMSMessageId",
          "string",
          "JMSXGroupSeq",
          "long");
  private final ConcurrentHashMap<String, SelectorSupport> selectors = new ConcurrentHashMap<>();
  private Boolean handleJMSExpiration = null;
  private Boolean handleOnlySelectors = null;

  private static final Histogram filterProcessingTime =
      Histogram.build()
          .name("pulsar_jmsfilter_processingtime_ondispatch")
          .help(
              "Time taken to compute filters on the broker while dispatching messages to consumers")
          .labelNames("topic", "subscription")
          .buckets(BUCKETS)
          .create();

  private static final AtomicBoolean metricRegistered = new AtomicBoolean(false);

  JMSFilter(boolean registerMetrics) {
    if (registerMetrics) {
      if (metricRegistered.compareAndSet(false, true)) {
        log.info("Registering JMSFilter metrics");
        try {
          CollectorRegistry.defaultRegistry.register(filterProcessingTime);
        } catch (IllegalArgumentException alreadyRegistered) {
          // this happens in Pulsar 2.10, because each JMSFilter is created in a different
          // classloader
          // so the metricRegistered flag doesn't help
          log.debug("JMSFilter metrics already registered");
        }
      }
    }
  }

  public JMSFilter() {
    // Pulsar 3.x created multiple instances of the filter, one per Dispatcher
    this(true);
  }

  @Override
  public FilterResult filterEntry(Entry entry, FilterContext context) {
    long start = System.nanoTime();
    try {
      return filterEntry(entry, context, false, null);
    } finally {
      filterProcessingTime
          .labels(context.getSubscription().getTopicName(), context.getSubscription().getName())
          .observe(System.nanoTime() - start);
    }
  }

  private boolean isHandleJMSExpiration(FilterContext context) {
    if (handleJMSExpiration != null) {
      return handleJMSExpiration;
    }
    boolean handleJMSExpiration =
        Boolean.parseBoolean(
            context
                    .getSubscription()
                    .getTopic()
                    .getBrokerService()
                    .getPulsar()
                    .getConfiguration()
                    .getProperty("jmsProcessJMSExpiration")
                + "");
    if (handleJMSExpiration) {
      boolean isHandleOnlySelectors = isHandleOnlySelectors(context);
      if (isHandleOnlySelectors) {
        handleJMSExpiration = false;
      } else {
        log.info(
            "jmsProcessJMSExpiration=true JMSExpiration will be handled by the broker and by the client");
      }
    } else {
      log.info("jmsProcessJMSExpiration=false JMSExpiration will be handled only by the client");
    }
    this.handleJMSExpiration = handleJMSExpiration;
    return handleJMSExpiration;
  }

  private boolean isHandleOnlySelectors(FilterContext context) {
    if (handleOnlySelectors != null) {
      return handleOnlySelectors;
    }
    Object mode =
        context
            .getSubscription()
            .getTopic()
            .getBrokerService()
            .getPulsar()
            .getConfiguration()
            .getProperty("jmsProcessingMode");
    if (mode == null) {
      mode = "selectors-only";
    }
    boolean handleOnlySelectors;
    switch (mode.toString()) {
      case "full":
        handleOnlySelectors = false;
        break;
      case "selectors-only":
        handleOnlySelectors = true;
        break;
      default:
        throw new IllegalArgumentException(
            "Invalid jmsProcessingMode: "
                + mode
                + " only 'full' or 'selectors-only' are supported");
    }
    this.handleOnlySelectors = handleOnlySelectors;
    if (handleOnlySelectors) {
      log.info(
          "jmsProcessingMode={} The broker will process only selectors and not other JMS features (like JMSExpiration, noLocal)",
          mode);
    } else {
      log.info(
          "jmsProcessingMode={} The broker will process all the JMS features (like JMSExpiration, noLocal) and the selectors",
          mode);
    }
    return handleOnlySelectors;
  }

  public FilterResult filterEntry(
      Entry entry,
      FilterContext context,
      boolean onMessagePublish,
      MessageMetadataCache messageMetadataCache) {
    Consumer consumer = context.getConsumer();
    Map<String, String> consumerMetadata =
        consumer != null ? consumer.getMetadata() : Collections.emptyMap();
    Subscription subscription = context.getSubscription();

    final String jmsSelectorOnSubscription = getJmsSelectorOnSubscription(subscription);

    // jms.filtering on the consumer metadata means that the client asked for server side filtering
    // (and it assumes that the broker is going to do it)
    boolean jmsFiltering =
        !jmsSelectorOnSubscription.isEmpty()
            || "true".equals(consumerMetadata.get("jms.filtering"));
    if (!jmsFiltering) {
      return FilterResult.ACCEPT;
    }

    String jmsSelector = consumerMetadata.getOrDefault("jms.selector", "");
    boolean onlySelectors = onMessagePublish || isHandleOnlySelectors(context);

    if (onlySelectors) {
      // we are using the server side filters only for selectors
      // but there is no selector configured here
      // even if the client asked for "server side filtering" we are not going to do any
      // processing
      if (jmsSelectorOnSubscription.isEmpty() && jmsSelector.isEmpty()) {
        return FilterResult.ACCEPT;
      }
    }

    MessageMetadata metadata = context.getMsgMetadata();
    if (metadata.hasMarkerType()) {
      // special messages...ignore
      return FilterResult.ACCEPT;
    }

    SelectorSupport selector = parseSelector(jmsSelector);
    if (selector == null && !jmsSelector.isEmpty()) {
      // error creating the selector. try again later
      return FilterResult.RESCHEDULE;
    }

    SelectorSupport selectorOnSubscription = parseSelector(jmsSelectorOnSubscription);
    if (selectorOnSubscription == null && !jmsSelectorOnSubscription.isEmpty()) {
      // error creating the selector. try again later
      return FilterResult.RESCHEDULE;
    }

    try {
      if (metadata.hasNumMessagesInBatch()) {
        if (onMessagePublish) {
          // we cannot process the batch message on publish
          return FilterResult.ACCEPT;
        }
        // this is batch message
        // we can reject/reschedule it only if all the messages are to be rejects
        // we must accept it if at least one message passes the filters
        return processBatchEntry(
            entry,
            context,
            metadata,
            subscription,
            consumerMetadata,
            selector,
            selectorOnSubscription);
      } else {
        return processSingleMessageEntry(
            context,
            onMessagePublish,
            metadata,
            selectorOnSubscription,
            selector,
            subscription,
            consumerMetadata,
            messageMetadataCache);
      }
    } catch (Throwable err) {
      log.error("Error while processing entry " + err, err);
      // also print on stdout:
      err.printStackTrace(System.out);
      return FilterResult.REJECT;
    }
  }

  private static String getJmsSelectorOnSubscription(Subscription subscription) {
    if (!(subscription instanceof PersistentSubscription)) {
      return "";
    }
    // in 2.10 only PersistentSubscription has getSubscriptionProperties method
    Map<String, String> subscriptionProperties =
        ((PersistentSubscription) subscription).getSubscriptionProperties();
    if (subscriptionProperties == null) {
      return "";
    }
    if (!("true".equals(subscriptionProperties.get("jms.filtering")))) {
      return "";
    }
    return subscriptionProperties.getOrDefault("jms.selector", "");
  }

  private SelectorSupport parseSelector(String jmsSelector) {
    return selectors.computeIfAbsent(
        jmsSelector,
        s -> {
          try {
            return SelectorSupport.build(s, !s.isEmpty());
          } catch (JMSException err) {
            log.error("Cannot parse selector {}", s, err);
            return null;
          }
        });
  }

  private FilterResult processSingleMessageEntry(
      FilterContext context,
      boolean onMessagePublish,
      MessageMetadata metadata,
      SelectorSupport selectorOnSubscription,
      SelectorSupport selector,
      Subscription subscription,
      Map<String, String> consumerMetadata,
      MessageMetadataCache messageMetadataCache)
      throws JMSException {
    // here we are dealing with a single message,
    // so we can reject the message more easily
    PropertyEvaluator typedProperties =
        new PropertyEvaluator(
            metadata.getPropertiesCount(),
            metadata.getPropertiesList(),
            null,
            metadata,
            context,
            messageMetadataCache);

    if (selectorOnSubscription != null) {
      boolean matchesSubscriptionFilter = matches(typedProperties, selectorOnSubscription);
      // the subscription filter always deletes the messages
      if (!matchesSubscriptionFilter) {
        return FilterResult.REJECT;
      }
    }

    // it is expensive to extract the message properties that are not usually set in the message
    // because you need to scan the list of properties
    // and it is very unlikely that onPublish we are accepting a message that is already expired
    if (!onMessagePublish && isHandleJMSExpiration(context)) {
      // timetoLive filter
      long jmsExpiration = getJMSExpiration(typedProperties);
      if (jmsExpiration > 0 && System.currentTimeMillis() > jmsExpiration) {
        // message expired, this does not depend on the Consumer
        // we can to REJECT it immediately
        return FilterResult.REJECT;
      }
    }

    boolean matches = true;
    if (selector != null) {
      matches = matches(typedProperties, selector);
    }

    if (!isHandleOnlySelectors(context)) {
      // noLocal filter
      String filterJMSConnectionID =
          consumerMetadata.getOrDefault("jms.filter.JMSConnectionID", "");
      FilterResult filterConnectionIdResult =
          handleConnectionIdFilter(
              filterJMSConnectionID, typedProperties, subscription, consumerMetadata);
      if (filterConnectionIdResult != null) {
        return filterConnectionIdResult;
      }
    }

    if (matches) {
      return FilterResult.ACCEPT;
    }

    final FilterResult rejectResultForSelector = getRejectResultForSelector(consumerMetadata);

    return rejectResultForSelector;
  }

  private FilterResult processBatchEntry(
      Entry entry,
      FilterContext context,
      MessageMetadata metadata,
      Subscription subscription,
      Map<String, String> consumerMetadata,
      SelectorSupport selector,
      SelectorSupport selectorOnSubscription)
      throws IOException, JMSException {
    // noLocal filter
    String filterJMSConnectionID = consumerMetadata.getOrDefault("jms.filter.JMSConnectionID", "");
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
      boolean allFilteredBySubscriptionFilter = selectorOnSubscription != null;
      for (int i = 0; i < numMessages; i++) {
        final SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
        final ByteBuf singleMessagePayload =
            Commands.deSerializeSingleMessageInBatch(
                uncompressedPayload, singleMessageMetadata, i, numMessages);
        try {
          PropertyEvaluator typedProperties =
              new PropertyEvaluator(
                  singleMessageMetadata.getPropertiesCount(),
                  singleMessageMetadata.getPropertiesList(),
                  singleMessageMetadata,
                  null,
                  context,
                  null);

          // noLocal filter
          // all the messages in the batch come from the Producer/Connection
          // so we can reject the whole batch immediately at the first entry
          FilterResult filterConnectionIdResult =
              handleConnectionIdFilter(
                  filterJMSConnectionID, typedProperties, subscription, consumerMetadata);
          if (filterConnectionIdResult != null) {
            return filterConnectionIdResult;
          }

          // it is expensive to extract the message properties that are not usually set in the
          // message
          // because you need to scan the list of properties
          if (isHandleJMSExpiration(context)) {
            // timeToLive filter
            long jmsExpiration = getJMSExpiration(typedProperties);
            if (jmsExpiration > 0 && System.currentTimeMillis() > jmsExpiration) {
              // we are going to send the batch to the client
              // in this case, this way the client
              // can discard it
            } else {
              allExpired = false;
            }
          } else {
            allExpired = false;
          }

          boolean matches = true;

          if (selector != null) {
            matches = matches(typedProperties, selector);
          }

          if (selectorOnSubscription != null) {
            boolean matchesSubscriptionFilter = matches(typedProperties, selectorOnSubscription);
            matches = matches && matchesSubscriptionFilter;
            if (matchesSubscriptionFilter) {
              allFilteredBySubscriptionFilter = false;
            }
          }

          oneAccepted = oneAccepted || matches;
        } finally {
          singleMessagePayload.release();
        }
      }
      if (allExpired) {
        return FilterResult.REJECT;
      }
      if (allFilteredBySubscriptionFilter && selectorOnSubscription != null) {
        return FilterResult.REJECT;
      }
      if (oneAccepted) {
        return FilterResult.ACCEPT;
      }
      final FilterResult rejectResultForSelector = getRejectResultForSelector(consumerMetadata);
      return rejectResultForSelector;
    } finally {
      uncompressedPayload.release();
    }
  }

  private static FilterResult handleConnectionIdFilter(
      String filterJMSConnectionID,
      Function<String, Object> typedProperties,
      Subscription subscription,
      Map<String, String> consumerMetadata) {
    if (!filterJMSConnectionID.isEmpty()
        && filterJMSConnectionID.equals(typedProperties.apply("JMSConnectionID"))) {
      CommandSubscribe.SubType subType = subscription.getType();
      final boolean isExclusive = isExclusiveSubscriptionType(subType);
      boolean forceDropRejected = isForceDropRejected(consumerMetadata);
      if (isExclusive || forceDropRejected) {
        return FilterResult.REJECT;
      } else {
        return FilterResult.RESCHEDULE;
      }
    }
    return null;
  }

  private static boolean isForceDropRejected(Map<String, String> consumerMetadata) {
    boolean forceDropRejected =
        "true".equals(consumerMetadata.getOrDefault("jms.force.drop.rejected", "false"));
    return forceDropRejected;
  }

  private static FilterResult getRejectResultForSelector(Map<String, String> consumerMetadata) {
    boolean forceDropRejected = isForceDropRejected(consumerMetadata);
    String jmsSelectorRejectAction = consumerMetadata.get("jms.selector.reject.action");
    final FilterResult rejectResultForSelector;
    if ("drop".equals(jmsSelectorRejectAction) || forceDropRejected) {
      // this is the common behaviour for a Topics
      // or happens for Queues with jms.acknowledgeRejectedMessages=true
      rejectResultForSelector = FilterResult.REJECT;
    } else {
      // this is the common behaviour for a Queue
      rejectResultForSelector = FilterResult.RESCHEDULE;
    }
    return rejectResultForSelector;
  }

  private static boolean isExclusiveSubscriptionType(CommandSubscribe.SubType subType) {
    if (subType == null) {
      // this is possible during backlog calculation if the dispatcher has not
      // been created yet, so no consumer ever connected and the type of
      // subscription is not known yet
      subType = CommandSubscribe.SubType.Shared;
    }
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
    return isExclusive;
  }

  @AllArgsConstructor
  private static class PropertyEvaluator implements Function<String, Object> {
    private int propertiesCount;
    private List<KeyValue> propertiesList;
    private SingleMessageMetadata singleMessageMetadata;
    private MessageMetadata metadata;
    private FilterContext context;
    private MessageMetadataCache messageMetadataCache;

    private Object getProperty(String name) {
      if (messageMetadataCache != null) {
        return messageMetadataCache.getProperty(
            name, n -> JMSFilter.getProperty(propertiesCount, propertiesList, n));
      }
      return JMSFilter.getProperty(propertiesCount, propertiesList, name);
    }

    @Override
    public Object apply(String name) {
      switch (name) {
        case "JMSReplyTo":
          {
            String _jmsReplyTo = safeString(getProperty("JMSReplyTo"));
            if (_jmsReplyTo != null) {
              String jmsReplyToType = getProperty("JMSReplyToType") + "";
              switch (jmsReplyToType) {
                case "topic":
                  return new ActiveMQTopic(_jmsReplyTo);
                default:
                  return new ActiveMQQueue(_jmsReplyTo);
              }
            } else {
              return null;
            }
          }
        case "JMSDestination":
          {
            Map<String, String> consumerMetadata =
                context.getConsumer() != null ? context.getConsumer().getMetadata() : null;
            String destinationTypeForTheClient =
                consumerMetadata != null ? consumerMetadata.get("jms.destination.type") : null;

            String topicName = context.getSubscription().getTopicName();
            return "queue".equalsIgnoreCase(destinationTypeForTheClient)
                ? new ActiveMQQueue(topicName)
                : new ActiveMQTopic(topicName);
          }
        case "JMSType":
        case "JMSMessageId":
          return getProperty(name);
        case "JMSCorrelationID":
          {
            String _correlationId = safeString(getProperty("JMSCorrelationID"));
            if (_correlationId != null) {
              return new String(Base64.getDecoder().decode(_correlationId), StandardCharsets.UTF_8);
            } else {
              return null;
            }
          }
        case "JMSPriority":
          {
            Object jmsPriorityString = getProperty("JMSPriority");
            if (jmsPriorityString != null) {
              try {
                return Integer.parseInt(jmsPriorityString + "");
              } catch (NumberFormatException err) {
                // cannot decode priority, not a big deal as it is not supported in Pulsar
                return Message.DEFAULT_PRIORITY;
              }
            } else {
              // we are not setting JMSPriority if it is the default value
              return Message.DEFAULT_PRIORITY;
            }
          }
        case "JMSDeliveryMode":
          {
            Object deliveryModeString = getProperty("JMSDeliveryMode");
            if (deliveryModeString != null) {
              try {
                return Integer.parseInt(deliveryModeString + "");
              } catch (NumberFormatException err) {
                // cannot decode deliveryMode, not a big deal as it is not supported in Pulsar
              }
            }
            return Message.DEFAULT_DELIVERY_MODE;
          }
        case "JMSTimestamp":
          {
            if (singleMessageMetadata != null) {
              if (singleMessageMetadata.hasEventTime()) {
                return singleMessageMetadata.getEventTime();
              }
            }
            if (metadata != null) {
              if (metadata.hasEventTime()) {
                return metadata.getEventTime();
              }
            }
            return 0L; // must be a long
          }
        case "JMSXDeliveryCount":
          // this is not supported on the broker
          return 0;
        case "JMSXGroupID":
          {
            if (singleMessageMetadata != null && singleMessageMetadata.hasPartitionKey()) {
              return singleMessageMetadata.getPartitionKey();
            }
            if (metadata != null && metadata.hasPartitionKey()) {
              return metadata.getPartitionKey();
            }
            return "";
          }
        case "JMSXGroupSeq":
          {
            Object rawJMSXGroupSeq = getProperty("JMSXGroupSeq");
            if (rawJMSXGroupSeq != null) {
              return rawJMSXGroupSeq;
            }
            if (singleMessageMetadata != null && singleMessageMetadata.hasSequenceId()) {
              return singleMessageMetadata.getSequenceId() + "";
            }
            if (metadata != null && metadata.hasSequenceId()) {
              return metadata.getSequenceId() + "";
            }
            return "0";
          }
        default:
          return getProperty(name);
      }
    }
  }

  private static long getJMSExpiration(Function<String, Object> typedProperties) {
    long jmsExpiration = 0;
    try {
      Object value = typedProperties.apply("JMSExpiration");
      if (value != null) {
        if (value instanceof Long) {
          // getProperty already decoded the value
          return ((Long) value);
        }
        jmsExpiration = Long.parseLong(value + "");
      }
    } catch (NumberFormatException err) {
      // cannot decode JMSExpiration
    }
    return jmsExpiration;
  }

  private static Object getProperty(
      int propertiesCount, List<KeyValue> propertiesList, String name) {
    if (propertiesCount <= 0) {
      return null;
    }
    // we don't write the type for system fields
    // we pre-compute the type in order to avoid to scan the list to fine the type
    String type = SYSTEM_PROPERTIES_TYPES.get(name);
    String value = null;
    String typeProperty = propertyType(name);
    for (KeyValue keyValue : propertiesList) {
      String key = keyValue.getKey();
      if (key.equals(typeProperty)) {
        type = keyValue.getValue();
      } else if (key.equals(name)) {
        value = keyValue.getValue();
      }
      if (type != null && value != null) {
        break;
      }
    }
    return getObjectProperty(value, type);
  }

  @Override
  public void close() {
    selectors.clear();
  }

  private static String propertyType(String name) {
    // this is a typo, but we cannot change it as it would break the compatibility
    return name + "_jsmtype";
  }

  private static Object getObjectProperty(String value, String type) {
    if (value == null) {
      return null;
    }
    if (type == null) {
      // strings
      return value;
    }
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

  private static boolean matches(Function<String, Object> typedProperties, SelectorSupport selector)
      throws JMSException {

    return selector.matches(typedProperties);
  }
}
