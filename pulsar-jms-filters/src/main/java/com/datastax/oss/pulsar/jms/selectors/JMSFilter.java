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

import io.netty.buffer.ByteBuf;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Histogram;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.jms.Destination;
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

  private final ConcurrentHashMap<String, SelectorSupport> selectors = new ConcurrentHashMap<>();
  private static final Histogram filterProcessingTime =
      Histogram.build()
          .name("pulsar_jmsfilter_processingtime_ondispatch")
          .help(
              "Time taken to compute filters on the broker while dispatching messages to consumers")
          .labelNames("topic", "subscription")
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
      return filterEntry(entry, context, false);
    } finally {
      filterProcessingTime
          .labels(context.getSubscription().getTopicName(), context.getSubscription().getName())
          .observe(System.nanoTime() - start);
    }
  }

  public FilterResult filterEntry(Entry entry, FilterContext context, boolean onMessagePublish) {
    Consumer consumer = context.getConsumer();
    Map<String, String> consumerMetadata =
        consumer != null ? consumer.getMetadata() : Collections.emptyMap();
    Subscription subscription = context.getSubscription();
    boolean jmsFiltering = "true".equals(consumerMetadata.get("jms.filtering"));
    final String jmsSelectorOnSubscription;
    if (subscription instanceof PersistentSubscription) {
      // in 2.10 only PersistentSubscription has getSubscriptionProperties method
      Map<String, String> subscriptionProperties =
          ((PersistentSubscription) subscription).getSubscriptionProperties();
      if (subscriptionProperties != null) {
        jmsSelectorOnSubscription = subscriptionProperties.getOrDefault("jms.selector", "");
        jmsFiltering = jmsFiltering || "true".equals(subscriptionProperties.get("jms.filtering"));
      } else {
        jmsSelectorOnSubscription = "";
      }
    } else {
      jmsSelectorOnSubscription = "";
    }
    if (!jmsFiltering) {
      return FilterResult.ACCEPT;
    }
    String topicName = context.getSubscription().getTopicName();
    String jmsSelector = consumerMetadata.getOrDefault("jms.selector", "");
    String destinationTypeForTheClient = consumerMetadata.get("jms.destination.type");
    String jmsSelectorRejectAction = consumerMetadata.get("jms.selector.reject.action");
    // noLocal filter
    String filterJMSConnectionID = consumerMetadata.getOrDefault("jms.filter.JMSConnectionID", "");
    boolean forceDropRejected =
        "true".equals(consumerMetadata.getOrDefault("jms.force.drop.rejected", "false"));
    final FilterResult rejectResultForSelector;
    if ("drop".equals(jmsSelectorRejectAction) || forceDropRejected) {
      // this is the common behaviour for a Topics
      // or happens for Queues with jms.acknowledgeRejectedMessages=true
      rejectResultForSelector = FilterResult.REJECT;
    } else {
      // this is the common behaviour for a Queue
      rejectResultForSelector = FilterResult.RESCHEDULE;
    }
    MessageMetadata metadata = context.getMsgMetadata();
    if (metadata.hasMarkerType()) {
      // special messages...ignore
      return FilterResult.ACCEPT;
    }
    SelectorSupport selector =
        selectors.computeIfAbsent(
            jmsSelector,
            s -> {
              try {
                return SelectorSupport.build(s, !jmsSelector.isEmpty());
              } catch (JMSException err) {
                log.error("Cannot build selector from '{}'", jmsSelector, err);
                return null;
              }
            });
    if (selector == null && !jmsSelector.isEmpty()) {
      // error creating the selector. try again later
      return FilterResult.RESCHEDULE;
    }

    SelectorSupport selectorOnSubscription =
        selectors.computeIfAbsent(
            jmsSelectorOnSubscription,
            s -> {
              try {
                return SelectorSupport.build(s, !jmsSelectorOnSubscription.isEmpty());
              } catch (JMSException err) {
                log.error(
                    "Cannot build subscription selector from '{}'", jmsSelectorOnSubscription, err);
                return null;
              }
            });
    if (selectorOnSubscription == null && !jmsSelectorOnSubscription.isEmpty()) {
      // error creating the selector. try again later
      return FilterResult.RESCHEDULE;
    }

    CommandSubscribe.SubType subType = subscription.getType();
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

    try {
      if (metadata.hasNumMessagesInBatch()) {

        // this is batch message
        // we can reject/reschedule it only if all the messages are to be rejects
        // we must accept it if at least one message passes the filters

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
          boolean allFilteredBySubscriptionFilter = !jmsSelectorOnSubscription.isEmpty();
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
                      destinationTypeForTheClient,
                      topicName,
                      singleMessageMetadata,
                      null);
              // noLocal filter
              // all the messages in the batch come from the Producer/Connection
              // so we can reject the whole batch immediately at the first entry
              if (!filterJMSConnectionID.isEmpty()
                  && filterJMSConnectionID.equals(typedProperties.apply("JMSConnectionID"))) {
                if (isExclusive || forceDropRejected) {
                  return FilterResult.REJECT;
                } else {
                  return FilterResult.RESCHEDULE;
                }
              }

              // timeToLive filter
              long jmsExpiration = getJMSExpiration(typedProperties);
              if (jmsExpiration > 0 && System.currentTimeMillis() > jmsExpiration) {
                // we are going to send the batch to the client
                // in this case, this way the client
                // can discard it
              } else {
                allExpired = false;
              }

              boolean matches = true;

              if (selector != null) {
                matches = matches(typedProperties, selector);
              }

              if (!jmsSelectorOnSubscription.isEmpty()) {
                boolean matchesSubscriptionFilter =
                    matches(typedProperties, selectorOnSubscription);
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
          if (allFilteredBySubscriptionFilter && !jmsSelectorOnSubscription.isEmpty()) {
            return FilterResult.REJECT;
          }
          if (oneAccepted) {
            return FilterResult.ACCEPT;
          }
          return rejectResultForSelector;
        } finally {
          uncompressedPayload.release();
        }
      } else {

        // here we are dealing with a single message,
        // so we can reject the message more easily
        PropertyEvaluator typedProperties =
            new PropertyEvaluator(
                metadata.getPropertiesCount(),
                metadata.getPropertiesList(),
                destinationTypeForTheClient,
                topicName,
                null,
                metadata);

        if (!jmsSelectorOnSubscription.isEmpty()) {
          boolean matchesSubscriptionFilter = matches(typedProperties, selectorOnSubscription);
          // the subscription filter always deletes the messages
          if (!matchesSubscriptionFilter) {
            return FilterResult.REJECT;
          }
        }

        // it is expensive to extract the message properties
        // and it is very unlikely that onPublish we are accepting a message that is already expired
        if (!onMessagePublish) {
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

        if (!filterJMSConnectionID.isEmpty()
            && filterJMSConnectionID.equals(typedProperties.apply("JMSConnectionID"))) {
          if (isExclusive || forceDropRejected) {
            return FilterResult.REJECT;
          } else {
            return FilterResult.RESCHEDULE;
          }
        }

        if (matches) {
          return FilterResult.ACCEPT;
        }
        return rejectResultForSelector;
      }

    } catch (Throwable err) {
      log.error("Error while processing entry " + err, err);
      // also print on stdout:
      err.printStackTrace(System.out);
      return FilterResult.REJECT;
    }
  }

  @AllArgsConstructor
  private static class PropertyEvaluator implements Function<String, Object> {
    private int propertiesCount;
    private List<KeyValue> propertiesList;
    private String destinationTypeForTheClient;
    private String topicName;
    private SingleMessageMetadata singleMessageMetadata;
    private MessageMetadata metadata;

    private Object getProperty(String name) {
      return JMSFilter.getProperty(propertiesCount, propertiesList, name);
    }

    @Override
    public Object apply(String name) {
      switch (name) {
        case "JMSReplyTo":
          {
            String _jmsReplyTo = safeString(getProperty("JMSReplyTo"));
            Destination jmsReplyTo = null;
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
    Object value = typedProperties.apply("JMSExpiration");
    if (value != null) {
      try {
        jmsExpiration = Long.parseLong(value + "");
      } catch (NumberFormatException err) {
        // cannot decode JMSExpiration
      }
    }
    return jmsExpiration;
  }

  private static Object getProperty(
      int propertiesCount, List<KeyValue> propertiesList, String name) {
    if (propertiesCount <= 0) {
      return null;
    }
    String type = null;
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
