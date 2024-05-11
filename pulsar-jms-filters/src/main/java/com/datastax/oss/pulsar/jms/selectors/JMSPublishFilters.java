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

import static com.datastax.oss.pulsar.jms.selectors.JMSFilter.BUCKETS;
import static org.apache.pulsar.common.protocol.Commands.skipBrokerEntryMetadataIfExist;
import static org.apache.pulsar.common.protocol.Commands.skipChecksumIfPresent;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.intercept.InterceptException;

@Slf4j
public class JMSPublishFilters implements BrokerInterceptor {
  private static final String JMS_FILTER_METADATA = "jms-msg-metadata";
  private static final ByteBuf COULDNOT_ACQUIRE_MEMORY_PLACEHOLDER = Unpooled.EMPTY_BUFFER;

  private static final Histogram filterProcessingTimeOnPublish =
      Histogram.build()
          .name("pulsar_jmsfilter_preprocessing_time_onpublish")
          .help(
              "Time taken to pre-process the message on the broker while accepting messages from producers before applying filters")
          .labelNames("topic")
          .buckets(BUCKETS)
          .create();
  private static final Histogram filterProcessingTimeOnProduce =
      Histogram.build()
          .name("pulsar_jmsfilter_processing_time_onpublish")
          .help(
              "Time taken to process the message on the broker while accepting messages from producers and applying filters")
          .labelNames("topic", "subscription")
          .buckets(BUCKETS)
          .create();

  private static final Gauge memoryUsed =
      Gauge.build()
          .name("pulsar_jmsfilter_processing_memory")
          .help("Current memory held by the JMSPublishFilters interceptor")
          .create();

  private static final Gauge pendingOperations =
      Gauge.build()
          .name("pulsar_jmsfilter_processing_pending_operations")
          .help("Number of pending operations in the JMSPublishFilters interceptor")
          .create();

  private final JMSFilter filter = new JMSFilter(false);
  private boolean enabled = false;
  private Semaphore memoryLimit;
  private final AtomicBoolean closed = new AtomicBoolean();
  private ExecutorService executor;

  @Override
  public void initialize(PulsarService pulsarService) {
    enabled =
        Boolean.parseBoolean(
            pulsarService
                .getConfiguration()
                .getProperties()
                .getProperty("jmsApplyFiltersOnPublish", "true"));
    log.info("jmsApplyFiltersOnPublish={}", enabled);

    int numThreads =
        Integer.parseInt(
            pulsarService
                .getConfiguration()
                .getProperties()
                .getProperty(
                    "jmsFiltersOnPublishThreads",
                    (Runtime.getRuntime().availableProcessors() * 2) + ""));
    log.info("jmsFiltersOnPublishThreads={}", numThreads);
    executor = Executors.newFixedThreadPool(numThreads);
    try {
      log.info("Registering JMSFilter metrics");
      CollectorRegistry.defaultRegistry.register(filterProcessingTimeOnPublish);
      CollectorRegistry.defaultRegistry.register(filterProcessingTimeOnProduce);
      CollectorRegistry.defaultRegistry.register(memoryUsed);
      CollectorRegistry.defaultRegistry.register(pendingOperations);
    } catch (IllegalArgumentException alreadyRegistered) {
      // ignore
      log.info("Filter metrics already registered", alreadyRegistered);
    }
    String memoryLimitString =
        pulsarService
            .getConfiguration()
            .getProperties()
            .getProperty("jmsFiltersOnPublishMaxMemoryMB", "128");

    try {
      int memoryLimitBytes = Integer.parseInt(memoryLimitString) * 1024 * 1024;
      if (memoryLimitBytes > 0) {
        memoryLimit = new Semaphore(memoryLimitBytes);
        log.info(
            "jmsFiltersOnPublishMaxMemoryMB={} ({} bytes)", memoryLimitString, memoryLimitBytes);
      } else {
        memoryLimit = null;
        log.info(
            "jmsFiltersOnPublishMaxMemoryMB={} (no cache for JMSPublishFilters)",
            memoryLimitString);
      }
    } catch (NumberFormatException e) {
      throw new RuntimeException(
          "Invalid memory limit jmsFiltersOnPublishMaxMemoryMB=" + memoryLimitString, e);
    }
  }

  @Override
  public void onMessagePublish(
      Producer producer, ByteBuf headersAndPayload, Topic.PublishContext publishContext) {
    if (!enabled) {
      return;
    }
    if (publishContext.isMarkerMessage()
        || publishContext.isChunked()
        || publishContext.getNumberOfMessages() > 1) {
      return;
    }
    long now = System.nanoTime();
    try {
      for (Subscription subscription : producer.getTopic().getSubscriptions().values()) {
        if (!(isPersistentSubscriptionWithSelector(subscription))) {
          continue;
        }
        // we must make a copy because the ByteBuf will be released
        ByteBuf messageMetadata = copyMessageMetadataAndAcquireMemory(headersAndPayload);
        if (messageMetadata != null) {
          publishContext.setProperty(JMS_FILTER_METADATA, messageMetadata);
        } else {
          publishContext.setProperty(JMS_FILTER_METADATA, COULDNOT_ACQUIRE_MEMORY_PLACEHOLDER);
        }
        // as soon as we find a good reason to apply the filters in messageProduced
        // we can exit
        return;
      }
    } finally {
      filterProcessingTimeOnPublish
          .labels(producer.getTopic().getName())
          .observe(System.nanoTime() - now);
    }
  }

  public ByteBuf copyMessageMetadataAndAcquireMemory(ByteBuf buffer) {
    if (memoryLimit == null) {
      return null;
    }
    int readerIndex = buffer.readerIndex();
    skipBrokerEntryMetadataIfExist(buffer);
    skipChecksumIfPresent(buffer);
    int metadataSize = (int) buffer.readUnsignedInt();
    // we cannot block the producer thread
    // if there is no memory available we must return null and the entry will be read before
    // applying the filters
    boolean acquired = memoryLimit.tryAcquire(metadataSize);
    if (!acquired) {
      buffer.readerIndex(readerIndex);
      return null;
    }
    // please note that Netty would probably retain more memory than this buffer
    // but this is the best approximation we can do
    memoryUsed.inc(metadataSize);
    ByteBuf copy = PooledByteBufAllocator.DEFAULT.buffer(metadataSize);
    buffer.readBytes(copy);
    buffer.readerIndex(readerIndex);
    return copy;
  }

  @Override
  public void messageProduced(
      ServerCnx cnx,
      Producer producer,
      long startTimeNs,
      long ledgerId,
      long entryId,
      Topic.PublishContext publishContext) {
    ByteBuf messageMetadataUnparsed = (ByteBuf) publishContext.getProperty(JMS_FILTER_METADATA);
    if (messageMetadataUnparsed == null) {
      return;
    }
    if (!enabled) {
      return;
    }
    int memorySize = messageMetadataUnparsed.readableBytes();
    Runnable onComplete =
        () -> {
          pendingOperations.dec();
          messageMetadataUnparsed.release();
          if (memoryLimit != null) {
            memoryLimit.release(memorySize);
          }
          memoryUsed.dec(memorySize);
        };
    List<Subscription> subscriptions = new ArrayList<>();
    Topic topic = producer.getTopic();
    topic
        .getSubscriptions()
        .forEach(
            (___, subscription) -> {
              if (!(isPersistentSubscriptionWithSelector(subscription))) {
                return;
              }
              subscriptions.add(subscription);
            });
    if (subscriptions.isEmpty()) { // this is very unlikely
      onComplete.run();
      return;
    }
    pendingOperations.inc();
    FilterAndAckMessageOperation filterAndAckMessageOperation =
        new FilterAndAckMessageOperation(
            ledgerId,
            entryId,
            (PersistentTopic) topic,
            subscriptions,
            messageMetadataUnparsed,
            onComplete);
    scheduleOnDispatchThread(filterAndAckMessageOperation);
  }

  private static boolean isPersistentSubscriptionWithSelector(Subscription subscription) {
    return subscription instanceof PersistentSubscription
        && subscription.getSubscriptionProperties().containsKey("jms.selector")
        && "true".equals(subscription.getSubscriptionProperties().get("jms.filtering"));
  }

  @AllArgsConstructor
  private class FilterAndAckMessageOperation implements Runnable {
    final long ledgerId;
    final long entryId;
    final PersistentTopic topic;
    final List<Subscription> subscriptions;
    final ByteBuf messageMetadataUnparsed;
    final Runnable onComplete;

    @Override
    public void run() {
      try {
        filterAndAckMessage(ledgerId, entryId, topic, subscriptions, messageMetadataUnparsed);
      } finally {
        onComplete.run();
      }
    }
  }

  private void filterAndAckMessage(
      long ledgerId,
      long entryId,
      PersistentTopic topic,
      List<Subscription> subscriptions,
      ByteBuf messageMetadataUnparsed) {
    if (closed.get()) {
      // the broker is shutting down, we cannot process the entries
      // this operation has been enqueued before the broker shutdown
      return;
    }
    ByteBuf entryReadFromBookie = null;
    try {
      final MessageMetadata messageMetadata;
      if (messageMetadataUnparsed == COULDNOT_ACQUIRE_MEMORY_PLACEHOLDER) {
        entryReadFromBookie = readSingleEntry(ledgerId, entryId, topic).join();
        if (entryReadFromBookie == null) {
          log.error("Could not read entry {}:{} from topic {}", ledgerId, entryId, topic);
          return;
        }
        messageMetadata = new MessageMetadata();
        skipBrokerEntryMetadataIfExist(entryReadFromBookie);
        skipChecksumIfPresent(entryReadFromBookie);
        int metadataSize = (int) entryReadFromBookie.readUnsignedInt();
        messageMetadata.parseFrom(entryReadFromBookie, metadataSize);
      } else {
        messageMetadata =
            getMessageMetadata(messageMetadataUnparsed, messageMetadataUnparsed.readableBytes());
      }
      for (Subscription subscription : subscriptions) {
        if (closed.get()) {
          // the broker is shutting down, we cannot process the entries
          // this operation has been enqueued before the broker shutdown
          return;
        }
        long now = System.nanoTime();
        try {
          FilterContext filterContext = new FilterContext();
          filterContext.setSubscription(subscription);
          filterContext.setMsgMetadata(messageMetadata);
          filterContext.setConsumer(null);
          Entry entry = null; // we would need the Entry only in case of batch messages
          EntryFilter.FilterResult filterResult = filter.filterEntry(entry, filterContext, true);
          if (filterResult == EntryFilter.FilterResult.REJECT) {
            if (log.isDebugEnabled()) {
              log.debug(
                  "Reject message {}:{} for subscription {}",
                  ledgerId,
                  entryId,
                  subscription.getName());
            }
            // ir is possible that calling this method in this thread may affect
            // performance
            // let's keep it simple for now, we can optimize it later
            subscription.acknowledgeMessage(
                Collections.singletonList(new PositionImpl(ledgerId, entryId)),
                CommandAck.AckType.Individual,
                null);
          }
        } finally {
          filterProcessingTimeOnProduce
              .labels(topic.getName(), subscription.getName())
              .observe(System.nanoTime() - now);
        }
      }
    } catch (Throwable error) {
      log.error("Error while filtering message", error);
    } finally {
      if (entryReadFromBookie != null) {
        entryReadFromBookie.release();
      }
    }
  }

  private static CompletableFuture<ByteBuf> readSingleEntry(
      long ledgerId, long entryId, PersistentTopic topic) {
    log.info("Reading entry {}:{} from topic {}", ledgerId, entryId, topic.getName());
    CompletableFuture<ByteBuf> entryFuture = new CompletableFuture<>();

    PositionImpl position = new PositionImpl(ledgerId, entryId);
    ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) topic.getManagedLedger();
    managedLedger.asyncReadEntry(
        position,
        new AsyncCallbacks.ReadEntryCallback() {
          @Override
          public void readEntryComplete(Entry entry, Object ctx) {
            ByteBuf data = entry.getDataBuffer();
            entryFuture.complete(data);
          }

          @Override
          public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
            log.error("Failed to read entry", exception);
            entryFuture.completeExceptionally(exception);
          }
        },
        null);
    return entryFuture;
  }

  private static MessageMetadata getMessageMetadata(ByteBuf messageMetadataUnparsed, int size) {
    MessageMetadata messageMetadata = new MessageMetadata();
    messageMetadata.parseFrom(messageMetadataUnparsed, size);
    return messageMetadata;
  }

  private void scheduleOnDispatchThread(Runnable runnable) {
    // we let the threadpool peek any thread,
    // this way a broker owning few paritions can still use all the threads
    try {
      executor.submit(runnable);
    } catch (Throwable error) {
      log.error("Error while scheduling on dispatch thread", error);
    }
  }

  @Override
  public void close() {
    log.info("Broker is shutting down. Disabling JMSPublishFilters interceptor");
    closed.set(true);
    filter.close();
    executor.shutdown();
  }

  @Override
  public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {}

  @Override
  public void onConnectionClosed(ServerCnx cnx) {}

  @Override
  public void onWebserviceRequest(ServletRequest request)
      throws IOException, ServletException, InterceptException {}

  @Override
  public void onWebserviceResponse(ServletRequest request, ServletResponse response)
      throws IOException, ServletException {}
}
