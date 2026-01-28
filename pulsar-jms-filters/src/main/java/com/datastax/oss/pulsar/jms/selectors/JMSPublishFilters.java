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
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.util.SafeRunnable;
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
  private static final int TIMEOUT_READ_ENTRY = 10000; // 10 seconds to read

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

  private static final Histogram filterAckTimeOnProduce =
      Histogram.build()
          .name("pulsar_jmsfilter_ack_time_onpublish")
          .help("Time taken to persist the ack on the broker after applying filters")
          .labelNames("topic", "subscription")
          .buckets(BUCKETS)
          .create();

  private static final Histogram filterOverallProcessingTimeOnPublish =
      Histogram.build()
          .name("pulsar_jmsfilter_overall_processing_time_onpublish")
          .help(
              "Time taken to process the message on the broker from publishers and applying filters")
          .labelNames("topic")
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

  private static final Gauge pendingAcks =
      Gauge.build()
          .name("pulsar_jmsfilter_processing_pending_acks")
          .help("Number of pending acks in the JMSPublishFilters interceptor")
          .create();

  private static final Counter readFromLedger =
      Counter.build()
          .name("pulsar_jmsfilter_entries_read_from_ledger")
          .help("Number of entries read from ledgers by JMSPublishFilters interceptor")
          .create();

  private final JMSFilter filter = new JMSFilter(false);
  private boolean enabled = false;
  private Semaphore memoryLimit;
  private final AtomicBoolean closed = new AtomicBoolean();
  private ExecutorService executor;
  private final BlockingQueue<AckFuture> ackQueue = new ArrayBlockingQueue<>(100000);
  private final Runnable drainAckQueueTask = SafeRunnable.safeRun(this::drainAckQueue);
  private ExecutorService drainAckQueueExecutor;

  @Override
  public void initialize(PulsarService pulsarService) {
    enabled =
        Boolean.parseBoolean(
            pulsarService
                .getConfiguration()
                .getProperties()
                .getProperty("jmsApplyFiltersOnPublish", "true"));
    log.info("jmsApplyFiltersOnPublish={}", enabled);

    // the number of threads is bigger than the number of cores because
    // sometimes the operation has to perform a blocking read to get the entry from the bookies
    int numThreads =
        Integer.parseInt(
            pulsarService
                .getConfiguration()
                .getProperties()
                .getProperty(
                    "jmsFiltersOnPublishThreads",
                    (Runtime.getRuntime().availableProcessors() * 4) + ""));
    log.info("jmsFiltersOnPublishThreads={}", numThreads);
    executor =
        Executors.newFixedThreadPool(numThreads, new WorkersThreadFactory("jms-filters-workers-"));
    int numThreadsAcks =
        Integer.parseInt(
            pulsarService
                .getConfiguration()
                .getProperties()
                .getProperty(
                    "jmsFiltersOnPublishAckThreads",
                    (Math.max(2, Runtime.getRuntime().availableProcessors() / 2)) + ""));
    log.info("jmsFiltersOnPublishAckThreads={}", numThreadsAcks);
    drainAckQueueExecutor =
        Executors.newFixedThreadPool(numThreadsAcks, new WorkersThreadFactory("jms-filters-acks-"));
    try {
      log.info("Registering JMSFilter metrics");
      CollectorRegistry.defaultRegistry.register(filterProcessingTimeOnPublish);
      CollectorRegistry.defaultRegistry.register(filterProcessingTimeOnProduce);
      CollectorRegistry.defaultRegistry.register(filterAckTimeOnProduce);
      CollectorRegistry.defaultRegistry.register(memoryUsed);
      CollectorRegistry.defaultRegistry.register(pendingOperations);
      CollectorRegistry.defaultRegistry.register(pendingAcks);
      CollectorRegistry.defaultRegistry.register(filterOverallProcessingTimeOnPublish);
      CollectorRegistry.defaultRegistry.register(readFromLedger);
    } catch (IllegalArgumentException alreadyRegistered) {
      // ignore
      log.info("Filter metrics already registered", alreadyRegistered);
    }
    String memoryLimitString =
        pulsarService
            .getConfiguration()
            .getProperties()
            .getProperty("jmsFiltersOnPublishMaxMemoryMB", "256");

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

    // start the ack queue draining
    drainAckQueueExecutor.submit(drainAckQueueTask);
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
    long startNanos = System.nanoTime();
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
    Topic topic = producer.getTopic();
    List<Subscription> subscriptions =
        topic
            .getSubscriptions()
            .values()
            .stream()
            .filter(JMSPublishFilters::isPersistentSubscriptionWithSelector)
            .collect(Collectors.toList());
    pendingOperations.inc();
    if (subscriptions.isEmpty()) { // this is very unlikely
      onComplete.run();
      return;
    }
    FilterAndAckMessageOperation filterAndAckMessageOperation =
        new FilterAndAckMessageOperation(
            ledgerId,
            entryId,
            startNanos,
            (PersistentTopic) topic,
            subscriptions,
            messageMetadataUnparsed,
            onComplete);
    scheduleOnWorkerThreads(filterAndAckMessageOperation, onComplete);
  }

  private static boolean isPersistentSubscriptionWithSelector(Subscription subscription) {
    return subscription instanceof PersistentSubscription
        && subscription.getSubscriptionProperties().containsKey("jms.selector")
        && "true".equals(subscription.getSubscriptionProperties().get("jms.filtering"));
  }

  @AllArgsConstructor
  private static class WorkersThreadFactory implements ThreadFactory {
    private static final AtomicInteger THREAD_COUNT = new AtomicInteger();
    private final String name;

    @Override
    public Thread newThread(Runnable r) {
      return new Thread(r, name + THREAD_COUNT.getAndIncrement());
    }
  }

  @AllArgsConstructor
  private class FilterAndAckMessageOperation implements Runnable {
    final long ledgerId;
    final long entryId;
    final long startNanos;
    final PersistentTopic topic;
    final List<Subscription> subscriptions;
    final ByteBuf messageMetadataUnparsed;
    final Runnable onComplete;

    @Override
    public void run() {
      try {
        filterAndAckMessage(ledgerId, entryId, topic, subscriptions, messageMetadataUnparsed);
      } catch (Throwable error) {
        log.error(
            "Error while filtering message {}:{}, topic {}",
            ledgerId,
            entryId,
            topic.getName(),
            error);
      } finally {
        onComplete.run();
        filterOverallProcessingTimeOnPublish
            .labels(topic.getName())
            .observe(System.nanoTime() - startNanos);
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

    if (!isTopicOwned(topic)) {
      return;
    }

    ByteBuf entryReadFromBookie = null;
    try {
      final MessageMetadata messageMetadata;
      if (messageMetadataUnparsed == COULDNOT_ACQUIRE_MEMORY_PLACEHOLDER) {
        // note that this is a blocking IO operation, for this reason
        // it makes sense to have more threads than the number of cores
        try {
          entryReadFromBookie =
              readSingleEntry(ledgerId, entryId, topic).get(TIMEOUT_READ_ENTRY, TimeUnit.SECONDS);
        } catch (InterruptedException interruptedException) {
          Thread.currentThread().interrupt();
        } catch (TimeoutException timeoutException) {
          // timeout
        } catch (ExecutionException err) {
          throw err.getCause();
        }
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

      // if we have more than one subscription we can save a lot of resources by caching the
      // properties
      MessageMetadataCache messageMetadataCache =
          subscriptions.size() > 1 ? new MessageMetadataCache(messageMetadata) : null;
      for (Subscription subscription : subscriptions) {
        if (closed.get()) {
          // the broker is shutting down, we cannot process the entries
          // this operation has been enqueued before the broker shutdown
          return;
        }
        if (!isTopicOwned(topic)) {
          return;
        }

        EntryFilter.FilterResult filterResult;
        long now = System.nanoTime();
        try {
          FilterContext filterContext = new FilterContext();
          filterContext.setSubscription(subscription);
          filterContext.setMsgMetadata(messageMetadata);
          filterContext.setConsumer(null);
          Entry entry = null; // we would need the Entry only in case of batch messages
          filterResult = filter.filterEntry(entry, filterContext, true, messageMetadataCache);
        } finally {
          filterProcessingTimeOnProduce
              .labels(topic.getName(), subscription.getName())
              .observe(System.nanoTime() - now);
        }

        if (filterResult == EntryFilter.FilterResult.REJECT) {
          if (log.isDebugEnabled()) {
            log.debug(
                "Reject message {}:{} for subscription {}",
                ledgerId,
                entryId,
                subscription.getName());
          }

          pendingAcks.inc();
          ackQueue.put(
              new AckFuture(
                  (PersistentSubscription) subscription,
                  PositionFactory.create(ledgerId, entryId)));
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

  @AllArgsConstructor
  private static final class AckFuture {
    private final PersistentSubscription subscription;
    private final Position position;
  }

  private void drainAckQueue() {
    try {
      Map<Subscription, List<Position>> acksBySubscription = new HashMap<>();
      try {
        int maxItems = 50000;
        while (maxItems-- > 0) {
          AckFuture ackFuture = ackQueue.poll(100, TimeUnit.MILLISECONDS);
          if (ackFuture == null) {
            break;
          }
          pendingAcks.dec();
          List<Position> acks =
              acksBySubscription.computeIfAbsent(ackFuture.subscription, k -> new ArrayList<>());
          acks.add(ackFuture.position);
        }
      } catch (InterruptedException exit) {
        Thread.currentThread().interrupt();
        log.info("JMSPublishFilter Ack queue draining interrupted");
      } catch (Throwable error) {
        log.error("Error while draining ack queue", error);
      } finally {
        // continue draining the queue
        if (!closed.get()) {
          drainAckQueueExecutor.submit(drainAckQueueTask);
        }
      }
      for (Map.Entry<Subscription, List<Position>> entry : acksBySubscription.entrySet()) {
        long now = System.nanoTime();
        Subscription subscription = entry.getKey();
        PersistentTopic topic = (PersistentTopic) subscription.getTopic();
        if (!isTopicOwned(topic)) {
          continue;
        }
        try {
          List<Position> acks = entry.getValue();
          subscription.acknowledgeMessage(acks, CommandAck.AckType.Individual, null);
        } finally {
          filterAckTimeOnProduce
              .labels(topic.getName(), subscription.getName())
              .observe(System.nanoTime() - now);
        }
      }
    } catch (Throwable error) {
      log.error("Error while draining ack queue", error);
    }
  }

  private static boolean isTopicOwned(PersistentTopic topic) {
    ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) topic.getManagedLedger();
    switch (managedLedger.getState()) {
      case Closed:
      case Terminated:
      case Fenced:
      case FencedForDeletion:
        return false;
      default:
        return true;
    }
  }

  private static CompletableFuture<ByteBuf> readSingleEntry(
      long ledgerId, long entryId, PersistentTopic topic) {
    readFromLedger.inc();
    CompletableFuture<ByteBuf> entryFuture = new CompletableFuture<>();

    Position position = PositionFactory.create(ledgerId, entryId);
    ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) topic.getManagedLedger();
    // asyncReadEntry reads from the Broker cache, and falls bach to the Bookie
    // is also leverage bookie read deduplication
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

  private void scheduleOnWorkerThreads(Runnable runnable, Runnable onError) {
    // we let the thread pool peek any thread,
    // this way a broker owning few partitions can still use all the cores
    try {
      executor.submit(runnable);
    } catch (Throwable error) {
      log.error("Error while scheduling on worker threads", error);
      onError.run();
    }
  }

  @Override
  public void close() {
    log.info("Broker is shutting down. Disabling JMSPublishFilters interceptor");
    closed.set(true);
    filter.close();
    if (executor != null) {
      executor.shutdown();
    }
    if (drainAckQueueExecutor != null) {
      drainAckQueueExecutor.shutdown();
    }
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
