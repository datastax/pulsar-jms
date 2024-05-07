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

import static org.apache.pulsar.common.protocol.Commands.skipBrokerEntryMetadataIfExist;
import static org.apache.pulsar.common.protocol.Commands.skipChecksumIfPresent;
import io.netty.buffer.ByteBuf;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.intercept.InterceptException;
import org.apache.pulsar.common.protocol.Commands;

@Slf4j
public class JMSPublishFilters implements BrokerInterceptor {
  private static final String JMS_FILTER_METADATA = "jms-msg-metadata";

  private static final Histogram filterProcessingTimeOnPublish =
      Histogram.build()
          .name("pulsar_jmsfilter_preprocessing_time_onpublish")
          .help(
              "Time taken to pre-process the message on the broker while accepting messages from producers before applying filters")
          .labelNames("topic")
          .create();
  private static final Histogram filterProcessingTimeOnProduce =
      Histogram.build()
          .name("pulsar_jmsfilter_processing_time_onpublish")
          .help(
              "Time taken to process the message on the broker while accepting messages from producers and applying filters")
          .labelNames("topic", "subscription")
          .create();

  private static final Gauge memoryUsed =
          Gauge.build()
                  .name("pulsar_jmsfilter_processing_memory")
                  .help(
                          "Current memory held by the JMSPublishFilters interceptor")
                  .create();

  private static final Gauge pendingOperations =
          Gauge.build()
                  .name("pulsar_jmsfilter_processing_pending_operations")
                  .help(
                          "Number of pending operations in the JMSPublishFilters interceptor")
                  .create();

  private final JMSFilter filter = new JMSFilter(false);
  private boolean enabled = false;
  private Semaphore memoryLimit;
  private final AtomicBoolean closed = new AtomicBoolean();

  private static final Field dispatchMessagesThreadFieldPersistentDispatcherMultipleConsumers;
  private static final Field dispatchMessagesThreadFieldPersistentDispatcherSingleActiveConsumer;

  static {
    Field fieldPersistentDispatcherMultipleConsumers = null;
    Field fieldPersistentDispatcherSingleActiveConsumer = null;
    try {
      fieldPersistentDispatcherMultipleConsumers =
          PersistentDispatcherMultipleConsumers.class.getDeclaredField("dispatchMessagesThread");
      fieldPersistentDispatcherMultipleConsumers.setAccessible(true);

      fieldPersistentDispatcherSingleActiveConsumer =
          PersistentDispatcherSingleActiveConsumer.class.getDeclaredField("executor");
      fieldPersistentDispatcherSingleActiveConsumer.setAccessible(true);

    } catch (NoSuchFieldException e) {
      log.error("Cannot access thread field: " + e);
    }
    dispatchMessagesThreadFieldPersistentDispatcherMultipleConsumers =
        fieldPersistentDispatcherMultipleConsumers;
    dispatchMessagesThreadFieldPersistentDispatcherSingleActiveConsumer =
        fieldPersistentDispatcherSingleActiveConsumer;
  }

  @Override
  public void initialize(PulsarService pulsarService) {
    enabled =
        Boolean.parseBoolean(
            pulsarService
                .getConfiguration()
                .getProperties()
                .getProperty("jmsApplyFiltersOnPublish", "true"));
    log.info("jmsApplyFiltersOnPublish={}", enabled);

    try {
      log.info("Registering JMSFilter metrics");
      CollectorRegistry.defaultRegistry.register(filterProcessingTimeOnPublish);
      CollectorRegistry.defaultRegistry.register(filterProcessingTimeOnProduce);
    } catch (IllegalArgumentException alreadyRegistered) {
      // ignore
      log.info("Filter metrics already registered", alreadyRegistered);
    }
    String memoryLimitString = pulsarService
            .getConfiguration()
            .getProperties()
            .getProperty("jmsFiltersOnPublishMaxMemoryMB", "128");

    try {
       int memoryLimitBytes = Integer.parseInt(pulsarService
              .getConfiguration()
              .getProperties()
              .getProperty("jmsFiltersOnPublishMaxMemoryMB", "64")) * 1024 * 1024;
       memoryLimit = new Semaphore(memoryLimitBytes);
       log.info("jmsFiltersOnPublishMaxMemoryMB={} ({} bytes)", memoryLimitString, memoryLimitBytes);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Invalid memory limit jmsFiltersOnPublishMaxMemoryMB=" + memoryLimitString, e);
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
        publishContext.setProperty(JMS_FILTER_METADATA, messageMetadata);
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
    int readerIndex = buffer.readerIndex();
    skipBrokerEntryMetadataIfExist(buffer);
    skipChecksumIfPresent(buffer);
     int metadataSize = (int) buffer.readUnsignedInt();
    // this is going to throttle the producer if the memory limit is reached
    // please note that this is a blocking operation on the Netty eventpool
    // currently we cannnot do better than this, as the interceptor API is blocking
    memoryLimit.acquireUninterruptibly(metadataSize);
    // please note that Netty would probably retain more memory than this buffer
    // but this is the best approximation we can do
    memoryUsed.inc(metadataSize);
    ByteBuf copy = buffer.slice(readerIndex, metadataSize).copy();
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
    AtomicInteger pending = new AtomicInteger(1);
    Runnable onComplete = () -> {
      pendingOperations.dec();
      if (pending.decrementAndGet() == 0) {
        messageMetadataUnparsed.release();
        memoryLimit.release(memorySize);
        memoryUsed.dec(memorySize);
      }
    };
    try {
      producer.getTopic().getSubscriptions().forEach((___, subscription) -> {
        if (!(isPersistentSubscriptionWithSelector(subscription))) {
          return;
        }
        pending.incrementAndGet();
        pendingOperations.inc();
        scheduleOnDispatchThread(subscription,
                new FilterAndAckMessageOperation(ledgerId, entryId, subscription, messageMetadataUnparsed, onComplete));
      });
    } finally {
      onComplete.run();
    }
  }

  private static boolean isPersistentSubscriptionWithSelector(Subscription subscription) {
    return subscription instanceof PersistentSubscription
        && subscription.getSubscriptionProperties().containsKey("jms.selector");
  }

  @AllArgsConstructor
  private class FilterAndAckMessageOperation implements Runnable {
    final long ledgerId;
    final long entryId;
    final Subscription subscription;
    final ByteBuf messageMetadataUnparsed;
    final Runnable onComplete;

    @Override
    public void run() {
      try {
        filterAndAckMessage(ledgerId, entryId, subscription, messageMetadataUnparsed);
      } finally {
        onComplete.run();
      }
    }
  }

  private void filterAndAckMessage(
      long ledgerId,
      long entryId,
      Subscription subscription,
      ByteBuf messageMetadataUnparsed) {
   if (closed.get()) {
      // the broker is shutting down, we cannot process the entries
      // this operation has been enqueued before the broker shutdown
      return;
    }
    MessageMetadata messageMetadata = new MessageMetadata();
    Commands.parseMessageMetadata(messageMetadataUnparsed, messageMetadata);
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
              .labels(subscription.getTopic().getName(), subscription.getName())
              .observe(System.nanoTime() - now);
    }
  }

  private static void scheduleOnDispatchThread(Subscription subscription, Runnable runnable) {
    try {
      Dispatcher dispatcher = subscription.getDispatcher();
      if (dispatcher instanceof PersistentDispatcherMultipleConsumers) {
        ExecutorService singleThreadExecutor =
            (ExecutorService)
                dispatchMessagesThreadFieldPersistentDispatcherMultipleConsumers.get(dispatcher);
        if (singleThreadExecutor != null) {
          singleThreadExecutor.submit(runnable);
          return;
        }
      }
      if (dispatcher instanceof PersistentDispatcherSingleActiveConsumer) {
        Executor singleThreadExecutor =
            (Executor)
                dispatchMessagesThreadFieldPersistentDispatcherSingleActiveConsumer.get(dispatcher);
        if (singleThreadExecutor != null) {
          singleThreadExecutor.execute(runnable);
          return;
        }
      }
      // this case also happens when there is no dispatcher (no consumer has connected since the
      // last
      // topic load)
      // this thread is on the same threadpool that is used by PersistentDispatcherMultipleConsumers
      // and PersistentDispatcherSingleActiveConsumer
      subscription.getTopic().getBrokerService().getTopicOrderedExecutor().execute(runnable);
    } catch (Throwable error) {
      log.error("Error while scheduling on dispatch thread", error);
    }
  }

  @Override
  public void close() {
    log.info("Broker is shutting down. Disabling JMSPublishFilters interceptor");
    closed.set(true);
    filter.close();
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
