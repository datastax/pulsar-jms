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
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
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
  private final JMSFilter filter = new JMSFilter();
  private boolean enabled = false;

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

    for (Subscription subscription : producer.getTopic().getSubscriptions().values()) {
      if (!(subscription instanceof PersistentSubscription)) {
        continue;
      }
      Map<String, String> subscriptionProperties = subscription.getSubscriptionProperties();
      if (!subscriptionProperties.containsKey("jms.selector")) {
        continue;
      }

      // we must make a copy because the ByteBuf will be released
      MessageMetadata messageMetadata =
          new MessageMetadata()
              .copyFrom(
                  Commands.peekMessageMetadata(headersAndPayload, "jms-filter-on-publish", -1));

      publishContext.setProperty(JMS_FILTER_METADATA, messageMetadata);
      // as soon as we find a good reason to apply the filters in messageProduced
      // we can exit
      return;
    }
    ;
  }

  @Override
  public void messageProduced(
      ServerCnx cnx,
      Producer producer,
      long startTimeNs,
      long ledgerId,
      long entryId,
      Topic.PublishContext publishContext) {
    if (!enabled) {
      return;
    }
    MessageMetadata messageMetadata =
        (MessageMetadata) publishContext.getProperty(JMS_FILTER_METADATA);
    if (messageMetadata == null) {
      return;
    }
    if (messageMetadata.hasNumMessagesInBatch()) {
      return;
    }
    for (Subscription subscription : producer.getTopic().getSubscriptions().values()) {
      scheduleOnDispatchThread(
          subscription,
          () -> {
            FilterContext filterContext = new FilterContext();
            filterContext.setSubscription(subscription);
            filterContext.setMsgMetadata(messageMetadata);
            filterContext.setConsumer(null);
            Entry entry = null; // we would need the Entry only in case of batch messages
            EntryFilter.FilterResult filterResult = filter.filterEntry(entry, filterContext);
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
          });
    }
    ;
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
