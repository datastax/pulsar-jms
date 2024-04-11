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
import java.util.Collections;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Topic;
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
  private static final String JMS_FILTERED_PROPERTY = "jms-filtered";
  private final JMSFilter filter = new JMSFilter();
  private boolean enabled = false;

  @Override
  public void initialize(PulsarService pulsarService) throws Exception {
    enabled =
        Boolean.parseBoolean(
            pulsarService.getConfiguration().getProperty("jmsApplyFiltersOnPublish") + "");
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

    MessageMetadata messageMetadata =
        Commands.peekMessageMetadata(headersAndPayload, "jms-filter-on-publish", -1);
    if (messageMetadata.hasNumMessagesInBatch()) {
      return;
    }
    producer
        .getTopic()
        .getSubscriptions()
        .forEach(
            (name, subscription) -> {
              if (!(subscription instanceof PersistentSubscription)) {
                return;
              }
              Map<String, String> subscriptionProperties = subscription.getSubscriptionProperties();
              if (!subscriptionProperties.containsKey("jms.selector")) {
                return;
              }
              FilterContext filterContext = new FilterContext();
              filterContext.setSubscription(subscription);
              filterContext.setMsgMetadata(messageMetadata);
              filterContext.setConsumer(null);
              Entry entry = null; // we would need the Entry only in case of batch messages
              EntryFilter.FilterResult filterResult = filter.filterEntry(entry, filterContext);
              if (filterResult == EntryFilter.FilterResult.REJECT) {
                String property = "filter-result-" + name + "@" + subscription.getTopicName();
                publishContext.setProperty(property, filterResult);
                publishContext.setProperty(JMS_FILTERED_PROPERTY, true);
              }
            });
  }

  @Override
  public void messageProduced(
      ServerCnx cnx,
      Producer producer,
      long startTimeNs,
      long ledgerId,
      long entryId,
      Topic.PublishContext publishContext) {
    if (!enabled || publishContext.getProperty(JMS_FILTERED_PROPERTY) == null) {
      return;
    }
    producer
        .getTopic()
        .getSubscriptions()
        .forEach(
            (name, subscription) -> {
              String property = "filter-result-" + name + "@" + subscription.getTopicName();
              EntryFilter.FilterResult filterResult =
                  (EntryFilter.FilterResult) publishContext.getProperty(property);
              if (filterResult == EntryFilter.FilterResult.REJECT) {
                if (log.isDebugEnabled()) {
                  log.debug("Reject message {}:{} for subscription {}", ledgerId, entryId, name);
                }
                // ir is possible that calling this method in this thread may affect performance
                // let's keep it simple for now, we can optimize it later
                subscription.acknowledgeMessage(
                    Collections.singletonList(new PositionImpl(ledgerId, entryId)),
                    CommandAck.AckType.Individual,
                    null);
              }
            });
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
