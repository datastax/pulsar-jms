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
import java.util.Arrays;
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
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.intercept.InterceptException;
import org.apache.pulsar.common.protocol.Commands;

@Slf4j
public class WritePathSelectors implements BrokerInterceptor {

  private final JMSFilter filter = new JMSFilter();

  @Override
  public void initialize(PulsarService pulsarService) throws Exception {
    log.info("initialize");
  }

  @Override
  public void onMessagePublish(
      Producer producer, ByteBuf headersAndPayload, Topic.PublishContext publishContext) {
    log.info("onMessagePublish {} {}", producer.getProducerName(), producer.getTopic().getName());
    MessageMetadata messageMetadata =
        Commands.peekMessageMetadata(headersAndPayload, "jms-filter-on-publish", -1);
    if (messageMetadata.hasNumMessagesInBatch() && messageMetadata.getNumMessagesInBatch() > 0) {
      log.info("Skip batch message");
      return;
    }
    if (messageMetadata.hasMarkerType()
        && messageMetadata.getMarkerType() != MarkerType.UNKNOWN_MARKER_VALUE) {
      log.info("Skip marker message");
      return;
    }

    producer
        .getTopic()
        .getSubscriptions()
        .forEach(
            (name, subscription) -> {
              Map<String, String> subscriptionProperties = subscription.getSubscriptionProperties();
              FilterContext filterContext = new FilterContext();
              filterContext.setSubscription(subscription);
              filterContext.setMsgMetadata(messageMetadata);
              filterContext.setConsumer(null);
              Entry entry = null; // we would need the Entry only in case of batch messages
              EntryFilter.FilterResult filterResult = filter.filterEntry(entry, filterContext);
              log.info("Filter result for subscription {} is {}", name, filterResult);
              String property = "filter-result-" + name + "@" + subscription.getTopicName();
              publishContext.setProperty(property, filterResult);
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
    log.info(
        "messageProduced {} {}: {}:{}",
        producer.getProducerName(),
        producer.getTopic().getName(),
        ledgerId,
        entryId);
    producer
        .getTopic()
        .getSubscriptions()
        .forEach(
            (name, subscription) -> {
              String property = "filter-result-" + name + "@" + subscription.getTopicName();
              EntryFilter.FilterResult filterResult =
                  (EntryFilter.FilterResult) publishContext.getProperty(property);
              log.info(
                  "Filter result for subscription {} is {} for entry {}:{}",
                  name,
                  filterResult,
                  ledgerId,
                  entryId);
              if (filterResult == EntryFilter.FilterResult.REJECT) {
                log.info("Reject message {}:{} for subscription {}", ledgerId, entryId, name);
                subscription.acknowledgeMessage(
                    Arrays.asList(new PositionImpl(ledgerId, entryId)),
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
