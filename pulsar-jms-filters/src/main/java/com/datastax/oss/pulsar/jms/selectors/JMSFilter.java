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

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.api.proto.MessageMetadata;

@Slf4j
public class JMSFilter implements EntryFilter {
  @Override
  public FilterResult filterEntry(Entry entry, FilterContext context) {
    Subscription subscription = context.getSubscription();
    if (!(subscription instanceof PersistentSubscription)) {
      return FilterResult.ACCEPT;
    }
    PersistentSubscription persistentSubscription = (PersistentSubscription) subscription;
    Map<String, String> subscriptionProperties = persistentSubscription.getSubscriptionProperties();
    String jmsSelector = subscriptionProperties.get("jms.selector");
    if (jmsSelector == null || jmsSelector.isEmpty()) {
      return FilterResult.ACCEPT;
    }
    MessageMetadata msgMetadata = context.getMsgMetadata();
    log.info("filterEntry {} {} {}", entry, subscriptionProperties, msgMetadata);
    return FilterResult.ACCEPT;
  }

  @Override
  public void close() {
    log.info("closing {}", this);
  }
}
