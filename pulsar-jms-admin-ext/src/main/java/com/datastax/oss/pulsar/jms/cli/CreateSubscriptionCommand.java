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
package com.datastax.oss.pulsar.jms.cli;

import javax.jms.Destination;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateSubscriptionCommand extends SubscriptionBaseCommand {
  public CreateSubscriptionCommand() {
    super(true, "topic");
  }

  @Override
  public String name() {
    return "create-subscription";
  }

  @Override
  public String description() {
    return "Create a Subscription on a JMS Topic";
  }

  public void executeInternal() throws Exception {
    String subscription = getSubscription();
    Destination destination = getDestination();
    getAdmin()
        .createSubscription(
            (Topic) destination, subscription, isEnableFiltering(), getSelector(), false);
  }
}
