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

import com.datastax.oss.pulsar.jms.api.JMSAdmin;
import lombok.extern.slf4j.Slf4j;

import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Topic;

@Slf4j
public class UpdateQueueCommand extends SubscriptionBaseCommand {
  public UpdateQueueCommand() {
    super(false, "queue");
  }

  @Override
  public String name() {
    return "update-queue";
  }

  @Override
  public String description() {
    return "Update a JMS Queue";
  }

  public void executeInternal() throws Exception {
    Destination destination = getDestination(false, true);
    JMSAdmin admin = getAdmin();
    admin.setSubscriptionSelector((Queue) destination, isEnableFiltering(), getSelector());
  }
}
