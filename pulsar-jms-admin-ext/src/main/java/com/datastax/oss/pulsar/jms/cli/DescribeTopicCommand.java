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

import com.datastax.oss.pulsar.jms.PulsarDestination;
import com.datastax.oss.pulsar.jms.api.JMSDestinationMetadata;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@Slf4j
class DescribeTopicCommand extends TopicBaseCommand {

  public DescribeTopicCommand() {
    super("topic");
  }

  @Override
  public String name() {
    return "describe-topic";
  }

  @Override
  public String description() {
    return "Describe a JMS Topic";
  }

  @Override
  protected void executeInternal() throws Exception {

    PulsarDestination destination = getDestination();

    JMSDestinationMetadata describe = getAdmin(destination.isRegExp()).describe(destination);
    String json =
        ObjectMapperFactory.create()
            .configure(SerializationFeature.INDENT_OUTPUT, true)
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .writeValueAsString(describe);
    println(json);
  }
}
