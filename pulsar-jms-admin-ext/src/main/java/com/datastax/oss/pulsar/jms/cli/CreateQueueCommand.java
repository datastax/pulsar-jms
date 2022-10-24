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
import java.util.Arrays;
import java.util.List;
import javax.jms.Queue;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.admin.cli.extensions.ParameterDescriptor;
import org.apache.pulsar.admin.cli.extensions.ParameterType;

@Slf4j
public class CreateQueueCommand extends SubscriptionBaseCommand {

  public CreateQueueCommand() {
    super(false, "queue");
  }

  @Override
  public String name() {
    return "create-queue";
  }

  @Override
  public String description() {
    return "Create a JMS Queue";
  }

  @Override
  protected void defineParameters(List<ParameterDescriptor> list) {
    super.defineParameters(list);
    list.add(
        ParameterDescriptor.builder()
            .description("Number of Partitions")
            .type(ParameterType.INTEGER)
            .names(Arrays.asList("--num-partitions", "-np"))
            .required(false)
            .build());
  }

  protected int getNumPartitions() {
    return Integer.parseInt(getStringParameter("--num-partitions", "0"));
  }

  public void executeInternal() throws Exception {
    PulsarDestination destination = getDestination();
    getAdmin()
        .createQueue((Queue) destination, getNumPartitions(), isEnableFiltering(), getSelector());
  }
}
