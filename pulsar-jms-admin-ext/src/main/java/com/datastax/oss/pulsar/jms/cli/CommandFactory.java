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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.admin.cli.extensions.CommandExecutionContext;
import org.apache.pulsar.admin.cli.extensions.CustomCommand;
import org.apache.pulsar.admin.cli.extensions.CustomCommandFactory;
import org.apache.pulsar.admin.cli.extensions.CustomCommandGroup;
import org.apache.pulsar.admin.cli.extensions.ParameterDescriptor;
import org.apache.pulsar.admin.cli.extensions.ParameterType;
import org.apache.pulsar.common.policies.data.TopicStats;

@Slf4j
public class CommandFactory implements CustomCommandFactory {
  @Override
  public List<CustomCommandGroup> commandGroups(CommandExecutionContext context) {
    return Arrays.asList(
        new CustomCommandGroup() {
          @Override
          public String name() {
            return "jms";
          }

          @Override
          public String description() {
            return "Starlight for JMS commands";
          }

          @Override
          public List<CustomCommand> commands(CommandExecutionContext context) {
            return Arrays.asList(
                new CustomCommand() {
                  @Override
                  public String name() {
                    return "describe";
                  }

                  @Override
                  public String description() {
                    return "Describe a JMS Destination";
                  }

                  @Override
                  public List<ParameterDescriptor> parameters() {
                    return Arrays.asList(
                        ParameterDescriptor.builder()
                            .description("Destination type")
                            .type(ParameterType.STRING)
                            .name("--type")
                            .required(true)
                            .build(),
                        ParameterDescriptor.builder()
                            .description("Destination")
                            .type(ParameterType.STRING)
                            .name("--destination")
                            .required(true)
                            .build());
                  }

                  @Override
                  public boolean execute(
                      Map<String, Object> parameters, CommandExecutionContext context)
                      throws Exception {
                    String destination = parameters.getOrDefault("--destination", "").toString();
                    TopicStats stats = context.getPulsarAdmin().topics().getStats(destination);
                    System.out.println("Topic stats: " + stats);
                    return false;
                  }
                });
          }
        });
  }
}
