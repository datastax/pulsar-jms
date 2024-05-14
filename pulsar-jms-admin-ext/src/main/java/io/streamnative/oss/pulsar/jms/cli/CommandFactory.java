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
package io.streamnative.oss.pulsar.jms.cli;

import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.admin.cli.extensions.CommandExecutionContext;
import org.apache.pulsar.admin.cli.extensions.CustomCommand;
import org.apache.pulsar.admin.cli.extensions.CustomCommandFactory;
import org.apache.pulsar.admin.cli.extensions.CustomCommandGroup;

@Slf4j
public class CommandFactory implements CustomCommandFactory {
  @Override
  public List<CustomCommandGroup> commandGroups(CommandExecutionContext context) {
    return Arrays.asList(new JMSCommandGroup());
  }

  private static class JMSCommandGroup implements CustomCommandGroup {
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
          new DescribeQueueCommand(),
          new CreateQueueCommand(),
          new UpdateQueueCommand(),
          new DescribeTopicCommand(),
          new CreateTopicCommand(),
          new CreateSubscriptionCommand(),
          new UpdateSubscriptionCommand());
    }
  }
}
