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
import org.apache.pulsar.admin.cli.extensions.ParameterDescriptor;
import org.apache.pulsar.admin.cli.extensions.ParameterType;

abstract class TopicBaseCommand extends BaseCommand {

  final String defaultDestinationType;

  public TopicBaseCommand(String defaultDestinationType) {
    this.defaultDestinationType = defaultDestinationType;
  }

  protected void defineParameters(List<ParameterDescriptor> list) {
    if (defaultDestinationType == null) {
      // if there is a default we don't let the use choose
      list.add(
              ParameterDescriptor.builder()
                      .description("Destination type")
                      .type(ParameterType.STRING)
                      .names(Arrays.asList("--destination-type", "-dt"))
                      .required(false)
                      .build());
    }
    list.add(
        ParameterDescriptor.builder()
            .description("Destination")
            .type(ParameterType.STRING)
            .mainParameter(true)
            .names(Arrays.asList("--destination", "-d"))
            .required(true)
            .build());
  }

  protected PulsarDestination getDestination(boolean requireTopic, boolean requireQueue) throws Exception {
    String destination = getStringParameter("--destination", "");
    String destinationType = getStringParameter("--destination-type", this.defaultDestinationType);
    switch (destinationType) {
      case "queue":
        if (requireTopic) {
          throw new IllegalArgumentException("this command is supported only on JMS Topics");
        }
        return (PulsarDestination) getContext().createQueue(destination);
      case "topic":
        if (requireQueue) {
          throw new IllegalArgumentException("this command is supported only on JMS Queues");
        }
        return (PulsarDestination) getContext().createTopic(destination);
      default:
        throw new IllegalArgumentException("Invalid destination type " + destinationType);
    }
  }
}
