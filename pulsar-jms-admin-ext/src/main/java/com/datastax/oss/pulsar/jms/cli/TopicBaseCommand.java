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

  final String destinationType;

  public TopicBaseCommand(String destinationType) {
    this.destinationType = destinationType;
  }

  protected void defineParameters(List<ParameterDescriptor> list) {
    list.add(
        ParameterDescriptor.builder()
            .description("Destination")
            .type(ParameterType.STRING)
            .mainParameter(true)
            .names(Arrays.asList("--destination", "-d"))
            .required(true)
            .build());
  }

  protected PulsarDestination getDestination() throws Exception {
    String destination = getStringParameter("--destination", "");
    switch (destinationType) {
      case "queue":
        return (PulsarDestination) getContext().createQueue(destination);
      case "topic":
        return (PulsarDestination) getContext().createTopic(destination);
      default:
        throw new IllegalArgumentException("Invalid destination type " + destinationType);
    }
  }
}
