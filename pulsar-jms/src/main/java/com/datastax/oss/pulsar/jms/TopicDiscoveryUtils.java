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
package com.datastax.oss.pulsar.jms;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.jms.JMSException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class TopicDiscoveryUtils {

  public static List<String> discoverTopicsByPattern(String regex, PulsarClient client, int timeout)
      throws JMSException {
    TopicName destination = TopicName.get(regex);
    NamespaceName namespaceName = destination.getNamespaceObject();

    LookupService lookup = ((PulsarClientImpl) client).getLookup();
    try {
      List<String> list =
          lookup
              .getTopicsUnderNamespace(
                  namespaceName, CommandGetTopicsOfNamespace.Mode.PERSISTENT, null, null)
              .get(timeout, TimeUnit.MILLISECONDS)
              .getTopics();
      return topicsPatternFilter(list, Pattern.compile(regex));
    } catch (InterruptedException | TimeoutException | ExecutionException err) {
      throw Utils.handleException(err);
    }
  }

  // get topics that match 'topicsPattern' from original topics list
  // return result should contain only topic names, without partition part
  public static List<String> topicsPatternFilter(List<String> original, Pattern topicsPattern) {
    // taken from Pulsar 2.10.x
    // https://github.com/datastax/pulsar/blob/6bc7cf8d7d5b2d5b5e7c5b93c820aa53319e500a/
    // pulsar-client/src/main/java/org/apache/pulsar/client/impl/PulsarClientImpl.java#L586
    final Pattern shortenedTopicsPattern =
        topicsPattern.toString().contains("://")
            ? Pattern.compile(topicsPattern.toString().split("\\:\\/\\/")[1])
            : topicsPattern;

    return original
        .stream()
        .map(TopicName::get)
        .map(TopicName::toString)
        .filter(topic -> shortenedTopicsPattern.matcher(topic.split("\\:\\/\\/")[1]).matches())
        .collect(Collectors.toList());
  }
}
