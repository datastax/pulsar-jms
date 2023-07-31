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

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.TopicStats;

@Slf4j
abstract class PulsarTemporaryDestination extends PulsarDestination {

  private final PulsarSession session;

  public PulsarTemporaryDestination(String topicName, PulsarSession session)
      throws InvalidDestinationException {
    super(topicName);
    this.session = session;
    if (isVirtualDestination()) {
      throw new InvalidDestinationException("Temporary destinations cannot be virtual");
    }
  }

  public PulsarSession getSession() {
    return session;
  }

  public final void delete() throws JMSException {
    try {
      log.info("Deleting {}", this);
      String topicName = getInternalTopicName();
      String fullQualifiedTopicName = session.getFactory().applySystemNamespace(topicName);
      TopicStats stats =
          session.getFactory().getPulsarAdmin().topics().getStats(fullQualifiedTopicName);
      log.info("Stats {}", stats);

      int numConsumers =
          stats.getSubscriptions().values().stream().mapToInt(s -> s.getConsumers().size()).sum();
      if (numConsumers > 0) {
        throw new JMSException("Cannot delete a temporary destination with active consumers");
      }

      if (session
          .getFactory()
          .getPulsarAdmin()
          .topics()
          .getPartitionedTopicList(session.getFactory().getSystemNamespace())
          .stream()
          .anyMatch(t -> t.equals(fullQualifiedTopicName))) {
        session
            .getFactory()
            .getPulsarAdmin()
            .topics()
            .deletePartitionedTopic(
                fullQualifiedTopicName, session.getFactory().isForceDeleteTemporaryDestinations());
      } else {
        session
            .getFactory()
            .getPulsarAdmin()
            .topics()
            .delete(
                fullQualifiedTopicName, session.getFactory().isForceDeleteTemporaryDestinations());
      }

    } catch (final PulsarAdminException paEx) {
      Utils.handleException(paEx);
    } finally {
      session.getConnection().removeTemporaryDestination(this);
    }
  }
}
