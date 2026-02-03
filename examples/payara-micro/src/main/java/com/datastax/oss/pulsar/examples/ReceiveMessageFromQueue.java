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
package com.datastax.oss.pulsar.examples;

import static com.datastax.oss.pulsar.examples.ReceiveMessageFromTopic.countQueue;
import static com.datastax.oss.pulsar.examples.ReceiveMessageFromTopic.countTopic;

import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.jms.Message;
import javax.jms.MessageListener;

@MessageDriven(
  name = "testmdbqueue",
  activationConfig = {
    @ActivationConfigProperty(propertyName = "resourceAdapter", propertyValue = "pulsarra"),
    @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
    @ActivationConfigProperty(
      propertyName = "destination",
      propertyValue = "${MPCONFIG=queue.name}"
    ),
    @ActivationConfigProperty(
      propertyName = "configuration",
      propertyValue = "${MPCONFIG=pulsar.config}"
    )
  }
)
public class ReceiveMessageFromQueue implements MessageListener {

  @Override
  public void onMessage(Message message) {

    int current = countQueue.incrementAndGet();
    System.out.println("QUEUE RECEIVED #" + countTopic + "+" + countQueue + " MESSAGE " + message);

    if (current == 10 && countTopic.get() == 10) {
      System.out.println("RECEIVED ENOUGH MESSAGES. Shutting down");
      Runtime.getRuntime().halt(0);
    }
  }
}
