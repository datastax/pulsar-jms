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

import jakarta.ejb.ActivationConfigProperty;
import jakarta.ejb.MessageDriven;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import java.util.concurrent.atomic.AtomicInteger;

@MessageDriven(
  name = "testmdbtopic",
  activationConfig = {
    @ActivationConfigProperty(propertyName = "resourceAdapter", propertyValue = "pulsarra"),
    @ActivationConfigProperty(
      propertyName = "destinationType",
      propertyValue = "jakarta.jms.Topic"
    ),
    @ActivationConfigProperty(propertyName = "subscriptionType", propertyValue = "Durable"),
    @ActivationConfigProperty(propertyName = "subscriptionName", propertyValue = "mysub"),
    @ActivationConfigProperty(propertyName = "subscriptionMode", propertyValue = "Shared"),
    @ActivationConfigProperty(
      propertyName = "destination",
      propertyValue = "${MPCONFIG=topic.name}"
    ),
    @ActivationConfigProperty(
      propertyName = "configuration",
      propertyValue = "${MPCONFIG=pulsar.config}"
    )
  }
)
public class ReceiveMessageFromTopic implements MessageListener {

  static AtomicInteger countTopic = new AtomicInteger();
  static AtomicInteger countQueue = new AtomicInteger();

  @Override
  public void onMessage(Message message) {

    int current = countTopic.incrementAndGet();
    System.out.println("TOPIC RECEIVED #" + countTopic + "+" + countQueue + " MESSAGE " + message);

    if (current == 10 && countQueue.get() == 10) {
      System.out.println("RECEIVED ENOUGH MESSAGES. Shutting down");
      Runtime.getRuntime().halt(0);
    }
  }
}
