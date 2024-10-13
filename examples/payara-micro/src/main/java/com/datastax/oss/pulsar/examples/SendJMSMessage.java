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

import jakarta.annotation.Resource;
import jakarta.ejb.Schedule;
import jakarta.ejb.Stateless;
import jakarta.jms.*;
import jakarta.resource.AdministeredObjectDefinition;
import jakarta.resource.AdministeredObjectDefinitions;
import jakarta.resource.ConnectionFactoryDefinition;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An example Timer Bean to send messages to an Pulsar broker
 *
 * @author Steve Millidge
 */
@Stateless
@ConnectionFactoryDefinition(
  name = "java:app/jms/SendJMS",
  interfaceName = "jakarta.jms.ConnectionFactory",
  resourceAdapter = "pulsarra",
  properties = {"configuration=${MPCONFIG=pulsar.config}"}
)
@AdministeredObjectDefinitions({
  @AdministeredObjectDefinition(
    resourceAdapter = "pulsarra",
    interfaceName = "jakarta.jms.Queue",
    className = "com.datastax.oss.pulsar.jms.PulsarQueue",
    name = "java:app/jms/MyQueue",
    properties = {"Name=${MPCONFIG=queue.name}"}
  ),
  @AdministeredObjectDefinition(
    resourceAdapter = "pulsarra",
    interfaceName = "jakarta.jms.Topic",
    className = "com.datastax.oss.pulsar.jms.PulsarTopic",
    name = "java:app/jms/MyTopic",
    properties = {"Name=${MPCONFIG=topic.name}"}
  )
})
public class SendJMSMessage {

  @Resource(lookup = "java:app/jms/MyQueue")
  Queue queue;

  @Resource(lookup = "java:app/jms/MyTopic")
  Topic topic;

  @Resource(lookup = "java:app/jms/SendJMS")
  ConnectionFactory factory;

  @Schedule(
    hour = "*",
    minute = "*",
    second = "*/2",
    info = "Every 5 second timer",
    timezone = "UTC",
    persistent = false
  )
  public void myTimer() {
    try (JMSContext context = factory.createContext()) {
      System.out.println("Sending a message...(factory: " + factory + ")");
      if (!factory
          .getClass()
          .getName()
          .equals("com.datastax.oss.pulsar.jms.PulsarConnectionFactory")) {
        throw new RuntimeException("Unexpected Factory type " + factory.getClass());
      }
      context.createProducer().send(queue, "This is a test");
      context.createProducer().send(topic, "This is a test");
    } catch (JMSRuntimeException ex) {
      Logger.getLogger(SendJMSMessage.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
