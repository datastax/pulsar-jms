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
package com.datastax.oss.pulsar.jms.tests;

import jakarta.annotation.Resource;
import jakarta.ejb.Schedule;
import jakarta.ejb.Stateless;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.Queue;
import java.util.Date;

// import jakarta.resource.AdministeredObjectDefinition;
// import jakarta.resource.ConnectionFactoryDefinition;

@Stateless
public class SendJMSMessage {

  @Resource(name = "pulsar-jakarta.jms.ConnectionFactory")
  ConnectionFactory factory;

  @Resource(name = "pulsar-jakarta.jms.Queue")
  private Queue queue;

  @Schedule(
    hour = "*",
    minute = "*",
    second = "*/1",
    info = "Every 1 second timer",
    timezone = "UTC",
    persistent = false
  )
  public void doSend() {
    System.out.println("factory " + factory + " queue " + queue + " " + queue.getClass());
    try (JMSContext context = factory.createContext()) {
      System.out.println("Sending a message..." + context);
      context.createProducer().send(queue, "This is a test at " + new Date());
      System.out.println("Sending a message...message sent");
    } catch (Throwable ex) {
      ex.printStackTrace(System.out);
    }
  }
}
