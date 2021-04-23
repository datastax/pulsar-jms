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
package fish.payara.examples.payaramicro.jms;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Resource;
import javax.ejb.Schedule;
import javax.ejb.Stateless;
import javax.jms.*;
import javax.resource.AdministeredObjectDefinition;
import javax.resource.ConnectionFactoryDefinition;

/**
 * An example Timer Bean to send messages to an Pulsar broker
 *
 * @author Steve Millidge
 */
@Stateless
@ConnectionFactoryDefinition(
  name = "java:app/jms/SendJMS",
  interfaceName = "javax.jms.ConnectionFactory",
  resourceAdapter = "pulsarra",
  properties = {"configuration=${MPCONFIG=pulsar.config}"}
)
@AdministeredObjectDefinition(
  resourceAdapter = "pulsarra",
  interfaceName = "javax.jms.Queue",
  className = "com.datastax.oss.pulsar.jms.PulsarQueue",
  name = "java:app/jms/MyQueue",
  properties = {"Name=${MPCONFIG=queue.name}"}
)
public class SendJMSMessage {

  @Resource(lookup = "java:app/jms/MyQueue")
  Queue queue;

  @Resource(lookup = "java:app/jms/SendJMS")
  ConnectionFactory factory;

  @Schedule(
    hour = "*",
    minute = "*",
    second = "*/5",
    info = "Every 5 second timer",
    timezone = "UTC",
    persistent = false
  )
  public void myTimer() {
    try (JMSContext context = factory.createContext()) {
      System.out.println("Sending a message...");
      context.createProducer().send(queue, "This is a test at " + new Date());
    } catch (JMSRuntimeException ex) {
      Logger.getLogger(SendJMSMessage.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
