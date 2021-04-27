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
package examples;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSRuntimeException;
import javax.jms.Queue;

@Stateless
public class SendJMSMessage {

  @Resource(name = "MyJmsConnectionFactory")
  ConnectionFactory factory;

  @Resource(name = "MyQueue")
  Queue queue;

  public void doSend() {
    System.out.println("Queue " + queue);
    System.out.println("factory " + factory);
    try (JMSContext context = factory.createContext()) {
      System.out.println("Sending a message...");
      context.createProducer().send(queue, "This is a test at " + new Date());
    } catch (JMSRuntimeException ex) {
      Logger.getLogger(SendJMSMessage.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
