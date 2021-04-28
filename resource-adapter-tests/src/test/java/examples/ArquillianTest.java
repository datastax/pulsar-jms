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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;
import javax.inject.Inject;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ArquillianExtension.class)
public class ArquillianTest {

  @Deployment
  public static Archive<?> createDeployment() throws Exception {
    return ShrinkWrap.create(WebArchive.class, "test.war")
        .addPackage(SendJMSMessage.class.getPackage())
        .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");
  }

  @Resource(name = "pulsar-javax.jms.ConnectionFactory")
  private ConnectionFactory cf;

  @Resource(name = "pulsar-javax.jms.Queue")
  private Queue queue;

  @Inject ReceivedMessages receivedMessages;

  @Test
  @Timeout(value = 20, unit = TimeUnit.SECONDS)
  void validate() throws Exception {
    System.out.println("Factory: " + cf.getClass() + " queue " + queue.getClass());
    assertEquals("com.datastax.oss.pulsar.jms.PulsarConnectionFactory", cf.getClass().getName());
    assertEquals("com.datastax.oss.pulsar.jms.PulsarQueue", queue.getClass().getName());

    int numMessagesFromTest = 10;
    for (int i = 0; i < numMessagesFromTest; i++) {
      cf.createContext().createProducer().send(queue, "test" + i);
    }

    int numMessagesFromTimer = 10;

    while (true) {
      System.out.println("Received: " + receivedMessages.getMessages());
      if (receivedMessages.getMessages().size() >= numMessagesFromTest + numMessagesFromTimer) {
        break;
      }
      Thread.sleep(100);
    }
    assertTrue(receivedMessages.getMessages().size() >= numMessagesFromTest + numMessagesFromTimer);
  }

}
