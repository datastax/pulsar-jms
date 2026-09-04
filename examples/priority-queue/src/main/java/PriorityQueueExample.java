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

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import jakarta.jms.*;
import java.util.HashMap;
import java.util.Map;

public class PriorityQueueExample {
  public static void main(String[] args) throws Exception {
    // Configure connection with priority enabled
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", "http://localhost:8080");
    properties.put("brokerServiceUrl", "pulsar://localhost:6650");

    // Enable JMS Priority support
    properties.put("jms.enableJMSPriority", true);
    properties.put("jms.priorityMapping", "linear"); // or "non-linear"

    // Configure consumer queue size
    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("receiverQueueSize", 100);
    properties.put("consumerConfig", consumerConfig);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);
        Connection connection = factory.createConnection()) {

      connection.start();

      try (Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE)) {
        Queue queue = session.createQueue("priority-test-queue");

        // Send messages with different priorities
        try (MessageProducer producer = session.createProducer(queue)) {
          // Send low priority messages
          for (int i = 0; i < 50; i++) {
            TextMessage msg = session.createTextMessage("Low priority message " + i);
            producer.setPriority(4); // Low priority
            producer.send(msg);
            System.out.println("Sent: " + msg.getText() + " with priority 4");
          }

          // Send high priority messages
          for (int i = 0; i < 50; i++) {
            TextMessage msg = session.createTextMessage("High priority message " + i);
            producer.setPriority(9); // High priority
            producer.send(msg);
            System.out.println("Sent: " + msg.getText() + " with priority 9");
          }
        }

        // Receive messages - high priority should come first
        try (MessageConsumer consumer = session.createConsumer(queue)) {
          System.out.println("\n--- Receiving Messages ---");
          for (int i = 0; i < 100; i++) {
            TextMessage msg = (TextMessage) consumer.receive(5000);
            if (msg != null) {
              System.out.println(
                  "Received: " + msg.getText() + " with priority " + msg.getJMSPriority());
            }
          }
        }
      }
    }
  }
}
