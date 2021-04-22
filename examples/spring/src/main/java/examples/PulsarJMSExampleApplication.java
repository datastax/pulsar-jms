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

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.core.JmsTemplate;

@SpringBootApplication
@EnableJms
@EnableConfigurationProperties(PulsarJMSConfigurationProperties.class)
public class PulsarJMSExampleApplication implements CommandLineRunner {

  private final JmsTemplate jmsTemplate;

  public PulsarJMSExampleApplication(JmsTemplate jmsTemplate) {
    this.jmsTemplate = jmsTemplate;
  }

  @Override
  public void run(String... args) {
    // Send a message with a POJO - the template use the message converter
    for (int i = 0; i < 100; i++) {
      jmsTemplate.convertAndSend("IN_QUEUE", new Email("info" + i + "@example.com", "Hello"));
    }

    // in the meantime the Listener will receive the message and write to the console
  }

  public static void main(String[] args) {
    SpringApplication.run(PulsarJMSExampleApplication.class, args);
  }
}
