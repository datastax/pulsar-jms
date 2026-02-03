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

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import java.util.HashMap;
import java.util.Map;
import javax.jms.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;

@Configuration
@Slf4j
public class PulsarJMSConfiguration {

  @Bean
  public ConnectionFactory connectionFactory(PulsarJMSConfigurationProperties pulsarConfiguration)
      throws Exception {
    Map<String, Object> configuration = new HashMap<>(pulsarConfiguration.getConfiguration());
    log.info("Configuration {}", configuration);
    return new PulsarConnectionFactory(configuration);
  }

  @Bean
  public JmsListenerContainerFactory<?> myFactory(
      ConnectionFactory connectionFactory,
      DefaultJmsListenerContainerFactoryConfigurer configurer) {
    DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
    // This provides all boot's default to this factory, including the message converter
    configurer.configure(factory, connectionFactory);

    // By default in Pulsar transactions are disabled
    // add enableTransaction=true to your PulsarConnectionFactory configuration
    // and also you will have to enable transaction support in your Pulsar broker
    factory.setSessionTransacted(false);

    // You could still override some of Boot's default if necessary.
    return factory;
  }

  @Bean // Serialize message content to json using TextMessage
  public MessageConverter jacksonJmsMessageConverter() {
    MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
    converter.setTargetType(MessageType.TEXT);
    converter.setTypeIdPropertyName("_type");
    return converter;
  }
}
