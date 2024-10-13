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
package com.datastax.oss.pulsar.jms.rar;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import com.datastax.oss.pulsar.jms.PulsarJMSContext;
import jakarta.jms.Destination;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class ResourceAdapterTest {

  @Test
  public void testPoolConnectionFactory() throws Exception {

    PulsarResourceAdapter adapter =
        new PulsarResourceAdapter() {
          @Override
          protected PulsarConnectionFactory buildConnectionFactory(String config) {
            return mock(PulsarConnectionFactory.class);
          }
        };
    adapter.setConfiguration("foo");

    PulsarConnectionFactory pulsarConnectionFactoryWithDefaultConfiguration =
        adapter.getPulsarConnectionFactory(null);
    PulsarConnectionFactory pulsarConnectionFactoryWithDefaultConfiguration2 =
        adapter.getPulsarConnectionFactory(null);
    assertSame(
        pulsarConnectionFactoryWithDefaultConfiguration2,
        pulsarConnectionFactoryWithDefaultConfiguration);
    PulsarConnectionFactory pulsarConnectionFactoryWithDefaultConfiguration3 =
        adapter.getPulsarConnectionFactory("");
    assertSame(
        pulsarConnectionFactoryWithDefaultConfiguration2,
        pulsarConnectionFactoryWithDefaultConfiguration3);

    PulsarConnectionFactory factory1 = adapter.getPulsarConnectionFactory("one");
    assertNotSame(pulsarConnectionFactoryWithDefaultConfiguration2, factory1);

    PulsarConnectionFactory factory2 = adapter.getPulsarConnectionFactory("one");
    assertSame(factory1, factory2);

    adapter.stop();

    verify(pulsarConnectionFactoryWithDefaultConfiguration, times(1)).close();
    verify(factory1, times(1)).close();
  }

  @Test
  public void testActivateEndpoint() throws Exception {

    PulsarResourceAdapter adapter =
        new PulsarResourceAdapter() {
          @Override
          protected PulsarConnectionFactory buildConnectionFactory(String config) {
            PulsarConnectionFactory res = mock(PulsarConnectionFactory.class);
            JMSContext context = mock(PulsarJMSContext.class);
            when(context.createConsumer(any(Destination.class)))
                .thenReturn(mock(JMSConsumer.class));
            when(res.createContext(anyInt())).thenReturn(context);
            return res;
          }

          @Override
          protected PulsarMessageEndpoint buildMessageEndpoint(
              MessageEndpointFactory messageEndpointFactory,
              PulsarActivationSpec pulsarActivationSpec,
              PulsarConnectionFactory connectionFactory)
              throws UnavailableException {
            return spy(
                super.buildMessageEndpoint(
                    messageEndpointFactory, pulsarActivationSpec, connectionFactory));
          }
        };
    adapter.setConfiguration("foo");

    MessageEndpointFactory factory = mock(MessageEndpointFactory.class);
    PulsarActivationSpec specs = new PulsarActivationSpec();
    specs.setDestination("MyQueue");
    adapter.endpointActivation(factory, specs);

    PulsarMessageEndpoint endpoint = adapter.getEndpoints().iterator().next();
    assertSame(specs, endpoint.getActivationSpec());
    assertSame(factory, endpoint.getMessageEndpointFactory());

    verify(endpoint, times(1)).start();

    adapter.stop();
    verify(endpoint, times(1)).stop();
  }
}
