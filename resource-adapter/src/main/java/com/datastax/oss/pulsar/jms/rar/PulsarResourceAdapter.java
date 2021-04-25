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

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import javax.jms.JMSException;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAResource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarResourceAdapter implements ResourceAdapter {

  private final Map<String, PulsarConnectionFactory> outboundConnections =
      new ConcurrentHashMap<>();
  private final Set<PulsarMessageEndpoint> endpoints = new CopyOnWriteArraySet<>();

  @Override
  public void start(BootstrapContext bootstrapContext) throws ResourceAdapterInternalException {}

  public PulsarConnectionFactory getPulsarConnectionFactory(String configuration) {
    return outboundConnections.computeIfAbsent(
        configuration,
        (config) -> {
          log.info("startPulsarConnectionFactory {}", config);
          try {
            PulsarConnectionFactory res = new PulsarConnectionFactory();
            res.setJsonConfiguration(config);
            return res;
          } catch (JMSException err) {
            log.error("Cannot start a connection factory with configuratiobn {}", config, err);
            throw new RuntimeException(err);
          }
        });
  }

  @Override
  public void stop() {
    for (PulsarConnectionFactory factory : outboundConnections.values()) {
      factory.close();
    }
    for (PulsarMessageEndpoint endpoint : endpoints) {
      endpoint.stop();
    }
  }

  @Override
  public void endpointActivation(
      MessageEndpointFactory messageEndpointFactory, ActivationSpec activationSpec)
      throws ResourceException {
    try {
      log.info("Activate endpoint {} {}", activationSpec, messageEndpointFactory);
      PulsarActivationSpec pulsarActivationSpec = (PulsarActivationSpec) activationSpec;
      PulsarConnectionFactory connectionFactory =
          getPulsarConnectionFactory(pulsarActivationSpec.getConfiguration());
      connectionFactory.setJsonConfiguration(pulsarActivationSpec.getConfiguration());
      PulsarMessageEndpoint endpoint =
          new PulsarMessageEndpoint(
              connectionFactory, messageEndpointFactory, pulsarActivationSpec);
      endpoints.add(endpoint);
      endpoint.start();
    } catch (Throwable t) {
      throw new ResourceException(t);
    }
  }

  @Override
  public void endpointDeactivation(
      MessageEndpointFactory messageEndpointFactory, ActivationSpec activationSpec) {
    for (Iterator<PulsarMessageEndpoint> it = endpoints.iterator(); it.hasNext(); ) {
      PulsarMessageEndpoint end = it.next();
      if (end.matches(messageEndpointFactory, activationSpec)) {
        log.info(
            "endpointDeactivation {} {} endpoint {}", messageEndpointFactory, activationSpec, end);
        end.stop();
        it.remove();
        ;
      }
    }
  }

  @Override
  public XAResource[] getXAResources(ActivationSpec[] activationSpecs) throws ResourceException {
    /*
        Upon being called by the application server during crash recovery through the getXAResources method, the resource adapter must return an array of XAResource objects, each of which represents a unique resource manager.
    The resource adapter may return null if it does not implement the XAResource interface. Otherwise, it must return an array of XAResource objects, each of which represents a unique resource manager that was used by the endpoint applications. The resource adapter may throw a ResourceException if it encounters an error condition.
         */
    return null;
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
