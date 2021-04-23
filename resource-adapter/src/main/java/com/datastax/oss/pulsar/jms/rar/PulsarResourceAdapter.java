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

  private final Set<PulsarConnectionFactory> outboundConnections = new CopyOnWriteArraySet<>();
  private final Map<ActivationSpec, PulsarMessageEndpoint> endpoints = new ConcurrentHashMap<>();

  @Override
  public void start(BootstrapContext bootstrapContext) throws ResourceAdapterInternalException {}

  public PulsarConnectionFactory startPulsarConnectionFactory(String configuration) {
    log.info("startPulsarConnectionFactory {}", configuration);
    try {
      PulsarConnectionFactory res = new PulsarConnectionFactory();
      res.setJsonConfiguration(configuration);
      outboundConnections.add(res);
      return res;
    } catch (JMSException err) {
      log.error("Cannot start a connection factory with configuratiobn {}", configuration, err);
      throw new RuntimeException(err);
    }
  }

  @Override
  public void stop() {
    for (PulsarConnectionFactory factory : outboundConnections) {
      factory.close();
    }
    for (PulsarMessageEndpoint endpoint : endpoints.values()) {
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
      PulsarConnectionFactory connectionFactory = new PulsarConnectionFactory();
      connectionFactory.setJsonConfiguration(pulsarActivationSpec.getConfiguration());
      PulsarMessageEndpoint endpoint =
          new PulsarMessageEndpoint(
              connectionFactory, messageEndpointFactory, pulsarActivationSpec);
      endpoints.put(activationSpec, endpoint);
      endpoint.start();
    } catch (Throwable t) {
      throw new ResourceException(t);
    }
  }

  @Override
  public void endpointDeactivation(
      MessageEndpointFactory messageEndpointFactory, ActivationSpec activationSpec) {
    PulsarMessageEndpoint removed = endpoints.remove(activationSpec);
    log.info(
        "endpointDeactivation {} {} endpoint {}", messageEndpointFactory, activationSpec, removed);
    if (removed != null) {
      removed.stop();
    }
  }

  @Override
  public XAResource[] getXAResources(ActivationSpec[] activationSpecs) throws ResourceException {
    return new XAResource[0];
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
