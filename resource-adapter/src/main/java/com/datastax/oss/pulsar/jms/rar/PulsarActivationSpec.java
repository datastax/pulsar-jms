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

import com.datastax.oss.pulsar.jms.PulsarDestination;
import com.datastax.oss.pulsar.jms.PulsarQueue;
import com.datastax.oss.pulsar.jms.PulsarTopic;
import java.util.Objects;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.InvalidPropertyException;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterAssociation;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarActivationSpec implements ActivationSpec, ResourceAdapterAssociation {

  private ResourceAdapter resourceAdapter;
  private String destination;
  private String destinationType = "queue";
  private String configuration = "{}";

  public String getConfiguration() {
    return configuration;
  }

  public void setConfiguration(String configuration) {
    log.info("setConfiguration {}", configuration);
    this.configuration = configuration;
  }

  public PulsarDestination getPulsarDestination() {
    if (destinationType == null || destinationType.toLowerCase().contains("queue")) {
      return new PulsarQueue(destination);
    } else {
      return new PulsarTopic(destination);
    }
  }

  public String getDestination() {
    return destination;
  }

  public void setDestination(String destination) {
    this.destination = destination;
  }

  public String getDestinationType() {
    return destinationType;
  }

  public void setDestinationType(String destinationType) {
    this.destinationType = destinationType;
  }

  @Override
  public void validate() throws InvalidPropertyException {
    if (destinationType == null) {
      throw new InvalidPropertyException("invalid null destinationType");
    }
    switch (destinationType) {
      case "queue":
      case "Queue":
      case "topic":
      case "Topic":
      case "javax.jms.Queue":
      case "javax.jms.Topic":
        break;
      default:
        throw new InvalidPropertyException(
            "Invalid destinationType '"
                + destinationType
                + "', only 'queue','topic','javax.jms.Queue','javax.jms.Topic'");
    }
    if (destination == null || destination.isEmpty()) {
      throw new InvalidPropertyException(
          "Invalid '" + destination + "' destination, it must be non empty");
    }
  }

  @Override
  public ResourceAdapter getResourceAdapter() {
    return resourceAdapter;
  }

  @Override
  public void setResourceAdapter(ResourceAdapter resourceAdapter) throws ResourceException {
    this.resourceAdapter = resourceAdapter;
  }

  @Override
  public String toString() {
    return "PulsarActivationSpec{"
        + "destination='"
        + destination
        + '\''
        + ", destinationType='"
        + destinationType
        + '\''
        + ", configuration='"
        + configuration
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PulsarActivationSpec that = (PulsarActivationSpec) o;
    return Objects.equals(destination, that.destination)
        && Objects.equals(destinationType, that.destinationType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(destination, destinationType);
  }

  public String getMergedConfiguration(String configuration) {
    if (this.configuration == null || this.configuration.trim().isEmpty()) {
      return configuration;
    } else {
      return this.configuration;
    }
  }
}
