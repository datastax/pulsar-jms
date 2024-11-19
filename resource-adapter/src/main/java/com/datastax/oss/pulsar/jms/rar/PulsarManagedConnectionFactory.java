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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jakarta.resource.ResourceException;
import jakarta.resource.spi.ConnectionManager;
import jakarta.resource.spi.ConnectionRequestInfo;
import jakarta.resource.spi.ManagedConnection;
import jakarta.resource.spi.ManagedConnectionFactory;
import jakarta.resource.spi.ResourceAdapter;
import jakarta.resource.spi.ResourceAdapterAssociation;
import java.io.PrintWriter;
import java.util.Set;
import javax.security.auth.Subject;
import lombok.extern.slf4j.Slf4j;

@SuppressFBWarnings("DM_DEFAULT_ENCODING")
@Slf4j
public class PulsarManagedConnectionFactory
    implements ManagedConnectionFactory, ResourceAdapterAssociation {

  private static final long serialVersionUID = 0;

  private transient PrintWriter printWriter = new PrintWriter(System.out);

  private transient PulsarResourceAdapter resourceAdapter;
  private String configuration = "{}";

  public String getConfiguration() {
    return configuration;
  }

  public void setConfiguration(String configuration) {
    log.info("setConfiguration {}", configuration);
    this.configuration = configuration;
  }

  @Override
  public ResourceAdapter getResourceAdapter() {
    return resourceAdapter;
  }

  @Override
  public void setResourceAdapter(ResourceAdapter resourceAdapter) throws ResourceException {
    this.resourceAdapter = (PulsarResourceAdapter) resourceAdapter;
  }

  String getMergedConfiguration() {
    if (this.configuration == null
        || this.configuration.trim().isEmpty()
        || this.configuration.replace(" ", "").equals("{}")) { // empty "{}") {
      return resourceAdapter.getConfiguration();
    } else {
      return configuration;
    }
  }

  @Override
  public Object createConnectionFactory(ConnectionManager connectionManager)
      throws ResourceException {
    return resourceAdapter.getPulsarConnectionFactory(getMergedConfiguration());
  }

  @Override
  public Object createConnectionFactory() throws ResourceException {
    return resourceAdapter.getPulsarConnectionFactory(getMergedConfiguration());
  }

  @Override
  public ManagedConnection createManagedConnection(
      Subject subject, ConnectionRequestInfo connectionRequestInfo) throws ResourceException {
    throw new ResourceException("Not implemented");
  }

  @Override
  public ManagedConnection matchManagedConnections(
      Set set, Subject subject, ConnectionRequestInfo connectionRequestInfo)
      throws ResourceException {
    // If the resource adapter cannot find an acceptable ManagedConnection instance,
    // it returns a null value. In this case, the application server requests
    // the resource adapter to create a new connection instance.
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter printWriter) throws ResourceException {
    this.printWriter = printWriter;
  }

  @Override
  public PrintWriter getLogWriter() throws ResourceException {
    return printWriter;
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
