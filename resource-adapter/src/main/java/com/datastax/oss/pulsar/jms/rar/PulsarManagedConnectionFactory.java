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
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Set;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.security.auth.Subject;

public class PulsarManagedConnectionFactory implements ManagedConnectionFactory {

  private transient PrintWriter printWriter = new PrintWriter(System.out);

  @Override
  public Object createConnectionFactory(ConnectionManager connectionManager)
      throws ResourceException {
    new Exception("createConnectionFactory " + connectionManager).printStackTrace();
    ;
    try {
      return new PulsarConnectionFactory(new HashMap<>());
    } catch (Exception err) {
      throw new ResourceException(err);
    }
  }

  @Override
  public Object createConnectionFactory() throws ResourceException {
    new Exception("createConnectionFactory").printStackTrace();
    ;
    try {
      return new PulsarConnectionFactory(new HashMap<>());
    } catch (Exception err) {
      throw new ResourceException(err);
    }
  }

  @Override
  public ManagedConnection createManagedConnection(
      Subject subject, ConnectionRequestInfo connectionRequestInfo) throws ResourceException {
    new Exception("createManagedConnection").printStackTrace();
    ;
    return null;
  }

  @Override
  public ManagedConnection matchManagedConnections(
      Set set, Subject subject, ConnectionRequestInfo connectionRequestInfo)
      throws ResourceException {
    new Exception("matchManagedConnections").printStackTrace();
    ;
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
}
