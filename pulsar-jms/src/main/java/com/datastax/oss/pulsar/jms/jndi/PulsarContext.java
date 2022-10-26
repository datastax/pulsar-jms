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
package com.datastax.oss.pulsar.jms.jndi;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import com.datastax.oss.pulsar.jms.PulsarQueue;
import com.datastax.oss.pulsar.jms.PulsarTopic;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.InvalidNameException;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.OperationNotSupportedException;

public class PulsarContext implements Context {

  public static final String USE_SHARED_JNDICONTEXT = "jms.useSharedJNDIContext";
  public static final String AUTOCLOSE_CONNECTION_FACTORY = "autoCloseConnectionFactory";

  private final Hashtable environment;
  private final Map<String, Object> properties;
  private PulsarConnectionFactory connectionFactory;
  private final boolean autoCloseConnectionFactory;
  private final boolean shared;

  final AtomicInteger referenceCount = new AtomicInteger(0);

  PulsarContext(Hashtable environment) {
    // reference to the original environment
    this.environment = environment;
    this.properties = new HashMap<>();
    environment.forEach(
        (k, v) -> {
          if (k.toString().startsWith("java.naming")) {
            // skip system configuration, only keep Context.PROVIDER_URL
            if (Context.PROVIDER_URL.equals(k.toString())) {
              properties.put("brokerServiceUrl", v);
            }
          } else {
            properties.put(k.toString(), v);
          }
        });
    this.shared = Boolean.parseBoolean(properties.remove(USE_SHARED_JNDICONTEXT) + "");
    this.autoCloseConnectionFactory =
        Boolean.parseBoolean(properties.remove(AUTOCLOSE_CONNECTION_FACTORY) + "");
  }

  private synchronized PulsarConnectionFactory getConnectionFactory() {
    if (connectionFactory != null) {
      return connectionFactory;
    }
    connectionFactory = new PulsarConnectionFactory(properties);
    return connectionFactory;
  }

  @Override
  public Object lookup(String name) throws NamingException {
    Objects.requireNonNull(name, "Name cannot be null");
    if (name.equals("ConnectionFactory")) {
      return getConnectionFactory();
    }
    int index = name.indexOf("/");
    if (index <= 0) {
      throw new InvalidNameException("Name " + name + " is not valid");
    }
    String type = name.substring(0, index);
    String fullDestinationName = name.substring(index + 1);
    if (fullDestinationName.isEmpty()) {
      throw new InvalidNameException("Name " + name + " is not valid");
    }
    switch (type) {
      case "queues":
        return new PulsarQueue(fullDestinationName);
      case "topics":
        return new PulsarTopic(fullDestinationName);
      default:
        throw new InvalidNameException("Name " + name + " is not valid");
    }
  }

  @Override
  public Object lookup(Name name) throws NamingException {
    String composed = Collections.list(name.getAll()).stream().collect(Collectors.joining("/"));
    return lookup(composed);
  }

  @Override
  public void bind(Name name, Object obj) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public void bind(String name, Object obj) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public void rebind(Name name, Object obj) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public void rebind(String name, Object obj) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public void unbind(Name name) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public void unbind(String name) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public void rename(Name oldName, Name newName) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public void rename(String oldName, String newName) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public NamingEnumeration<NameClassPair> list(String name) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public NamingEnumeration<Binding> listBindings(String name) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public void destroySubcontext(Name name) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public void destroySubcontext(String name) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public Context createSubcontext(Name name) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public Context createSubcontext(String name) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public Object lookupLink(Name name) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public Object lookupLink(String name) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public NameParser getNameParser(Name name) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public NameParser getNameParser(String name) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public Name composeName(Name name, Name prefix) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public String composeName(String name, String prefix) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public Object addToEnvironment(String propName, Object propVal) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public Object removeFromEnvironment(String propName) throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public Hashtable<?, ?> getEnvironment() throws NamingException {
    return new Hashtable<>(environment);
  }

  @Override
  public synchronized void close() throws NamingException {
    if (shared && !PulsarInitialContextFactory.releaseSharedContext(environment, this)) {
      return;
    }
    if (connectionFactory != null) {
      if (autoCloseConnectionFactory) {
        connectionFactory.close();
      }
      connectionFactory = null;
    }
  }

  @Override
  public String getNameInNamespace() throws NamingException {
    throw new OperationNotSupportedException();
  }

  @Override
  public String toString() {
    return "PulsarContext{" + System.identityHashCode(this) + '}';
  }
}
