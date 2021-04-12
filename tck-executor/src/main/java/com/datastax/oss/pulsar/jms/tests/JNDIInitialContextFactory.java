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
package com.datastax.oss.pulsar.jms.tests;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import com.datastax.oss.pulsar.jms.PulsarQueue;
import com.datastax.oss.pulsar.jms.PulsarTopic;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

public class JNDIInitialContextFactory implements InitialContextFactory {

  @Override
  public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
    try {
      Context context =
          (Context)
              Proxy.newProxyInstance(
                  this.getClass().getClassLoader(),
                  new Class[] {Context.class},
                  new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args)
                        throws Throwable {
                      if (method.getName().equals("lookup")
                          && args.length == 1
                          && args[0] instanceof String) {
                        return lookup((String) args[0]);
                      }
                      throw new UnsupportedOperationException(
                          "method " + method + " with " + Arrays.toString(args));
                    }
                  });
      return context;
    } catch (Exception err) {
      throw (NamingException) (new NamingException("Generic error").initCause(err));
    }
  }

  private static PulsarConnectionFactory buildConnectionFactory(String name) throws Exception {
    Map<String, Object> configuration = new HashMap<>();
    configuration.put("enableTransaction", true);
    configuration.put("forceDeleteTemporaryDestinations", true);

    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("batchingEnabled", false);

    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("receiverQueueSize", 1);
    configuration.put("consumerConfig", consumerConfig);
    configuration.put("producerConfig", producerConfig);
    if (name.equals("DURABLE_SUB_CONNECTION_FACTORY")) {
      // see
      // com.sun.ts.tests.jms.core20.jmscontexttopictests.Client.verifyClientIDOnAdminConfiguredIDTest
      configuration.put("jms.clientId", "cts");
    }
    return new PulsarConnectionFactory(configuration);
  }

  public Object lookup(String name) throws Exception {
    System.out.println("lookup " + name);
    switch (name) {
      case "MyQueueConnectionFactory":
      case "MyTopicConnectionFactory":
      case "MyConnectionFactory":
      case "DURABLE_SUB_CONNECTION_FACTORY":
        return buildConnectionFactory(name);
      case "MY_QUEUE":
      case "MY_QUEUE2":
      case "testQ0":
      case "testQ1":
      case "testQ2":
      case "testQueue2":
      case "Q2":
        return new PulsarQueue("persistent://public/default/" + name);
      case "MY_TOPIC":
      case "MY_TOPIC2":
      case "testT0":
      case "testT1":
      case "testT2":
        return new PulsarTopic("persistent://public/default/" + name);
    }
    throw new RuntimeException("lookup " + name);
  }
}
