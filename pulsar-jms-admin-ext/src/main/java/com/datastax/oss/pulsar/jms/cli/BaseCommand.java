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
package com.datastax.oss.pulsar.jms.cli;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import com.datastax.oss.pulsar.jms.PulsarJMSContext;
import com.datastax.oss.pulsar.jms.api.JMSAdmin;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.jms.JMSException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.admin.cli.extensions.CommandExecutionContext;
import org.apache.pulsar.admin.cli.extensions.CustomCommand;
import org.apache.pulsar.admin.cli.extensions.ParameterDescriptor;
import org.apache.pulsar.admin.cli.extensions.ParameterType;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.helpers.MessageFormatter;

@Slf4j
abstract class BaseCommand implements CustomCommand {

  private static final ObjectMapper YAML_PARSER = new ObjectMapper(new YAMLFactory());
  private Map<String, Object> currentParameters;
  private CommandExecutionContext currentContext;

  @Override
  public final List<ParameterDescriptor> parameters() {
    List<ParameterDescriptor> result = new ArrayList<>();
    defineParameters(result);
    return result;
  }

  protected void defineParameters(List<ParameterDescriptor> list) {
    list.add(
        ParameterDescriptor.builder()
            .description("JMS YAML Configuration file")
            .type(ParameterType.STRING)
            .names(Arrays.asList("--jms-config", "-jc"))
            .required(false)
            .build());
  }

  protected abstract void executeInternal() throws Exception;

  @Override
  public boolean execute(Map<String, Object> parameters, CommandExecutionContext context)
      throws Exception {
    this.currentParameters = parameters;
    this.currentContext = context;
    try {
      executeInternal();
    } catch (JMSException err) {
      println("Error: " + err.getMessage());
      log.debug("Error", err);
      return false;
    } finally {
      this.currentParameters = null;
      this.currentContext = null;
      dispose();
    }
    return true;
  }

  protected String getStringParameter(String name, String defaultValue) {
    Object value = currentParameters.get(name);
    if (value == null) {
      return defaultValue;
    }
    return value.toString();
  }

  private ArrayList<AutoCloseable> closeables = new ArrayList<>();
  protected PulsarConnectionFactory factory;
  protected PulsarJMSContext context;

  protected void println(String template, Object... parameters) {
    System.out.println(MessageFormatter.arrayFormat(template, parameters).getMessage());
  }

  protected PulsarConnectionFactory getFactory() throws Exception {
    return getFactory(false);
  }

  protected JMSAdmin getAdmin() throws Exception {
    return getFactory().getAdmin();
  }

  protected JMSAdmin getAdmin(boolean createClient) throws Exception {
    return getFactory(createClient).getAdmin();
  }

  protected PulsarConnectionFactory getFactory(boolean createClient) throws Exception {
    if (factory != null && createClient && factory.getPulsarClient() == null) {
      factory.close();
      factory = null;
    }
    if (factory == null) {
      String config = getStringParameter("--jms-config", "");

      final Map<String, Object> configuration = new HashMap<>();
      // apply client.conf configuration
      currentContext
          .getConfiguration()
          .forEach(
              (k, v) -> {
                if (isAllowedByPulsarConnectionFactory(k + "")) {
                  configuration.put(k + "", v);
                }
              });
      if (config != null && !config.isEmpty()) {

        File file = new File(config);
        log.debug("Reading {}", file.getAbsolutePath());
        configuration.putAll(YAML_PARSER.readValue(file, Map.class));
      }
      log.debug("Configuration {}", configuration);
      if (createClient) {
        factory = new PulsarConnectionFactory(configuration);
      } else {
        factory =
            new PulsarConnectionFactory(configuration) {
              @Override
              protected PulsarClient buildPulsarClient(ClientBuilder builder)
                  throws PulsarClientException {
                // PulsarClient is not needed, do not create it
                return null;
              }
            };
      }
      closeables.add(factory);
    }
    return factory;
  }

  private static boolean isAllowedByPulsarConnectionFactory(String key) {
    // keep from client.conf the most useful parameters
    // if you want to override other values you can use --jms-config
    switch (key) {
      case "brokerServiceUrl":
      case "webServiceUrl":
      case "authPlugin":
      case "authParams":
        return true;
      default:
        return false;
    }
  }

  protected PulsarJMSContext getContext() throws Exception {
    if (context != null) {
      return context;
    }
    PulsarConnectionFactory factory = getFactory();
    context = (PulsarJMSContext) factory.createContext();
    closeables.add(context);
    return context;
  }

  private void dispose() throws Exception {
    Collections.reverse(closeables);
    for (AutoCloseable c : closeables) {
      c.close();
    }
  }
}
