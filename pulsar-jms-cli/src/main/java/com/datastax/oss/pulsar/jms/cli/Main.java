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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import com.datastax.oss.pulsar.jms.Utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@Slf4j
public class Main {

  public static void main(String... args) {
    try {
      JCommander jcommander =
          JCommander.newBuilder()
              .addCommand("createConsumer", new CreateConsumerCmd())
              .addCommand("createDurableConsumer", new CreateDurableConsumerCmd())
              .addCommand("createSharedDurableConsumer", new CreateSharedDurableConsumerCmd())
              .addCommand("produce", new ProduceCmd())
              .args(args)
              .build();
      String cmd = jcommander.getParsedCommand();
      if (cmd == null) {
        jcommander.usage();
        exit(-1);
        return;
      } else {
        JCommander obj = jcommander.getCommands().get(cmd);
        BaseCmd cmdObj = (BaseCmd) obj.getObjects().get(0);

        try {
          try {
            cmdObj.run();
          } finally {
            cmdObj.dispose();
          }
          exit(0);
        } catch (IllegalArgumentException e) {
          System.err.println(e.getMessage());
          System.err.println();
          exit(-1);
        } catch (Exception e) {
          e.printStackTrace();
          exit(-1);
        }
      }
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      System.err.println();
      exit(-1);
    }
  }

  private static void exit(int code) {
    System.exit(code);
  }

  private abstract static class BaseCmd {

    private ArrayList<AutoCloseable> closeables = new ArrayList<>();

    @Parameter(
      description = "YAML Configuration file",
      names = {"--config", "-c"},
      required = false
    )
    private String config;

    protected PulsarConnectionFactory factory;
    protected JMSContext context;

    protected PulsarConnectionFactory getFactory() throws Exception {
      if (factory == null) {
        Map<String, Object> configuration = new HashMap<>();
        if (config != null) {
          ObjectMapper yaml = ObjectMapperFactory.create();
          File file = new File(config);
          configuration = yaml.readValue(file, Map.class);
          log.info("Configuration {}", configuration);
        }
        factory = new PulsarConnectionFactory(configuration);
        closeables.add(factory);
      }
      return factory;
    }

    protected JMSContext getContext() throws Exception {
      if (context != null) {
        return context;
      }
      PulsarConnectionFactory factory = getFactory();
      context = factory.createContext();
      closeables.add(context);
      return context;
    }

    abstract void run() throws Exception;

    public void dispose() throws Exception {
      Collections.reverse(closeables);
      for (AutoCloseable c : closeables) {
        c.close();
      }
    }
  }

  public abstract static class TopicBasedCmd extends BaseCmd {

    @Parameter(
      description = "Destination",
      names = {"--destination", "-d"},
      required = true
    )
    private String destination;

    @Parameter(
      description = "Destination Type",
      names = {"--destination-type", "-dt"},
      required = false
    )
    private String destinationType = "queue";

    protected Destination getDestination(boolean requireTopic) throws Exception {
      switch (destinationType) {
        case "queue":
          if (requireTopic) {
            throw new IllegalArgumentException(
                "createSharedDurableConsumer is available only for JMS Topics, use -t topic");
          }
          return getContext().createQueue(destination);
        case "topic":
          return getContext().createTopic(destination);
        default:
          throw new IllegalArgumentException("Invalid destination type " + destinationType);
      }
    }
  }

  @Parameters(commandDescription = "Consume from destination, using createConsumer")
  public static class CreateConsumerCmd extends TopicBasedCmd {

    @Parameter(
      description = "Selector",
      names = {"--selector", "-s"},
      required = false
    )
    private String selector;

    @Parameter(
      description = "Number for messages (0 for infinite)",
      names = {"--num-messages", "-n"},
      required = false
    )
    private int numMessages = 0;

    public void run() throws Exception {
      Destination destination = getDestination(false);
      JMSContext context = getContext();
      CountDownLatch countDownLatch =
          new CountDownLatch(numMessages > 0 ? numMessages : Integer.MAX_VALUE);
      context
          .createConsumer(destination, selector)
          .setMessageListener(
              new MessageListener() {
                @Override
                public void onMessage(Message message) {
                  printMessage(message);
                  countDownLatch.countDown();
                }
              });
      countDownLatch.await();
    }
  }

  @Parameters(commandDescription = "Produce some messages")
  public static class ProduceCmd extends TopicBasedCmd {

    @Parameter(
      description = "Type",
      names = {"--message-type", "-mt"},
      required = false
    )
    private String messageType = "text";

    @Parameter(
      description = "Payload",
      names = {"--payload"},
      required = false
    )
    private String payload = "test";

    @Parameter(
      description = "Properties",
      names = {"--property", "-p"},
      required = false
    )
    private List<String> properties;

    @Parameter(
      description = "Number for messages (0 for infinite)",
      names = {"--num-messages", "-n"},
      required = false
    )
    private int numMessages = 0;

    public void run() throws Exception {
      Destination destination = getDestination(false);
      JMSContext context = getContext();
      if (numMessages <= 0) {
        numMessages = Integer.MAX_VALUE;
      }
      Map<String, Object> properties = new HashMap<>();
      if (this.properties != null) {
        this.properties.forEach(
            p -> {
              int equals = p.indexOf("=");
              if (equals >= 0) {
                properties.put(p.substring(0, equals), p.substring(equals + 1));
              } else {
                properties.put(p, "");
              }
            });
      }
      for (int i = 0; i < numMessages; i++) {
        JMSProducer producer = context.createProducer();
        properties.forEach(
            (k, v) -> {
              producer.setProperty(k, v);
            });
        switch (messageType) {
          case "text":
            producer.send(destination, payload);
            break;
          case "bytes":
            producer.send(destination, payload.getBytes(StandardCharsets.UTF_8));
            break;
          default:
            throw new IllegalArgumentException(
                "Invalid message type, only 'text' and 'bytes' are supported");
        }
      }
    }
  }

  @Parameters(commandDescription = "Consume from destination, using createDurableConsumer")
  public static class CreateDurableConsumerCmd extends TopicBasedCmd {

    @Parameter(
      description = "Selector",
      names = {"--selector", "-s"},
      required = false
    )
    private String selector;

    @Parameter(
      description = "Subscription",
      names = {"--subscription", "-sub"},
      required = true
    )
    private String subscription;

    @Parameter(
      description = "Number for messages (0 for infinite)",
      names = {"--num-messages", "-n"},
      required = false
    )
    private int numMessages = 0;

    public void run() throws Exception {
      Destination destination = getDestination(true);
      JMSContext context = getContext();
      CountDownLatch countDownLatch =
          new CountDownLatch(numMessages > 0 ? numMessages : Integer.MAX_VALUE);
      context
          .createDurableConsumer((Topic) destination, subscription, selector, false)
          .setMessageListener(
              new MessageListener() {
                @Override
                public void onMessage(Message message) {
                  printMessage(message);
                  countDownLatch.countDown();
                }
              });
      countDownLatch.await();
    }
  }

  @Parameters(commandDescription = "Consume from destination, using createDurableConsumer")
  public static class CreateSharedDurableConsumerCmd extends TopicBasedCmd {

    @Parameter(
      description = "Selector",
      names = {"--selector", "-s"},
      required = false
    )
    private String selector;

    @Parameter(
      description = "Subscription",
      names = {"--subscription", "-sub"},
      required = true
    )
    private String subscription;

    @Parameter(
      description = "Number for messages (0 for infinite)",
      names = {"--num-messages", "-n"},
      required = false
    )
    private int numMessages = 0;

    public void run() throws Exception {
      Destination destination = getDestination(true);
      JMSContext context = getContext();
      CountDownLatch countDownLatch =
          new CountDownLatch(numMessages > 0 ? numMessages : Integer.MAX_VALUE);
      context
          .createSharedDurableConsumer((Topic) destination, subscription, selector)
          .setMessageListener(
              new MessageListener() {
                @Override
                public void onMessage(Message message) {
                  printMessage(message);
                  countDownLatch.countDown();
                }
              });
      countDownLatch.await();
    }
  }

  private static void printMessage(Message message) {
    Utils.runtimeException(
        () -> {
          Map<String, Object> properties =
              Collections.list(
                      (Enumeration<String>)
                          Utils.runtimeException(() -> message.getPropertyNames()))
                  .stream()
                  .collect(
                      Collectors.toMap(
                          Function.identity(),
                          s -> Utils.runtimeException(() -> message.getObjectProperty(s))));
          if (message instanceof BytesMessage) {
            log.info("Received BytesMessage with properties {}", properties);
          } else if (message instanceof TextMessage) {
            log.info(
                "Received TextMessage {} with properties {}",
                ((TextMessage) message).getText(),
                properties);
          } else {
            log.info(
                "Received {} with properties {}", message.getClass().getSimpleName(), properties);
          }
        });
  }
}
