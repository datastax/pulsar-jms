# DataStax Apache Pulsar JMS Client

This is a Java library that implements the JMS 2.0 (Java Messaging Service ®) over the Apache Pulsar® Java Client.

This library is Open Source Software, Apache 2 licensed.

Please refer to the [official JMS documentation](https://jakarta.ee/specifications/messaging/2.0/) in order to learn about JMS.
This [website](https://javaee.github.io/jms-spec/) is useful as well as it contains the former JMS 2.0 specifications before the Jakarta transitions.

You can find [here](https://pulsar.apache.org) the official Apache Pulsar documentation.

## Installation

In order to use this library just add this dependency:

```
<dependency>
   <artifactId>pulsar-jms</artifactId>
   <groupId>com.datastax.oss</groupId>
   <version>VERSION</version>
</dependency>
```

## Getting started

In JMS you need these three concepts to get started:
- a ConnectionFactory: use com.datastax.oss.pulsar.jms.PulsarConnectionFactory
- a Queue: use com.datastax.oss.pulsar.jms.PulsarQueue (or better Session#createQueue)
- a Topic: use com.datastax.oss.pulsar.jms.PulsarTopic (or better Session#createTopic)

This is how you access them for Pulsar

```
   Map<String, Object> configuration = new HashMap<>();
   configuration.put("webServiceUrl", "http://localhost:8080"); 
   configuration.put("brokerServiceUrl", "pulsar://localhost:6650"); 
   PulsarConnectionFactory factory = new PulsarConnectionFactory(configuration);
   
   try (JMSContext context = factory.createContext()) {
       Destination destination = context.createQueue("persistent://public/default/test");
       context.createProducer().send(destination, "text");
       try (JMSConsumer consumer = context.createConsumer(destination)) {
            String message = consumer.receiveBody(String.class);
            ....
       }
   }`
```

Ensure you have a Pulsar service running at http://localhost:8080 before trying out the example.

You can for instance run Pulsar Standalone using docker

```
docker run --name pulsar-jms-runner -d -p 8080:8080 -p 6650:6650 apachepulsar/pulsar:2.7.1 /pulsar/bin/pulsar standalone
```

## JakartaEE® Resource Adapter

In order to use this JMS Client inside a JakartaEE® or JavaEE® application you can use the ResourceAdapter.

The source code of the resource adapter is in this repository, in this [directory](resource-adapter).

## Examples

In this repository you can find examples about how to run the client:

- with [Spring Boot®](examples/spring)
- with [Payara Micro®](examples/payara-micro)
- with [Apache TomEE®](resource-adapter-tests)

## Building from the sources

If you want to develop and test this library you need to build the jar from sources.

This is a standard Maven project, so use the default commands:

       mvn clean install

## Running the TCK

You can download the TCK [here](https://jakarta.ee/specifications/messaging/2.0/).
This repository contains a copy of the TCK in order to automate the execution of the tests.

In the tck-executor module you find:
- the Java Code needed to initialize the TCK (`JNDIInitialContextFactory.java`)
- the configuration file for the TCK runner: `ts.jte`
- a file that contains the excluded tests that cannot pass with this client: `ts.jtx` (see below for the list of unsupported features)
- a couple of scripts to run Apache Pulsar 2.7.1, configure the Transaction Coordinator and prepare for the execution of the TCK

With this command you build the package, run unit tests and then you run the TCK

       mvn clean install -Prun-tck

In order to only run the TCK

       mvn clean install -Prun-tck -am -DskipTests -pl tck-executor

This library, when you run it using Apache Pulsar 2.7.x passes most of the TCK, except from the few tests around the need
of supporting globally unique subscription names, those tests are skipped by the configuration applied to the TCK runner.

## Mapping Apache Pulsar Concepts to JMS Specifications

JMS Specifications are built over the concepts of **Topics** and **Queues**, but in Pulsar we only have a general concept of **Topic**
that can model both of the two domains.

In Pulsar there is no concept of Queue: this JMS **client will treat as Queue your Pulsar topic when
you use the Queue related JMS API**. There is no strict, cluster wide, verification that you are accessing
a JMS Queue using the Topic API and vice versa.

In JMS a **Topic** is written by many **Producers** and read by many **Consumers**, that share one or many **Subscriptions**.
Subscriptions may be **Durable** or **Non-Durable**, **Shared** or **Non-Shared**. So the same message may be received and processed by more than one Consumer. 

This is the mapping between JMS Consumer/Subscriptions and Apache Pulsar Consumers for Topics:

| JMS Concept  | Pulsar Concept |
| ------------- | ------------- |
| Topic | Persistent topic |
| Consumer | Exclusive Non-Durable subscription with random name (UUID) |
| DurableConsumer | Exclusive Durable subscription with the given name + clientId |
| SharedConsumer | Shared Non-Durable subscription with the given name + clientId |
| SharedDurableConsumer | Shared Durable subscription with the given name + clientId |
| DurableSubscriber | Exclusive Durable subscription with the given name + clientId |

As in Pulsar 2.7.x Delayed messages to not work on Exclusive subscriptions you can force the usage of Shared Non-Durable subscriptions
for simple Consumers by setting `jms.useExclusiveSubscriptionsForSimpleConsumers=false` on your client configuration.

In JMS a **Queue** is written by many **Producers** but only one **Consumer** eventually processed each message.

In order to implement this behaviour the first time you create a Consumer over a Queue the JMS client creates a Durable Subscription
named **jms-queue** from the beginning (initial position = Earliest) of the Pulsar Topic.

Every access to the Queue pass through this Shared subscription and this guarantees that only one Consumer receives and process each message.

This is the mapping between JMS Consumer/Subscriptions and Apache Pulsar Consumers for Queue:

| JMS Concept  | Pulsar Concept |
| ------------- | ------------- |
| Queue | Persistent topic |
| Consumer | Shared Durable subscription with name 'jms-queue' |
| QueueBrowser | Pulsar Reader on the topic, beginning from the next message on 'jms-queue' subscription |

You can change the name of the shared subscription using `jms.queueSubscriptionName` configuration parameter, but you must ensure that you change 
this configuration on every client.

In order to implement QueueBrowser we use the Pulsar Reader API, starting from the next message available on the 'jms-queue' subscription.
In order to peek the next message we use the Pulsar Admin API `peekMessages`.
Sometimes it happens that the 'peekMessages' API still returns the last consumed message of the subscription, so the QueueBrowser may return non-accurate
results.

## Pulsar Message Key and JMSXGroupID

The special JMSXGroupID property is defined in the JMS specs as a way to group the messages and possibly route them to the same destination.

This is the same behaviour implemented in [Apache ActiveMQ](https://activemq.apache.org/message-groups).

For this reason we are mapping this property to the Message Key in Pulsar, this way JMSXGroupID will be used as routing key.

## Interoperability with other Pulsar Clients

The Pulsar JMS Client do not deal with Schema, and it treats every message as a raw array of bytes, interpreting the content of the message according to the JMS API
that is used and to a special JMSPulsarMessageType property.

JMS specs require that on the consumer side you receive a message of the same type that has been sent by the producer: TextMessage,BytesMessage,StreamMessage,MapMessage,ObjectMessage.

When the JMS consumer receives a message that has not been produced by the JMS client itself and lacks the JMSPulsarMessageType property it converts it to a BytesMessage
in order to allow the access of the Message.

The Key of the Pulsar message is always mapped to the JMSXGroupID message property.

## Configuration reference

This is the complete reference for configuring the Pulsar JMS client.
The configuration is passed to the PulsarConnectionFactory constructor.

```
    Map<String, Object> configuration = new HashMap<>();
    configuration.put("...","...");
    ConnectionFactory factory = new PulsarConnectionFactory(configuration);
    ....
    factory.close();
```

You can also pass it as a JSON encoded string
```
    String configuration = "{.....}";
    ConnectionFactory factory = new PulsarConnectionFactory();
    factory.setJsonConfiguration(configuration);
    ....
    factory.close();
```

Once you start using the ConnectionFactory, you are no more allowed to change the configuration.

Configuration reference:

| Configuration Entry  | Required | Type | Default value | Meaning | Notes |
| ------------- | ------------- | -------------| ------------- | ------------- | ------------- |
| webServiceUrl | yes | String | http://localhost:8080 | Main Pulsar HTTP endpoint | Configure this in order to access to your cluster |
| brokerServiceUrl | no | String | same value as webServiceUrl | The URL to connect to the Pulsar broker or proxy | |
| enableTransaction | no | boolean | false | Enable transactions | It defaults to false because Transaction support is not enabled by default in Pulsar 2.7 and the client won't be able to connect to a cluster that does not enable transactions |
| jms.enableClientSideEmulation | no | boolean | false | Enable emulated features | Enable features that are not supported directly by the Pulsar Broker but they are emulated on the client side. |
| jms.acknowledgeRejectedMessages | no | boolean | false | Force acknowledgment of filtered messages on Shared subscriptions | By default filtered out messages are negatively acknowledged on Shared subscriptions, with this flag they are acknowledged in order to skip them definitively |
| jms.clientId | no | String | empty string | Administratively assigned clientId (see JMS specs) | It is the default value assigned to every Connection. |
| producerConfig | no | Map<String,Object> | Empty Map | Additional configuration to be set on every Pulsar Producer | |
| consumerConfig | no | Map<String,Object> | Empty Map | Additional configuration to be set on every Pulsar Consumer | |
| jms.systemNamespace | no | String | public/default | Default Pulsar namespace in which create TemporaryDestinations and destinations without an explicit namespace| |
| jms.queueSubscriptionName | no | String | jms-queue | Name of the system subscription used to emulate JMS Queues | |
| jms.useExclusiveSubscriptionsForSimpleConsumers | no | boolean | true | Use Exclusive subscription for Topic consumers | Set this to 'false' to make Delayed Messages work properly |
| jms.forceDeleteTemporaryDestinations | no | boolean | false | Force the deletion of Temporary Destinations | Use Pulsar API to force the deletion even in case of active subscriptions (as 'jms-queue' for instance) |
| jms.waitForServerStartupTimeout | no | number | 60000 | Grace period to wait for the Pulsar broker to be available in milliseconds| Currently used to wait for Queue subscriptions to be ready |

Every other option is passed as configuration to the Pulsar Client and to the Pulsar Admin client, this way
you can configure additional Pulsar features, like [security](https://pulsar.apache.org/docs/en/security-tls-keystore/#configuring-clients).

Please check Apache Pulsar documentation for the complete list of configuration options.

## Implementation details

Please refer to [this page](INTERNALS.md) in order to read about all of the implememtation details
and how this JMS client emulates some features that are not directly provided by Pulsar.