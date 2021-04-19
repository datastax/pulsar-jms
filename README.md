# DataStax Apache Pulsar JMS Client

This is a Java library that implements the JMS 2.0 (Java Messaging Service ®) over the Apache Pulsar® Java Client..

This library is Open Source Software, Apache 2 licenced.

Please refer to [official JMS documentation](https://jakarta.ee/specifications/messaging/2.0/) in order to learn about JMS.

You can find [here](https://pulsar.apache.org) the official Aaache Pulsar documentation.

## Installation

In order to use this library just add this dependency:

```
<dependency>
   <artifactId>pulsar-jms</artifactId>
   <groupId>com.datastax.oss.pulsar.jms</groupId>
   <version>1.0.0</version>
</dependency>
```

## Getting started

In JMS you need these three concepts in order to get started:
- a ConnectionFactory: use com.datastax.oss.pulsar.jms.PulsarConnectionFactory
- a Queue: use com.datastax.oss.pulsar.jms.PulsarQueue (or better Session#createQueue)
- a Topic: use com.datastax.oss.pulsar.jms.PulsarTopic (or better Session#createTopic)

```
   Map<String, Object> configuration = new HashMap<>();
   configuration.put("webServiceUrl", "http://localhost:8080"); 
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

## Building from the sources

If you want to develop and test this library you need to build the jar from sources.

This is a standard Maven project, so use the default commands:

       mvn clean install

## Running the TCK

You can download the TCK [here](https://jakarta.ee/specifications/messaging/2.0/).
This repository contains a copy of the TCK in order to automate the execution of the tests.

In the tck-executor module you find:
- the Java Code needed to initialize the TCK (JNDIInitialContextFactory)
- the configuration file for the TCK runner: ts.jte
- a file that contains the excluded tests that cannot pass with this client: ts.jtx (see below for the list of unsupported features)
- a couple of scripts to run Aaache Pulsar 2.7.1, configure the Transaction Coordinator and prepare for the execution of the TCK

With this command you build the package, run unit tests and then you run the TCK

       mvn clean install -Prun-tck

In order to only run the TCK

       mvn clean install -Prun-tck -pl tck-executor

## Mapping Apache Pulsar Concept to JMS Specification

JMS Specifications are built over the concepts of **Topics** and **Queues**, but in Pulsar we only have a general concept of **Topic**
that can model both of the two modes.

In JMS a **Topic** is written by many **Producers** and read by many **Consumers**, that share one or many **Subscriptions**.
Subscriptions may be **Durable** or **Non-Durable**, **Shared** or **Non-Shared**. So the same message may be received and processed by more than one Consumer. 

This is the mapping between JMS Consumer/Subscriptions and Apache Pulsar Consumers for Topics:

| JMS Concept  | Pulsar Concept |
| ------------- | ------------- |
| Topic | Persistent topic |
| Consumer | Exclusive Non-Durable subscription with random UUID |
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

You can change the name of the shared subscription using `jms.queueName` configuration parameter but you must ensure that you change 
this configuration on every client.

In order to implement QueueBrowser we use the Pulsar Reader API, starting from the next message available on the 'jms-queue' subscription.
In order to peek the next message we use the Pulsar Admin API `peekMessages`.
Sometimes it happens that the 'peekMessages' API still returns the last consumed message of the subscription, so the QueueBrowser may return non-accurate
results.

## Message Listeners and Concurrency
TODO

## Interoperability with other Pulsar Clients
TODO

## Unsupported features

The JMS 2.0 specifications describe broadly a generic Messaging service and define many interfaces and services.
Apache Pulsar does not support every of the required features, and this library is a wrapper over the Apache Pulsar Client.

This library, when you run it using Apache Pulsar 2.7.x passes most of the TCK, except from the few tests around this list of features.

| Feature  | Supported by Pulsar | Emulated by client |
| ------------- | ------------- | ------------- |
| Message selectors | Not supported by Pulsar | Emulated on Client |
| NoLocal subscriptions | Not supported by Pulsar | Emulated on Client |
| Per message Time To Live | Pulsar supports TTL at topic level, not per-message | Emulated on Client |
| Global registry of clientId  | Not supported by Pulsar | Partially emulated by JSM Client |
| Global unique subscription names  | In Pulsar the subcription name is unique per topic | Partially emulated by JSM Client |
| Temporary destinations (auto deleted when Connection is closed) | Not supported by Pulsar | Emulated on Client, require admin privileges |
| Creation of subscriptions from client | Supported by it requires relevant privileges granted to the client | |
| Delayed messages | It does not work for Exclusive subscriptions | There is an option to use Shared subscriptions even in cases there an Exclusive subscription would be preferred |
| Message Priority | Unsupported | Priority is stored as property and delivered to the consumer, but ignored|
| Non-Durable Messages | Unsupported, every Message is persisted | DeliveryMode is stored as property and delivered to the consumer, but ignored|
| Transactions | Supported as BETA in Pulsar 2.7.x | |
| StreamMessage | Unsupported in Pulsar | The message contains the whole stream | 
| Topic vs Queue | Unsupported in Pulsar | Every destination is a Pulsar Topic, the behaviour of the client depends on which API you use |

## Message selectors and noLocal subscriptions.

Message selectors are about the ability to not receive messages that do not verify a given condition.
Pulsar does not support natively this feature but it can be partially implemented by executing the filter on the client side. 

A NoLocal subscription is a special mode in which a consumer requires to not receive the messages sent by the same Connector that created the Consumer itself.
That is to not receive the messages originated locally.
 
Both of these features can be emulated on the client side with these limitations:
- for Exclusive subscriptions you can discard the message on the client (and automatically acknowledge id)
- for Shared subscriptions (espcially on Queues) the message is discarded on the client and it is "negative acknowledged" in order to give a change to other consumers to receive the message
- for QueueBrowsers you simply discard the message on the client side

Currently, the implementation of Message Selectors is based on Apache ActiveMQ® Java client classes, imported as dependency, Apache ActiveMQ is Apache 2 licensed.

## Global registry of subscription names

The JMS specifications require a subscription name, together with the clientId, to be globally unique in the system, but in Pulsar the subscription name is defined
in the scope of the Topic, so you can use the same subscription name on two different topics, referring to district entities.

These are the most notable side effects while using Pulsar with the JMS API:
- `Session.unsubscribe(String subscriptionName)` cannot be used, because it refers to the subscriptionName without a topic name
- We cannot implement the requirements of the specs when for instance you change a message selector, and you should _unsubscribe_ the old subscriptionName and create a new subscription

In Pulsar, we cannot attach labels or metadata to subscriptions and we cannot enforce globally that a subscription is accessed everywhere using the same "messaage selector" and "noLocal" options.

## Delayed Messages

In Pulsar 2.7.x delayed messages are delivered immediately (without respecting the delay) in case of Exclusive subscriptions.
In order to mitigate this problem this client allows you to use Shared subscriptions even where an Exclusive subscription would be a better fit.
But anyway you are not going to use a Shared subscription when an Exclusive subscription is required (Durable Non-Shared Consumers).
The most notable case is Session.createConsumer(destination), that creates an unnamed subscription, and an Exclusive subscription would be a better fit.

If you set `jms.useExclusiveSubscriptionsForSimpleConsumers=false` the client will use a Shared subscription, and the delay will be respected.

## Temporary destinations

Temporary destinations are created using `Session.createTemporaryQueue` and `Session.createTemporaryTopic`. They should create a destination that is automatically
deleted then the Connection is closed.
This behaviour should be implemented on the server side, but in Pulsar there is no concept of JMS Connection and so this cannot be implemented.

We are emulating this behaviour on the client, by trying to delete the destination on `Connection.close()` and in `ConnectionFactory.close()` but
there is no guarantee that this will eventually happen, for instance in case of crash of the client application or in case of temporary error during the deletion of the destination.

It must also be noted that creating a temporary destination requires the client to be allowed to create the destination and also to configure the broker to allow
automatic topic creation (`allowAutoTopicCreation=true`).

## Creation of subscriptions

In order to allow the JMS client to create subscriptions the client must be allowed to do it and also the broker must be configured to create automatically the subscriptions,
please check `allowAutoSubscriptionCreation=true` parameter on your broker configuration.



## Configuration reference

TODO