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

## Pulsar Message Key and JMSXGroupID

The special JMSXGroupID property is defined in the JMS specs as a way to group the messages and possibly route them to the same destination.
This is the same behaviour implemented in [Apache ActiveMQ](https://activemq.apache.org/message-groups).
For this reason we are mapping this property to the Message Key in Pulsar, this way JMSXGroupID will be used as routing key.

## Message Listeners and Concurrency

The JMS specifications require a specific behaviour for MessageListener in respect to concurrency, for this reason the JMS client starts a dedicated
thread per each Session in order process every MessageListener.

There are also specific behaviours mandates but the specs regarding these APIs:
- Connection/JMSContext.start() and Connection/JMSContext.stop()
- Session.close()/JMSContext.close()
- Connection/JMSContext.close()

Pulsar JMS client implements its own concurrent processing model in order to obey the specs, and it cannot use the builtin facilities provider by the Pulsar client.

For CompletionListeners, that are useful for asynchronous sending of messages the JMS relies on Pulsar async API, but there are some behaviour that are still to be enforced
in respect to Session/JMSContext.close().

## Message properties
In Pulsar properties are always of type String, but JMS specs require to support every Java primitive type.
In order to emulate this behaviour for every custom property set on the message we set an additional property that describes the original type of the property.

For instance if you set a message property foo=1234 (integer) we add a property foo_type=integer on the message in order to reconstruct properly 
the value when the receiver call "getObjectProperty".
The value is always serialized as string, and for floating point numbers we are using Double.toString/parseString and Float.toString/parseString, with the behaviour mandated
by Java specifications.

## Interoperability with other Pulsar Clients

The Pulsar JMS Client do not deal with Schema, and it treats every message as a raw array of bytes, interpreting the content of the message according to the JMS API
that is used and to a special JMSPulsarMessageType property.
JMS specs require that on the consumer side you receive a message of the same type that has been sent by the producer: TextMessage,BytesMessage,StreamMessage,MapMessage.

When the JMS consumer receives a message that has not been produced bu the JMS client itself and lacks the JMSPulsarMessageType property it converts it to a BytesMessage
in order to allow the access of the Message.

The Key of the Pulsar message is always mapped to the JMSXGroupID message property.

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
| Username/password authentication | Unsupported in Pulsar | Unsupported in the JMS client |

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

## Global registry of subscription names and of clientIds

The JMS specifications require a subscription name, together with the clientId, to be globally unique in the system, but in Pulsar the subscription name is defined
in the scope of the Topic, so you can use the same subscription name on two different topics, referring to district entities.

These are the most notable side effects while using Pulsar with the JMS API:
- `Session.unsubscribe(String subscriptionName)` cannot be used, because it refers to the subscriptionName without a topic name
- We cannot implement the requirements of the specs when for instance you change a message selector, and you should _unsubscribe_ the old subscriptionName and create a new subscription

In Pulsar, we cannot attach labels or metadata to subscriptions, and we cannot enforce globally that a subscription is accessed everywhere using the same "messaage selector" and "noLocal" options.
Pulsar does not have the concept of clientId, so it is not possible to prevent the existence of multiple Connections with the same clientId in the cluster, such check is performed only locally in the context of the JVM/Classloader execution (using a _static_ registry)

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


This is the complete reference for configuring the Pulsar JMS client.
The configuration is passed to the PulsarConnectionFactory constructor.

```
    Map<String, Object> configuration = new HashMap<>();
    configuration.put("...","...");
    ConnectionFactory factory = new PulsarConnectionFactory(configuration);
    ....
    factory.close();
```

| Configuration Entry  | Required | Type | Default value | Meaning | Notes |
| ------------- | ------------- | -------------| ------------- | ------------- | ------------- |
| webServiceUrl | yes | String | http://localhost:8080 | Main Pulsar HTTP endpoint | Configure this in order to access to your cluster |
| jms.clientId | no | String | empty string | Administratively assigned clientId (see JMS specs) | It is the default value assigned to every Connection |
| producerConfig | no | Map<String, Object> | Empty Map | Additional configuration to be set on every Pulsar Producer | |
| consumerConfig | no | Map<String, Object> | Empty Map | Additional configuration to be set on every Pulsar Consumer | |
| jms.systemNamespace | no | String | public/default | Pulsar namespace in which create TemporaryDestinations | |
| jms.queueName | no | String | jms-queue | Name of the system subscription used to emulate JMS Queues | |
| jms.useExclusiveSubscriptionsForSimpleConsumers | no | boolean | true | Use Exclusive subscription for Topic consumers | Set this to 'false' to make Delayed Messages work properly |
| jms.forceDeleteTemporaryDestinations | no | boolean | false | Force the deletion of Temporary Destinations | Use Pulsar API to force the deletion even in case of active subscriptions (as 'jms-queue' for instance) |
| jms.tckUsername | no | String | empty string | Username for running the TCK | Not used in production |
| jms.tckPassword | no | String | empty string | Password for running the TCK | Not used in production |
| enableTransaction | no | boolean | false | Enable transactions | It defaults to false because Transaction support is not enabled by default in Pulsar 2.7 and the client won't be able to connect to a cluster that does not enable transactions |
