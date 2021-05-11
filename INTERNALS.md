# Implementation details

The JMS specification require to support a few features that are not natively supported by Apache Pulsar.

This document describes in detail the design and implementation decisions made to support most 
of the JMS requirements.

Please refer to the main [README](README.md) page for general information about this project. 

## Message Listeners and Concurrency

The JMS specifications require a specific behaviour for MessageListener in respect to concurrency, for this reason the JMS client starts a dedicated
thread per each Session in order process every MessageListener.

There are also specific behaviours mandates but the specs regarding these APIs:
- Connection/JMSContext.start() and Connection/JMSContext.stop()
- Session.close()/JMSContext.close()
- Connection/JMSContext.close()

Pulsar JMS client implements its own concurrent processing model in order to obey the specs, and it cannot use the builtin facilities provider by the Pulsar client.

For CompletionListeners, that are useful for asynchronous sending of messages, the JMS client relies on Pulsar async API, but there are some behaviour that are still to be enforced
in respect to Session/JMSContext.close().

## Message properties
In Pulsar properties are always of type String, but JMS specs require to support every Java primitive type.

In order to emulate this behaviour for every custom property set on the message we set an additional property that describes the original type of the property.

For instance if you set a message property foo=1234 (integer) we add a property foo_jmstype=integer in order to reconstruct properly 
the value when the receiver calls `getObjectProperty`.

The value is always serialized as string, and for floating point numbers we are using Double.toString/parseString and Float.toString/parseString, with the behaviour mandated
by Java specifications.

### System properties and fields:

These properties are used by the JMS Client is a special way:
- All property whose name ends with `_jsmtype`: they are additional properties that hold the original data type
- `JMSType`: value for the standard field JMSType
- `JMSCorrelationID`: base64 representation of the standard JMSCorrelationID field
- `JMSPulsarMessageType`: type of message
- `JMSMessageId`: logical id of the message
- `JMSReplyTo`: fully qualified name of the topic referred by the JMSReplyTo field
- `JMSReplyToType`: JMS type for the JMSReplyTo topic. Allowed values are "topic" or "queue" (default is "topic")
- `JMSDeliveryMode`: integer value of the JMSDeliveryMode standard field, if is set in case of a value different from DeliveryMode.PERSISTENT
- `JMSPriority`: integer value of the priority requested for the message, it is set in case of a value different from Message.DEFAULT_PRIORITY
- `JMSDeliveryTime`: representation in milliseconds since the UNIX epoch of the "JMSDeliveryTime" field
- `JMSXGroupID`: this property is mapped to the "key" of the Pulsar Message, and it is not represented by a Message property
- `JMSXGroupSeq`: this is mapped to Pulsar Message "sequenceId" in case it is not overridden with a custom value
- `JMSConnectionID`: id of the Connection 

Mapping of special fields of the Message:
- property `JMSXDeliveryCount`: this is mapped to 1 + Pulsar Message RedeliveryCount field
- field `JMSExpiration`: this is the representation in milliseconds since the UNIX epoch of the expiration date of the message, used to emulate "time to live"
- field `JMSRedelivered`: this is mapped to "true" in case of JMSXDeliveryCount > 1

The properties `JMSXUserID`,`JMSXAppID`,`JMSXProducerTXID`,`JMSXConsumerTXID`,`JMSXRcvTimestamp`,`JMSXState` are ignored.

Please refer to section "3.5.9. JMS defined properties" of the JMS 2.0 specifications.

## Client identifiers

Each Connection may have a client identifier, that can be set programmatically or configured administratively using the 
`jms.clientId` configuration parameters.
Client identifiers must be globally unique but there is no way to enforce this constraint on a Pulsar cluster.

When you set a clientId the actual subscription name in Pulsar is composed by `clientId + '_' + subscriptionName`. 

## Unsupported and Emulated features

The JMS 2.0 specifications describe broadly a generic Messaging service and define many interfaces and services.
Apache Pulsar does not support every of the required features, and this library is a wrapper over the Apache Pulsar Client.

Most of the features that are not natively supported by Pulsar are emulated by the JMS Client,
this may help in porting existing JMS based applications application to Pulsar.
If you want to use emulated features, but the emulated behaviour does not fit your needs please
open an issue in order to request an improvement, that will have to be implemented on Pulsar core.

| Feature  | Supported by Pulsar | Emulated by client |
| ------------- | ------------- | ------------- |
| Message selectors | Not supported by Pulsar | Emulated on Client |
| NoLocal subscriptions | Not supported by Pulsar | Emulated by JMS Client |
| Per message Time To Live | Pulsar supports TTL at topic level, not per-message | Emulated by JMS Client |
| Global registry of clientId  | Not supported by Pulsar | Partially emulated by JMS Client |
| Global unique subscription names  | In Pulsar the subscription name is unique per topic | Partially emulated by JMS Client |
| Temporary destinations (auto deleted when Connection is closed) | Not supported by Pulsar | Partially emulated by JMS Client |
| Creation of subscriptions from client | Supported, but it requires relevant privileges granted to the client | |
| Delayed messages | It does not work for Exclusive subscriptions | There is an option to use Shared subscriptions even in cases there an Exclusive subscription would be preferred |
| Message Priority | Unsupported | Priority is stored as property and delivered to the consumer, but ignored|
| Non-Persistent Messages | Unsupported, every Message is persisted | DeliveryMode.NON_PERSISTENT is stored as property and delivered to the consumer, but ignored|
| Transactions | Supported as BETA in Pulsar 2.7.x | Transactions must be enabled on client and on server |
| StreamMessage | Unsupported in Pulsar | Emulated by storing the whole stream in one message | 
| Topic vs Queue | Unsupported in Pulsar | Every destination is a Pulsar Topic, the behaviour of the client depends on which API you use |
| Username/password authentication | Unsupported in Pulsar | Unsupported in the JMS client, but you can configure Pulsar client security features |
| JMSXDeliveryCount/JMSRedelivered | Unsupported JMS behaviour in Pulsar | The behaviour of the delivery counter in Pulsar follows different semantics in respect to JMS |

This library, when you run it using Apache Pulsar 2.7.x passes most of the TCK, except from the few tests around the need
of supporting globally unique subscription names.

## Message selectors and noLocal subscriptions

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

See [PIP-26](https://github.com/apache/pulsar/wiki/PIP-26:-Delayed-Message-Delivery) for reference.

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