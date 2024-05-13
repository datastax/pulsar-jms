# Starlight for JMS

Starlight for JMS implements the JMS 2.0 (Java Messaging Service ®) API over the Apache Pulsar® Java Client.

This library is Open Source Software, Apache 2 licensed.

Please refer to the [official JMS documentation](https://jakarta.ee/specifications/messaging/2.0/) in order to learn about JMS.
This [website](https://javaee.github.io/jms-spec/) is useful as well as it contains the former JMS 2.0 specifications before the Jakarta transitions.

You can find the official Apache Pulsar documentation [here](https://pulsar.apache.org).

The documentation for this project is located [here](https://docs.datastax.com/en/fast-pulsar-jms/docs/1.1/).

## Installation

In order to use this library just add this dependency to your Maven pom:

```
<dependency>
   <artifactId>pulsar-jms</artifactId>
   <groupId>io.streamnative.oss</groupId>
   <version>VERSION</version>
</dependency>
```

That file contains only Starlight for JMS code, and transitively imports the Apache Pulsar client and the Jakarta JMS 2.0 specifications JAR.

You can also use a "fat" JAR that includes all dependencies:

```
<dependency>
   <artifactId>pulsar-jms-all</artifactId>
   <groupId>io.streamnative.oss</groupId>
   <version>VERSION</version>
</dependency>
```

## Which version ?

Starlight for JMS works well with every Pulsar version in case you use basic features.
But there are some specific features that require you to use a given JMS Client version dependending on your Pulsar server.

If you use Transactions and you are running Pulsar 2.8.x you must use Starlight for JMS with a version starting with 1, like 1.5.0;
If you run Pulsar 2.10 or above you have to use Starlight for JMS with version 2.0.0 or above.

If you want to use Server Side Filtering then you must use Pulsar 2.10.1 or above and Starlight for JMS 2.0.0 or above.

## Getting started

In JMS you need these three concepts to get started:
- a ConnectionFactory: use io.streamnative.oss.pulsar.jms.PulsarConnectionFactory
- a Queue: use io.streamnative.oss.pulsar.jms.PulsarQueue (or better Session#createQueue)
- a Topic: use io.streamnative.oss.pulsar.jms.PulsarTopic (or better Session#createTopic)

This is how you access them with Starlight for JMS:

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
            ...
       }
   }`
```

Ensure you have a Pulsar service running at http://localhost:8080 before trying out the example.

You can for instance run Pulsar Standalone using docker like this:

```
docker run --name pulsar-jms-runner -d -p 8080:8080 -p 6650:6650 apachepulsar/pulsar:2.7.1 /pulsar/bin/pulsar standalone
```

## JakartaEE® Resource Adapter

In order to use this JMS Client inside a JakartaEE® or JavaEE® application you can use the `ResourceAdapter`.

The source code for the resource adapter is in this [directory](resource-adapter).

## Using JNDI to Connect to Pulsar

You can use the JNDI API to build the ConnectionFactory and the Destination references.

Steps:
* Use `io.streamnative.oss.pulsar.jms.jndi.PulsarInitialContextFactory` as `Context.INITIAL_CONTEXT_FACTORY`
* Pass the configuration (authentication, broker address...) using the `Properties` object
* Lookup the ConnectionFactory using the system name `ConnectionFactory`
* Lookup destinations using `queues/` or `topics/` prefix

```
Properties properties = new Properties();
properties.setProperty(Context.INITIAL_CONTEXT_FACTORY, "io.streamnative.oss.pulsar.jms.jndi.PulsarInitialContextFactory");
properties.setProperty(Context.PROVIDER_URL, "pulsar://localhost:6650");
properties.setProperty("webServiceUrl", "http://localhost:8080");
// automatically close the ConnectionFactory when closing the JDNI context
properties.setProperty("autoCloseConnectionFactory", "true");
properties.setProperty("jms.systemNamespace", "public/default");

// add here the rest of your configuration
// properties.setProperty("jms.clientId", "my-id");

javax.naming.Context jndiContext = new InitialContext(properties);

// get access to the ConnectionFactory
ConnectionFactory factory = (ConnectionFactory) jndiContext.lookup("ConnectionFactory");
Queue queue = (Queue) jndiContext.lookup("queues/MyQueue");
Topic topic = (Topic) jndiContext.lookup("topics/MyQueue");

// use fully qualified Pulsar topic name
Queue queue = (Queue) jndiContext.lookup("queues/persistent://tenant/namespace/MyQueue");

// disposing the InitialContext closes the ConnectionFactory
jndiContext.close();
```


## Examples

We have two example apps, one for Pulsar standalone, and one for Astra Streaming:

- [Pulsar standalone](https://docs.datastax.com/en/fast-pulsar-jms/docs/1.1/pulsar-jms-quickstart-sa.html)
- [Astra Streaming](https://docs.datastax.com/en/fast-pulsar-jms/docs/1.1/pulsar-jms-quickstart-astra.html)

In addition, we’ve provided the following integration examples:

- with [Spring Boot®](examples/spring)
- with [Payara Micro®](examples/payara-micro)
- with [Apache TomEE®](resource-adapter-tests)

## JMS TCK (Technology Compatibility Kit) Verification

You can download the TCK [here](https://jakarta.ee/specifications/messaging/2.0/). The repository contains a copy of the TCK that automates the execution of the tests.

In the tck-executor module you'll find:

- The Java Code needed to initialize the TCK, `JNDIInitialContextFactory.java`.
- The configuration file for the TCK runner, `ts.jte`.
- A file that contains the excluded tests that cannot pass with this client, `ts.jtx`
- Scripts to run Apache Pulsar, configure the Transaction Coordinator, and prepare for the execution of the TCK.

Please **NOTE**: 
1. Regarding the TCK configuration file, `ts.jte`, you don't need to configure it manually. The maven process (as described below) will configure it automatically.
2. Regarding the test exclusion file, 'ts.jtx', you don't need to make any changes. This file contains a minimum list of TCK tests that are not applicable when using Pulsar as the JMS provider. For example, globally unique subscription names are not supported so the corresponding tests are skipped.

### Build Prerequisite

In order to run th TCK tests, please make sure the following prerequisites are met:

* JDK 8 (Note that this is only needed to run TCK test. Starlight for JMS itself can run on newer java versions)
* docker
* ant
* maven

Also, when running the TCK tests on one machine, please make sure that there is **NO** existing Pulsar server running on the same machine. The execution of the TCK tests will launch a standalone Pulsar server running in a docker container. It will run into port conflicts if a Pulsar server is already running. 

### Build and run TCK

To build the package, run unit tests, and run the TCK:

```
mvn clean install -Prun-tck
```

To run only the TCK:

```
mvn clean install -Prun-tck -am -DskipTests -pl tck-executor
```

## Building from source

If you want to develop and test this library you need to build the jar from sources.

This is a standard Maven project, so use the default commands:

       mvn clean install

## Configuration reference

For a complete list of configuration options, please see [Starlight for JMS Configuration Reference](https://docs.datastax.com/en/fast-pulsar-jms/docs/1.1/pulsar-jms-reference.html#_configuration_options)

## Implementation details

For general background as well as implementation details, see:

- [Mapping Pulsar concepts to JMS specifications](https://docs.datastax.com/en/fast-pulsar-jms/docs/1.1/pulsar-jms-mappings.html)
- [Starlight for JMS implementation details](https://docs.datastax.com/en/fast-pulsar-jms/docs/1.1/pulsar-jms-implementation.html)
