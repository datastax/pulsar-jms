# DataStax Fast JMS for Apache Pulsar

This is a Java library that implements the JMS 2.0 (Java Messaging Service ®) over the Apache Pulsar® Java Client.

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
   <groupId>com.datastax.oss</groupId>
   <version>VERSION</version>
</dependency>
```

That file contains only Fast JMS for Pulsar code and transitively imports the Apache Pulsar client and the Jakarta JMS 2.0 specifications JAR.

You can also use a "fat" JAR that includes all dependencies:

```
<dependency>
   <artifactId>pulsar-jms-all</artifactId>
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
            ...
       }
   }`
```

Ensure you have a Pulsar service running at http://localhost:8080 before trying out the example.

You can for instance run Pulsar Standalone using docker

```
docker run --name pulsar-jms-runner -d -p 8080:8080 -p 6650:6650 apachepulsar/pulsar:2.7.1 /pulsar/bin/pulsar standalone
```

## JakartaEE® Resource Adapter

In order to use this JMS Client inside a JakartaEE® or JavaEE® application you can use the `ResourceAdapter`.

The source code for the resource adapter is in this [directory](resource-adapter).

## Examples

We have two example apps, one for Pulsar standalone, and one for Astra Streaming:

- [Pulsar standalone](https://docs.datastax.com/en/fast-pulsar-jms/docs/1.1/pulsar-jms-quickstart-sa.html)
- [Astra Streaming](https://docs.datastax.com/en/fast-pulsar-jms/docs/1.1/pulsar-jms-quickstart-astra.html)

In addition, we’ve provided the following integration examples:

- with [Spring Boot®](examples/spring)
- with [Payara Micro®](examples/payara-micro)
- with [Apache TomEE®](resource-adapter-tests)

## Building from source

If you want to develop and test this library you need to build the jar from sources.

This is a standard Maven project, so use the default commands:

       mvn clean install

## Running the TCK

You can download the TCK [here](https://jakarta.ee/specifications/messaging/2.0/). The repository contains a copy of the TCK that automates the execution of the tests.

In the tck-executor module you'll find:

- The Java Code needed to initialize the TCK, `JNDIInitialContextFactory.java`.
- The configuration file for the TCK runner, `ts.jte`.
- A file that contains the excluded tests that cannot pass with this client, `ts.jtx`
- Scripts to run Apache Pulsar 2.7.1, configure the Transaction Coordinator, and prepare for the execution of the TCK.

To build the package, run unit tests, and run the TCK:

```
mvn clean install -Prun-tck
```

To run only the TCK:

```
mvn clean install -Prun-tck -am -DskipTests -pl tck-executor
```

> :warning: Globally unique subscription names are not supported so the corresponding tests are skipped.

## Configuration reference

For a complete list of configuration options, see: https://docs.datastax.com/en/fast-pulsar-jms/docs/1.1/pulsar-jms-reference.html

## Implementation details

For general background as well as implementation details, see:

- [Mapping Pulsar concepts to JMS specifications](https://docs.datastax.com/en/fast-pulsar-jms/docs/1.1/pulsar-jms-mappings.html)
- [DataStax Fast JMS for Apache Pulsar implementation details](https://docs.datastax.com/en/fast-pulsar-jms/docs/1.1/pulsar-jms-implementation.html)
