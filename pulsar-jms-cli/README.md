# Starlight for JMS - CLI Tools

This module contains a sample JMS application that 

## Prerequisites

In order to launch this tool you need:
- A Java runtime, JDK8 and JDK11 are preferred
- The jms-cli.jar

## Running the command

To run the tool use this command line. It will show all the available actions.

```
java -jar jms-cli.jar
```

## Configuration

The configuration is passed using a JSON file using the '-c' parameter.

```
java -jar jms-cli.jar -c conf/sample.json
```

You can take a look to [conf/sample.json](conf/sample.json) for some examples.
Please refer to the main [../README.md](README file) for the reference documentation.

## Sample commands

### Produce messages to a JMS destination

```
java -jar jms-cli.jar produce -d test -n 10 -p foo=bar -p foo2=bar2 -c conf/sample.json
```

With '-p' you can add Message properties, this is very useful while working with Selectors on the Consumer.

There is no need to specify if you are writing to a JMS Topic or JMS Queue while producing.

### Consuming messages

In order to consume the messages you have to choose among three flavours, that match the available
Consumer types in JMS: createConsumer, createDurableConsumer, createSharedDurableConsumer.

```
java -jar jms-cli.jar createConsumer -d test -n 0 -dt queue -c conf/sample.json -s "foo='bar'"
```

With '-s' you can set a Selector, to filter the messages.

Some commands, like createDurableConsumer and createSharedDurableConsumer require you to use a JMS Topic
and not a Queue.
In Starlight for JMS you connect to Pulsar topics and you interpret them as Queues or Topics only on the client, so you have to manually tell if you want to connect as a JMS Queue (-t queue) or as a JMS Topic (-t topic).

If you are connecting in JMS Queue mode, then you can specify the subscription name using the special syntax queue:subscriptionName.
In this case the subscription in Pulsar will be named "queue:subscriptionName".
This is very useful to map Pulsar Subscription to JMS Queues.

```
java -jar jms-cli.jar createConsumer -d test:subscriptionName -n 0 -dt queue -c conf/sample.json -s "foo='bar'"
```