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

### Describe a JMS destination

```
java -jar jms-cli.jar describe -d test-c conf/sample.json
```

With this command you describe the JMS destination, in particular you will see
the subscriptions, the consumers and any Server Side filters that are applied currently.

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

### Create a JMS Subscription with Server Side Filtering

```
java -jar jms-cli.jar create-jms-subscription -d test-queue -dt queue -sub mysub -s 'keepme = TRUE'  conf/sample.json
```

With this command you create a Pulsar subscription on a JMS Destination, and you can set a server 
side selector that is applied per subscription.

If you run the `describe` command you will see the new subscription.