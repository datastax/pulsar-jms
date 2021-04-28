# DataStax Apache Pulsar JMS Client - JakartaEE® ResourceAdapter

A ResourceAdapter is a preconfigured bundle (a .rar file) that you can deploy to your application server in order
to configure access to Managed resources.

In this case the Pulsar Resource Adapter allows you to easily connect to Pulsar from a Web Application or from an Enterprise Java Bean. 

## Contents of the Resource Adapter

This resource adapter defines:
- The main ResourceAdapter implementation (com.datastax.oss.pulsar.jms.rar.PulsarResourceAdapter)
- The implementation for Outbound Connections (com.datastax.oss.pulsar.jms.rar.PulsarManagedConnectionFactory)
- The implementation for Inbound Messages (com.datastax.oss.pulsar.jms.rar.PulsarActivationSpec) 
- The mapping for Queues and for Topics administered objects

Usually you do not have to use directly the names of the Java Classes, because the AppServer is able to discover
them automatically using the [ra.xml](src/main/rar/META-INF/ra.xml) file.

## Destinations

In JMS you define Queues and Topics, in case of this ResourceAdapter you must use:
- com.datastax.oss.pulsar.jms.PulsarQueue for Queues
- com.datastax.oss.pulsar.jms.PulsarTopic for Topics

These to types define the "Name" configuration entry, that represents the fully qualified name of the Pulsar topic, like
persistent://public/default/topic.

If you omit the initial part then the JMS Client applies automatically the `jms.systemNamespace` prefix.

## ResourceAdapter Configuration

The ResourceAdapter defines a "Configuration" property that contains the default configuration to be applied to every MesasgeListener and
ConnectionFactory.
It is encoded in JSON, please refer to [the main readme](../README.md) for reference.

## ConfigurationFactory

The ConfigurationFactory defines only this property:
- Configuration: this is the configuration for the underlying PulsarConnectionFactory, in JSON encoding

If you do not set a configuration, or set an empty value, then the general configuration of the ResourceAdapter is applied

## Message Listener

When you define a MessageListener you define a PulsarActivationSpec, that holds these properties:
- Configuration: this is the configuration for the underlying PulsarConnectionFactory, in JSON encoding
- Destination: the Name of the destination
- DestinationType: the type of destination, it defaults to javax.jms.Queue, use javax.jms.Topic in order to use the Pulsar Topic as a topic

If you do not set a configuration, or set an empty value, then the general configuration of the ResourceAdapter is applied

## Mapping ResourceAdapter managed resources

The ResourceAdapter is responsible for managing a pool of PulsarConnectionFactory, that in turn contains a Pulsar Java Client.
The ResourceAdapter will try to share the Pulsar Client as much as possible, by keeping only one client per each different Configuration.

## Examples

You can find [here](../resource-adapter-tests) and example about how to configure Apache TomEE® with the ResourceAdapter.
Please refer to the documentation of your container for instructions about how to deploy a ResourceAdapter bundle.

