# DataStax Apache Pulsar JMS Client - Spring Example

This is a sample project that shows how to use Spring Builtin capabilities

This example uses [Spring JMS support](https://docs.spring.io/spring-framework/docs/current/reference/html/integration.html#jms) in order to send and receive a message with Pulsar.

## Running the example

The example is preconfigured to use a Pulsar broker running at http://localhost:8080.

You can start it using Docker

`docker run -p 8080:800 -p 6650:6650 apachepulsar/pulsar:latest bin/pulsar standalone`

or by starting [Pulsar Standalone](https://pulsar.apache.org/docs/en/standalone/)

Bundle the repository using Maven

`mvn clean package -DskipTests`

then run the Main class using Maven
`mvn exec:java@run`

## Configuration

In the appContext.xml file you can see the definition of a PulsarConnectionFactory that is the implementation of javax.jms.ConnectionFactory provided by DataStax JMS Client.

```
  <util:map id="pulsarConfiguration" map-class="java.util.HashMap">
     <entry key="brokerServiceUrl" value="http://localhost:8080"/>
     <entry key="webServiceUrl" value="http://localhost:8080"/>
     <entry key="jms.enableClientSideFeatures" value="true"/>
     <entry key="enableTransaction" value="false"/>
  </util:map>

  <bean id="connectionFactory" class="com.datastax.oss.pulsar.jms.PulsarConnectionFactory">
    <constructor-arg ref="pulsarConfiguration" />
  </bean>

```

### Sending a message

The application uses JmsTemplate to send a message, it is a TextMessage that contains the serialized version of a Java bean

```
    JmsTemplate jmsTemplate = context.getBean(JmsTemplate.class);
    jmsTemplate.convertAndSend("IN_QUEUE", new Email("info@example.com", "Hello"));
```

### Receiving messages

We receive the JMS message using a simple `SessionAwareMessageListener`, declare in the XML configuration file

```
public class ExampleListener implements SessionAwareMessageListener {
   @Override
   public void onMessage(Message message, Session session) throws JMSException { 
        System.out.println(((TextMessage) message).getText());
   }
}
```

The listener is activated by a `DefaultMessageListenerContainer`, and configured to read from `IN_QUEUE` queue

```
  <bean id="messageListener" class="examples.ExampleListener"/>

  <bean id="jmsContainer" class="org.springframework.jms.listener.DefaultMessageListenerContainer">
    <property name="connectionFactory" ref="connectionFactory"/>
    <property name="destinationName" value="IN_QUEUE"/>
    <property name="messageListener" ref="messageListener"/>
  </bean>
```
