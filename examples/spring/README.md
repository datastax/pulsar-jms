# DataStax Apache Pulsar JMS Client - Spring Example

This is a sample project that shows how to use Spring Builtin capabilities

This example uses [Spring JMS support](https://spring.io/guides/gs/messaging-jms/) in order to send and receive a message with Pulsar.

## Running the example

The example is preconfigured to use a Pulsar broker running at http://localhost:8080.

You can start it using Docker

`docker run -p 8080:800 -p 6650:6650 apachepulsar/pulsar:latest bin/pulsar standalone`

or by starting [Pulsar Standalone](https://pulsar.apache.org/docs/en/standalone/)

Bundle the repository using Maven

`mvn clean package -DskipTests`

then run the Main class using Maven
`mvn spring-boot:run`

### Sending a message

The application uses JmsTemplate to send a message, it is a TextMessage that contains the serialized version of a Java bean

```
    JmsTemplate jmsTemplate = context.getBean(JmsTemplate.class);
    jmsTemplate.convertAndSend("IN_QUEUE", new Email("info@example.com", "Hello"));
```

### Receiving messages

We receive the JMS message using a simple `@JmsListener`

```
@Component
public class ExampleListener {

  @JmsListener(destination = "IN_QUEUE", containerFactory = "myFactory")
  public void onMessage(Email email) {
    System.out.println("Received " + email);
  }
}
```