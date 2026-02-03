/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.jms;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.pulsar.jms.utils.PulsarContainerExtension;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import lombok.Data;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class PulsarInteropTest {

  @RegisterExtension
  static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

  @Test
  public void sendFromJMSReceiveFromPulsarClientTest() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (Connection connection = factory.createConnection()) {
        try (Session session = connection.createSession(); ) {
          String topic = "persistent://public/default/test-" + UUID.randomUUID();
          Destination destination = session.createTopic(topic);

          try (PulsarClient client =
              PulsarClient.builder().serviceUrl(pulsarContainer.getBrokerUrl()).build(); ) {

            try (Consumer<String> consumer =
                client
                    .newConsumer(Schema.STRING)
                    .subscriptionName("test")
                    .topic(topic)
                    .subscribe()) {

              try (MessageProducer producer = session.createProducer(destination)) {
                TextMessage textMsg = session.createTextMessage("foo");
                textMsg.setStringProperty("JMSXGroupID", "bar");
                producer.send(textMsg);

                Message<String> receivedMessage = consumer.receive();
                assertEquals("foo", receivedMessage.getValue());
                assertEquals("bar", receivedMessage.getKey());
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void sendFromPulsarClientReceiveWithJMS() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext()) {

        String topic = "persistent://public/default/test-" + UUID.randomUUID();
        Destination destination = context.createTopic(topic);

        try (PulsarClient client =
            PulsarClient.builder().serviceUrl(pulsarContainer.getBrokerUrl()).build(); ) {
          try (JMSConsumer consumer = context.createConsumer(destination)) {

            try (Producer<String> producer =
                client.newProducer(Schema.STRING).topic(topic).create(); ) {
              producer.newMessage().value("foo").key("bar").send();

              // the JMS client reads raw messages always as BytesMessage
              BytesMessage message = (BytesMessage) consumer.receive();
              assertArrayEquals(
                  "foo".getBytes(StandardCharsets.UTF_8), message.getBody(byte[].class));
              assertEquals("bar", message.getStringProperty("JMSXGroupID"));
            }
          }
        }
      }
    }
  }

  @Test
  public void stringSchemaTest() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    Map<String, Object> consumerConfig = new HashMap<>();
    properties.put("consumerConfig", consumerConfig);
    consumerConfig.put("useSchema", true);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext()) {

        String topic = "persistent://public/default/test-" + UUID.randomUUID();
        Destination destination = context.createTopic(topic);

        try (PulsarClient client =
            PulsarClient.builder().serviceUrl(pulsarContainer.getBrokerUrl()).build(); ) {
          try (JMSConsumer consumer = context.createConsumer(destination)) {
            try (Producer<String> producer =
                client.newProducer(Schema.STRING).topic(topic).create(); ) {
              producer.newMessage().value("foo").key("bar").send();

              // the JMS client reads Schema String as TextMessage
              TextMessage message = (TextMessage) consumer.receive();
              assertEquals("foo", message.getText());
              assertEquals("bar", message.getStringProperty("JMSXGroupID"));
            }
          }
        }
      }
    }
  }

  @Test
  public void longSchemaTest() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    Map<String, Object> consumerConfig = new HashMap<>();
    properties.put("consumerConfig", consumerConfig);
    consumerConfig.put("useSchema", true);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext()) {

        String topic = "persistent://public/default/test-" + UUID.randomUUID();
        Destination destination = context.createTopic(topic);

        try (PulsarClient client =
            PulsarClient.builder().serviceUrl(pulsarContainer.getBrokerUrl()).build(); ) {
          try (JMSConsumer consumer = context.createConsumer(destination)) {
            try (Producer<Long> producer =
                client.newProducer(Schema.INT64).topic(topic).create(); ) {
              producer.newMessage().value(23432424L).key("bar").send();

              // the JMS client reads Schema INT64 as ObjectMessage
              ObjectMessage message = (ObjectMessage) consumer.receive();
              assertEquals(23432424L, message.getObject());
              assertEquals("bar", message.getStringProperty("JMSXGroupID"));
            }
          }
        }
      }
    }
  }

  @Data
  static final class Nested {
    int age;
    Pojo pojo;
  }

  @Data
  static final class Pojo {
    String name;
    Nested nested;
    List<Nested> nestedList;
  }

  @Test
  public void avroSchemaTest() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    Map<String, Object> consumerConfig = new HashMap<>();
    properties.put("consumerConfig", consumerConfig);
    consumerConfig.put("useSchema", true);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext()) {

        String topic = "persistent://public/default/test-" + UUID.randomUUID();
        Destination destination = context.createTopic(topic);

        try (PulsarClient client =
            PulsarClient.builder().serviceUrl(pulsarContainer.getBrokerUrl()).build(); ) {
          try (JMSConsumer consumer = context.createConsumer(destination)) {
            try (Producer<Pojo> producer =
                client.newProducer(Schema.AVRO(Pojo.class)).topic(topic).create(); ) {
              Pojo pojo = new Pojo();
              pojo.setName("foo");
              Nested nested = new Nested();
              nested.setAge(23);

              Pojo pojo2 = new Pojo();
              pojo2.setName("foo2");
              nested.setPojo(pojo2);

              pojo.setNested(nested);
              pojo.setNestedList(Arrays.asList(nested));
              producer.newMessage().value(pojo).key("bar").send();

              // the JMS client reads Schema AVRO as TextMessage
              MapMessage message = (MapMessage) consumer.receive();
              assertEquals("foo", message.getString("name"));
              Map<String, Object> nestedValue = (Map<String, Object>) message.getObject("nested");
              assertEquals(23, nestedValue.get("age"));
              assertEquals("bar", message.getStringProperty("JMSXGroupID"));
              Map<String, Object> nestedPojo = (Map<String, Object>) nestedValue.get("pojo");
              assertEquals("foo2", nestedPojo.get("name"));

              List<Map<String, Object>> nestedValueList =
                  (List<Map<String, Object>>) message.getObject("nestedList");
              nestedValue = nestedValueList.get(0);
              assertEquals(23, nestedValue.get("age"));
              assertEquals("bar", message.getStringProperty("JMSXGroupID"));
              nestedPojo = (Map<String, Object>) nestedValue.get("pojo");
              assertEquals("foo2", nestedPojo.get("name"));
            }
          }
        }
      }
    }
  }

  @Test
  public void avroKeyValueSchemaTest() throws Exception {

    Map<String, Object> properties = pulsarContainer.buildJMSConnectionProperties();
    Map<String, Object> consumerConfig = new HashMap<>();
    properties.put("consumerConfig", consumerConfig);
    consumerConfig.put("useSchema", true);

    try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties); ) {
      try (JMSContext context = factory.createContext()) {

        String topic = "persistent://public/default/test-" + UUID.randomUUID();
        Destination destination = context.createTopic(topic);

        try (PulsarClient client =
            PulsarClient.builder().serviceUrl(pulsarContainer.getBrokerUrl()).build(); ) {
          try (JMSConsumer consumer = context.createConsumer(destination)) {
            try (Producer<KeyValue<Nested, Pojo>> producer =
                client
                    .newProducer(
                        Schema.KeyValue(
                            Schema.AVRO(Nested.class),
                            Schema.AVRO(Pojo.class),
                            KeyValueEncodingType.INLINE))
                    .topic(topic)
                    .create(); ) {
              Pojo pojo = new Pojo();
              pojo.setName("foo");
              Nested nested = new Nested();
              nested.setAge(23);

              Pojo pojo2 = new Pojo();
              pojo2.setName("foo2");
              nested.setPojo(pojo2);

              pojo.setNested(nested);
              pojo.setNestedList(Arrays.asList(nested));

              KeyValue<Nested, Pojo> keyValue = new KeyValue<>(nested, pojo);

              producer.newMessage().value(keyValue).send();

              // the JMS client reads Schema AVRO as TextMessage
              MapMessage message = (MapMessage) consumer.receive();

              Map<String, Object> key = (Map<String, Object>) message.getObject("key");
              assertEquals(23, key.get("age"));

              Map<String, Object> value = (Map<String, Object>) message.getObject("value");

              assertEquals("foo", value.get("name"));
              Map<String, Object> nestedValue = (Map<String, Object>) value.get("nested");
              assertEquals(23, nestedValue.get("age"));
              Map<String, Object> nestedPojo = (Map<String, Object>) nestedValue.get("pojo");
              assertEquals("foo2", nestedPojo.get("name"));

              List<Map<String, Object>> nestedValueList =
                  (List<Map<String, Object>>) value.get("nestedList");
              nestedValue = nestedValueList.get(0);
              assertEquals(23, nestedValue.get("age"));
              nestedPojo = (Map<String, Object>) nestedValue.get("pojo");
              assertEquals("foo2", nestedPojo.get("name"));
            }
          }
        }
      }
    }
  }
}
