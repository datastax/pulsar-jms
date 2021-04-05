package com.datastax.oss.pulsar.jms;

import com.datastax.oss.pulsar.jms.utils.PulsarCluster;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class SimpleTest {

    @TempDir
    public Path tempDir;

    @Test
    public void sendMessageTest() throws Exception{

        try (PulsarCluster cluster = new PulsarCluster(tempDir)) {
            Map<String, Object> properties = new HashMap<>();
            properties.put("webserviceUrl", cluster.getAddress());
            try (PulsarConnectionFactory factory = new PulsarConnectionFactory(properties);) {
                try (Connection connection = factory.createConnection()) {
                    try (Session session = connection.createSession();) {
                        Destination destination = session.createTopic("test");
                        try (MessageProducer producer = session.createProducer(destination);) {
                            TextMessage msg = session.createTextMessage();
                            msg.setText("foo");
                            producer.send(msg);
                        }
                    }
                }
            }
        }
    }
}
