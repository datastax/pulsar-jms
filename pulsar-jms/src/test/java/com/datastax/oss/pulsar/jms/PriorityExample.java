package com.datastax.oss.pulsar.jms;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.Queue;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.common.policies.data.BacklogQuota;

public class PriorityExample
{


    public static void main(String[] args) throws Exception
    {
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put("batchingEnabled", false);
        Map<String, Object> consumerConfig = new HashMap<>();

        // Observation:
        // receiverQueueSize = 128
        // 128 stay in the queue for prio 0
        // at every re-fill round we get 64 prio 9 messages and stop refilling for 9
        // at every re-fill round we get 64 prio 0 messages and stop refilling for 0

        consumerConfig.put("receiverQueueSize", "128");
        //consumerConfig.put("maxTotalReceiverQueueSizeAcrossPartitions", "64");
        try (PulsarConnectionFactory factory = new PulsarConnectionFactory(
        ImmutableMap.of("jms.useServerSideFiltering", "true", "jms.clientId",
                "test", "producerConfig", producerConfig,
                "consumerConfig", consumerConfig,
                "jms.emulateTransactions", true,
                "jms.enableJMSPriority", "true"));) {

            Queue topic1;
            try (JMSContext jmsContext = factory.createContext()) {
                topic1 = jmsContext.createQueue("mytopic");
                factory.getPulsarAdmin().namespaces().setBacklogQuota("public/default", BacklogQuota
                        .builder()
                        .limitSize(-1)
                        .limitTime(-1)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold)
                        .build());
                try {
                    factory.getPulsarAdmin().topics().deletePartitionedTopic(topic1.getQueueName());
                } catch (PulsarAdminException.NotFoundException notFoundException) {

                }
                factory.getPulsarAdmin().topics().createPartitionedTopic(topic1.getQueueName(), 10);
            }

            try (JMSContext jmsContext = factory.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
                 )
            {
                try (JMSConsumer consumer = jmsContext.createConsumer(topic1, null);){
                }

                int numMessages = 10000;
                for (int i = 0; i < numMessages; i++) {
                    JMSProducer producer = jmsContext.createProducer();
                    if (i % 2 == 0) {
                        producer.setPriority(0);
                    } else {
                        producer.setPriority(9);
                    }
                    String text = "text-"+i+"-"+producer.getPriority();
                    producer.send(topic1, text);
                    //System.out.println("Sent message "+ i + " with priority: " + producer.getPriority() + " and body: " + text);
                    if (i % 1000 == 0) {
                        System.out.println("Sent " + i + " messages");
                    }
                }

                int lastPrio = -1;
                int groupSize = 0;
                try (JMSConsumer consumer = jmsContext.createConsumer(topic1, null);) {
                    for (int i = 0; i < numMessages; i++) {



                        Message receive = consumer.receive();
                        if (lastPrio != receive.getJMSPriority() && i > 0)
                        {
                            System.out.println("************************************************************************+");
                            System.out.println("AFTER "+groupSize +" with prio "+lastPrio);
                            dumpInternalQueue(consumer);
                            groupSize = 0;
                        }
                        lastPrio = receive.getJMSPriority();
                        groupSize++;
                            System.out.println("Received priority: " + receive.getJMSPriority() + " and body: "
                                + receive.getBody(String.class));
                        Thread.sleep(10);
                        receive.acknowledge();
                    }
                    System.out.println("************************************************************************+");
                    System.out.println("AFTER "+groupSize +" with prio "+lastPrio);
                    dumpInternalQueue(consumer);
                }
            }


        }
    }

    private static void dumpInternalQueue(JMSConsumer consumer) throws Exception {
        PulsarMessageConsumer pulsarMessageConsumer = ((PulsarJMSConsumer) consumer).asPulsarMessageConsumer();
        ConsumerBase<?> consumer1 = pulsarMessageConsumer.getConsumer();
        Field incomingMessages = ConsumerBase.class.getDeclaredField("incomingMessages");
        incomingMessages.setAccessible(true);

        Field pausedConsumers = MultiTopicsConsumerImpl.class.getDeclaredField("pausedConsumers");
        pausedConsumers.setAccessible(true);
        Object o = pausedConsumers.get(consumer1);

        MessagePriorityGrowableArrayBlockingQueue oldQueue =
                (MessagePriorityGrowableArrayBlockingQueue) incomingMessages.get(consumer1);
        System.out.println("Contents of the internal queue: "+ oldQueue.getPriorityStats()+" paused "+o);

    }

}