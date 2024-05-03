package com.datastax.oss.pulsar.jms;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.pulsar.client.api.Message;
import org.junit.jupiter.api.Test;

class MessagePriorityGrowableArrayBlockingQueueTest {
    @Test
    public void basicTest() {
        MessagePriorityGrowableArrayBlockingQueue queue = new MessagePriorityGrowableArrayBlockingQueue();


        queue.add(messageWithPriority(1));
        dumpQueue(queue);
        queue.add(messageWithPriority(2));
        dumpQueue(queue);
        queue.add(messageWithPriority(10));
        dumpQueue(queue);



    }

    private static void dumpQueue(MessagePriorityGrowableArrayBlockingQueue queue) {
        List<Integer> priorities = new ArrayList<>();
        queue.forEach(m -> {
            System.out.println("prio: "+ m.getProperty("JMSPriority"));
            priorities.add(Integer.parseInt(m.getProperty("JMSPriority")));
        });

        List<Integer> sorted = new ArrayList<>(priorities);
        sorted.sort(Comparator.reverseOrder());

        assertEquals(priorities, sorted);
    }

    private static Message messageWithPriority(int priority) {
        Message message = mock(Message.class);
        when(message.hasProperty(eq("JMSPriority"))).thenReturn(true);
        when(message.getProperty("JMSPriority")).thenReturn(priority + "");
        return message;
    }
}