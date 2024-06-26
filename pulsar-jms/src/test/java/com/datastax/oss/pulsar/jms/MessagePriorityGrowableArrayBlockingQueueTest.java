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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.jupiter.api.Test;

class MessagePriorityGrowableArrayBlockingQueueTest {
  @Test
  public void basicTest() {
    test(1, 2, 9);
    test(2, 1, 9);
    test(2, 9, 1);
  }

  private static void test(int... priorities) {
    List<Integer> prio = new ArrayList<>();
    for (int i : priorities) {
      prio.add(i);
    }

    List<Integer> sorted = prio;
    sorted.sort(Comparator.reverseOrder());

    MessagePriorityGrowableArrayBlockingQueue<String> queue =
        new MessagePriorityGrowableArrayBlockingQueue<>();
    int position = 0;
    for (int i : priorities) {
      queue.offer(messageWithPriority(i, position++));
    }

    List<Integer> prioritiesForEach = new ArrayList<>();
    queue.forEach(
        m -> {
          System.out.println("prio: " + m.getProperty("JMSPriority"));
          prioritiesForEach.add(PulsarMessage.readJMSPriority(m));
        });
    assertEquals(prioritiesForEach, sorted);

    List<Integer> polledPriorities = new ArrayList<>();
    while (queue.peek() != null) {
      Message message = queue.poll();
      polledPriorities.add(Integer.parseInt(message.getProperty("JMSPriority")));
    }
    assertEquals(polledPriorities, sorted);
  }

  private static Message<String> messageWithPriority(int priority, int position) {
    Message<String> message = mock(Message.class);
    when(message.hasProperty(eq("JMSPriority"))).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn(priority + "");
    when(message.getMessageId()).thenReturn(new MessageIdImpl(1, position, 1));
    return message;
  }
}
