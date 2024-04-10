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

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;

import javax.annotation.security.RunAs;

@Slf4j
public class MessagePriorityGrowableArrayBlockingQueue extends GrowableArrayBlockingQueue<Message> {

  @AllArgsConstructor
  static class StaleMessagesChecker implements Runnable {
    private final MessagePriorityGrowableArrayBlockingQueue queue;
    private final org.apache.pulsar.client.api.Consumer consumer;

    @Override
    public void run() {
      try {
        long now = System.currentTimeMillis();
        long expireBefore = now - TimeUnit.SECONDS.toMillis(10);
        List<MessageInfo> removed = new ArrayList<>();
        queue.queue.forEach((info) -> {
          if (info.insertTs <= expireBefore) {
            removed.add(info);
          }
        });
        queue.queue.removeAll(removed);
        for (MessageInfo messageInfo : removed) {

          try {
            // this operation shouldn't fail because we're simply adding it to the nack backlog
            // but it's better to be conservative
            consumer.negativeAcknowledge(messageInfo.message);
            log.info("Message {} was stale (spent {} seconds in the queue)", messageInfo.message, (now - messageInfo.insertTs) / 1000);
          } catch (Throwable ex) {
            log.error("Failed to append message to nack queue", ex);
            queue.add(messageInfo.message);
          }
        }
      } catch (Throwable ex) {
        log.error("Failed to detect stale messages for consumer {}", consumer, ex);
      }
    }
  }

  @Data
  @AllArgsConstructor
  static final class MessageInfo {
    Message message;
    long insertTs;
  }


  static int getPriority(Message m) {
    Integer priority = PulsarMessage.readJMSPriority(m);
    return priority == null ? PulsarMessage.DEFAULT_PRIORITY : priority;
  }

  private final PriorityBlockingQueue<MessageInfo> queue;
  private final AtomicBoolean terminated = new AtomicBoolean(false);

  public MessagePriorityGrowableArrayBlockingQueue() {
    this(10);
  }

  public MessagePriorityGrowableArrayBlockingQueue(int initialCapacity) {
    queue =
        new PriorityBlockingQueue<>(
            initialCapacity,
            new Comparator<MessageInfo>() {
              @Override
              public int compare(MessageInfo o1, MessageInfo o2) {
                int priority1 = getPriority(o1.message);
                int priority2 = getPriority(o2.message);
                return Integer.compare(priority2, priority1);
              }
            });
  }

  @Override
  public Message remove() {
    return toMessage(queue.remove());
  }

  private static Message toMessage(MessageInfo info) {
    if (info == null) {
      return null;
    }
    return info.message;
  }
  private static MessageInfo newInfo(Message msg) {
    return new MessageInfo(msg, System.currentTimeMillis());
  }

  @Override
  public Message poll() {
    return toMessage(queue.poll());
  }

  @Override
  public Message element() {
    return toMessage(queue.element());
  }

  @Override
  public Message peek() {
    return toMessage(queue.peek());
  }

  @Override
  public boolean offer(Message e) {
    return queue.offer(newInfo(e));
  }

  @Override
  public void put(Message e) {
    queue.put(newInfo(e));
  }

  @Override
  public boolean add(Message e) {
    return queue.add(newInfo(e));
  }

  @Override
  public boolean offer(Message e, long timeout, TimeUnit unit) {
    return queue.offer(newInfo(e), timeout, unit);
  }

  @Override
  public Message take() throws InterruptedException {
    return toMessage(queue.take());
  }

  @Override
  public Message poll(long timeout, TimeUnit unit) throws InterruptedException {
    return toMessage(queue.poll(timeout, unit));
  }

  @Override
  public int remainingCapacity() {
    return queue.remainingCapacity();
  }

  @Override
  public int drainTo(Collection<? super Message> c) {
    return queue.drainTo(c.stream().map(msg -> newInfo((Message) msg)).collect(Collectors.toList()));
  }

  @Override
  public int drainTo(Collection<? super Message> c, int maxElements) {
    return queue.drainTo(c.stream().map(msg -> newInfo((Message) msg)).collect(Collectors.toList()), maxElements);
  }

  @Override
  public void clear() {
    queue.clear();
  }

  @Override
  public boolean remove(Object o) {
    return queue.remove(o);
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public Iterator<Message> iterator() {
    Iterator<MessageInfo> iterator = queue.iterator();
    return new Iterator<Message>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Message next() {
        return iterator.next().message;
      }
    };
  }

  @Override
  public List<Message> toList() {
    List<Message> list = new ArrayList<>(size());
    forEach(list::add);
    return list;
  }

  @Override
  public void forEach(Consumer<? super Message> action) {
    queue.forEach(messageInfo -> action.accept(messageInfo.message));
  }

  @Override
  public String toString() {
    return queue.toString();
  }

  @Override
  public void terminate(Consumer<Message> itemAfterTerminatedHandler) {
    terminated.set(true);
  }

  @Override
  public boolean isTerminated() {
    return terminated.get();
  }
}
