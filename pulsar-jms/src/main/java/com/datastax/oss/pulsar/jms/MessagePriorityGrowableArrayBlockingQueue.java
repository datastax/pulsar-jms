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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;

@Slf4j
public class MessagePriorityGrowableArrayBlockingQueue<T>
    extends GrowableArrayBlockingQueue<Message<T>> {
  private final PriorityBlockingQueue<MessageWithPriority<T>> queue;
  private final AtomicBoolean terminated = new AtomicBoolean(false);

  private volatile Consumer<Message<T>> itemAfterTerminatedHandler;
  private final AtomicInteger[] numberMessagesByPriority = new AtomicInteger[10];

  @AllArgsConstructor
  private static final class MessageWithPriority<T> {
    final int priority;
    final Message<T> message;
  }

  private static final Comparator<MessageWithPriority<?>> comparator =
      (o1, o2) -> {
        // ORDER BY priority DESC, messageId ASC
        int priority1 = o1.priority;
        int priority2 = o2.priority;
        if (priority1 == priority2) {
          // if priorities are equal, we want to sort by messageId
          return o1.message.getMessageId().compareTo(o2.message.getMessageId());
        }
        return Integer.compare(priority2, priority1);
      };

  public MessagePriorityGrowableArrayBlockingQueue() {
    this(10);
  }

  public MessagePriorityGrowableArrayBlockingQueue(int initialCapacity) {
    queue = new PriorityBlockingQueue<>(initialCapacity, comparator);
    for (int i = 0; i < 10; i++) {
      numberMessagesByPriority[i] = new AtomicInteger();
    }
  }

  @Override
  public Message<T> remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Message<T> poll() {
    MessageWithPriority<T> pair = queue.poll();
    if (pair == null) {
      return null;
    }
    Message<T> result = pair.message;
    int prio = pair.priority;
    if (log.isDebugEnabled()) {
      log.debug(
          "polled message prio {}  {}  stats {}",
          prio,
          result.getMessageId(),
          Arrays.toString(numberMessagesByPriority));
    }
    numberMessagesByPriority[prio].decrementAndGet();
    return result;
  }

  @Override
  public Message<T> element() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Message<T> peek() {
    MessageWithPriority<T> pair = queue.peek();
    if (pair == null) {
      return null;
    }
    Message<T> result = pair.message;
    if (log.isDebugEnabled()) {
      log.debug(
          "peeking message: {} prio {}",
          result.getMessageId(),
          PulsarMessage.readJMSPriority(result));
    }
    return result;
  }

  @Override
  public boolean offer(Message<T> e) {
    boolean result;
    if (!this.terminated.get()) {
      int prio = PulsarMessage.readJMSPriority(e);
      numberMessagesByPriority[prio].incrementAndGet();
      result = queue.offer(new MessageWithPriority(prio, e));
      if (log.isDebugEnabled()) {
        log.debug(
            "offered message: {} prio {} stats {}",
            e.getMessageId(),
            prio,
            Arrays.toString(numberMessagesByPriority));
      }
    } else {
      if (log.isDebugEnabled()) {
        log.debug("queue is terminated, not offering message: {}", e.getMessageId());
      }
      if (itemAfterTerminatedHandler != null) {
        itemAfterTerminatedHandler.accept(e);
      }
      result = false;
    }
    return result;
  }

  @Override
  public void put(Message<T> e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean add(Message<T> e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean offer(Message e, long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Message<T> take() throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Message<T> poll(long timeout, TimeUnit unit) throws InterruptedException {
    MessageWithPriority<T> pair = queue.poll(timeout, unit);
    if (pair == null) {
      return null;
    }
    Message<T> result = pair.message;
    int prio = pair.priority;
    if (log.isDebugEnabled()) {
      log.debug(
          "polled message (tm {} {}):prio {}  {} stats {}",
          timeout,
          unit,
          prio,
          result.getMessageId(),
          Arrays.toString(numberMessagesByPriority));
    }
    numberMessagesByPriority[prio].decrementAndGet();
    return result;
  }

  @Override
  public void clear() {
    queue.clear();
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public void forEach(Consumer<? super Message<T>> action) {
    queue.stream().sorted(comparator).forEach(x -> action.accept(x.message));
  }

  @Override
  public String toString() {
    return "queue:" + queue + ", stats:" + getPriorityStats() + ", terminated:" + terminated.get();
  }

  @Override
  public void terminate(Consumer<Message<T>> itemAfterTerminatedHandler) {
    this.itemAfterTerminatedHandler = itemAfterTerminatedHandler;
    terminated.set(true);
  }

  @Override
  public boolean isTerminated() {
    return terminated.get();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int remainingCapacity() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int drainTo(Collection<? super Message<T>> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int drainTo(Collection<? super Message<T>> c, int maxElements) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<Message<T>> iterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Message<T>> toList() {
    throw new UnsupportedOperationException();
  }

  public String getPriorityStats() {
    return Arrays.toString(numberMessagesByPriority);
  }
}
