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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;

@Slf4j
public class MessagePriorityGrowableArrayBlockingQueue extends GrowableArrayBlockingQueue<Message> {

  static int getPriority(Message m) {
    return PulsarMessage.readJMSPriority(m, PulsarMessage.DEFAULT_PRIORITY);
  }

  private final PriorityBlockingQueue<Pair<Integer, Message>> queue;
  private final AtomicBoolean terminated = new AtomicBoolean(false);

  private volatile Consumer<Message> itemAfterTerminatedHandler;
  private final AtomicInteger[] numberMessagesByPrority = new AtomicInteger[10];

  private static final Comparator<Pair<Integer, Message>> comparator =
      (o1, o2) -> {
        int priority1 = o1.getLeft();
        int priority2 = o2.getLeft();
        if (priority1 == priority2) {
          // if priorities are equal, we want to sort by messageId
          return o2.getRight().getMessageId().compareTo(o1.getRight().getMessageId());
        }
        return Integer.compare(priority2, priority1);
      };

  public MessagePriorityGrowableArrayBlockingQueue() {
    this(10);
  }

  public MessagePriorityGrowableArrayBlockingQueue(int initialCapacity) {
    queue = new PriorityBlockingQueue<>(initialCapacity, comparator);
    for (int i = 0; i < 10; i++) {
      numberMessagesByPrority[i] =  new AtomicInteger();
    }
  }

  @Override
  public Message remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Message poll() {
    Pair<Integer, Message> pair = queue.poll();
    if (pair == null) {
      return null;
    }
    Message result = pair.getRight();
    int prio = pair.getLeft();
    if (log.isDebugEnabled()) {
      log.debug("polled message prio {}  {}  stats {}", prio, result.getMessageId(), Arrays.toString(numberMessagesByPrority));
    }
    numberMessagesByPrority[prio].decrementAndGet();
    return result;
  }

  @Override
  public Message element() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Message peek() {
    Pair<Integer, Message> pair = queue.peek();
    if (pair == null) {
      return null;
    }
    Message result = pair.getRight();
    log.info("peeking message: {} prio {}", result.getMessageId(), getPriority(result));
    return result;
  }

  @Override
  public boolean offer(Message e) {

    boolean result;
    if (!this.terminated.get()) {
      int prio = getPriority(e);
      numberMessagesByPrority[prio].incrementAndGet();
      result = queue.offer(Pair.of(prio, e));
      if (log.isDebugEnabled()) {
        log.debug("offered message: {} prio {} stats {}",
                e.getMessageId(), getPriority(e), Arrays.toString(numberMessagesByPrority));
      }
    } else {
      log.info("queue is terminated, not offering message: {}", e.getMessageId());
      if (itemAfterTerminatedHandler != null) {
        itemAfterTerminatedHandler.accept(e);
      }
      result = false;
    }
    return result;
  }

  @Override
  public void put(Message e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean add(Message e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean offer(Message e, long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Message take() throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Message poll(long timeout, TimeUnit unit) throws InterruptedException {
    Pair<Integer, Message> pair = queue.poll(timeout, unit);
    if (pair == null) {
      return null;
    }
    Message result = pair.getRight();
    int prio = pair.getLeft();
    if (log.isDebugEnabled()) {
      log.debug(
              "polled message (tm {} {}):prio {}  {} stats {}",
              timeout,
              unit,
              prio,
              result.getMessageId(),
              Arrays.toString(numberMessagesByPrority));
    }
    numberMessagesByPrority[prio].decrementAndGet();
    return result;
  }

  @Override
  public void clear() {
    queue.clear();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public void forEach(Consumer<? super Message> action) {
    queue.stream().sorted(comparator).forEach(x -> action.accept(x.getRight()));
  }

  @Override
  public String toString() {
    return queue.toString();
  }

  @Override
  public void terminate(Consumer<Message> itemAfterTerminatedHandler) {
    this.itemAfterTerminatedHandler = itemAfterTerminatedHandler;
    terminated.set(true);
  }

  @Override
  public boolean isTerminated() {
    return terminated.get();
  }

  @Override
  public int remainingCapacity() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int drainTo(Collection<? super Message> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int drainTo(Collection<? super Message> c, int maxElements) {
    throw new UnsupportedOperationException();
  }


  @Override
  public Iterator<Message> iterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Message> toList() {
    throw new UnsupportedOperationException();
  }


}
