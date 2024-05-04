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

import com.google.common.collect.Streams;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;

public class MessagePriorityGrowableArrayBlockingQueue extends GrowableArrayBlockingQueue<Message> {

  static int getPriority(Message m) {
    return PulsarMessage.readJMSPriority(m, PulsarMessage.DEFAULT_PRIORITY);
  }

  // for drainTo only, needs .add() method and that's it
  private static class ForwardingCollection implements Collection<Pair<Integer, Message>> {

    private final Collection<? super Message> c;

    public ForwardingCollection(Collection<? super Message> c) {
      this.c = c;
    }

    @Override
    public int size() {
      throw new IllegalStateException("Not supported");
    }

    @Override
    public boolean isEmpty() {
      throw new IllegalStateException("Not supported");
    }

    @Override
    public boolean contains(Object o) {
      throw new IllegalStateException("Not supported");
    }

    @Override
    public Iterator<Pair<Integer, Message>> iterator() {
      throw new IllegalStateException("Not supported");
    }

    @Override
    public Object[] toArray() {
      throw new IllegalStateException("Not supported");
    }

    @Override
    public <T> T[] toArray(T[] a) {
      throw new IllegalStateException("Not supported");
    }

    @Override
    public boolean add(Pair<Integer, Message> message) {
      return c.add(message.getRight());
    }

    @Override
    public boolean remove(Object o) {
      throw new IllegalStateException("Not supported");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new IllegalStateException("Not supported");
    }

    @Override
    public boolean addAll(Collection<? extends Pair<Integer, Message>> c) {
      throw new IllegalStateException("Not supported");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new IllegalStateException("Not supported");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new IllegalStateException("Not supported");
    }

    @Override
    public void clear() {}

    @Override
    public boolean equals(Object o) {
      // shouldn't be used, but we need to keep spotbugs happy
      return this == o || c.equals(o);
    }

    @Override
    public int hashCode() {
      return c.hashCode();
    }
  }

  private final PriorityBlockingQueue<Pair<Integer, Message>> queue;
  private final AtomicBoolean terminated = new AtomicBoolean(false);

  private static final Comparator<Pair<Integer, Message>> comparator =
      (o1, o2) -> {
        int priority1 = getPriority(o1.getRight());
        int priority2 = getPriority(o2.getRight());
        return Integer.compare(priority2, priority1);
      };

  public MessagePriorityGrowableArrayBlockingQueue() {
    this(10);
  }

  public MessagePriorityGrowableArrayBlockingQueue(int initialCapacity) {
    queue = new PriorityBlockingQueue<>(initialCapacity, comparator);
  }

  @Override
  public Message remove() {
    return queue.remove().getRight();
  }

  @Override
  public Message poll() {
    Pair<Integer, Message> pair = queue.poll();
    if (pair == null) {
      return null;
    }
    return pair.getRight();
  }

  @Override
  public Message element() {
    return queue.element().getRight();
  }

  @Override
  public Message peek() {
    Pair<Integer, Message> pair = queue.peek();
    if (pair == null) {
      return null;
    }
    return pair.getRight();
  }

  @Override
  public boolean offer(Message e) {
    return queue.offer(Pair.of(getPriority(e), e));
  }

  @Override
  public void put(Message e) {
    queue.put(Pair.of(getPriority(e), e));
  }

  @Override
  public boolean add(Message e) {
    return queue.add(Pair.of(getPriority(e), e));
  }

  @Override
  public boolean offer(Message e, long timeout, TimeUnit unit) {
    return queue.offer(Pair.of(getPriority(e), e), timeout, unit);
  }

  @Override
  public Message take() throws InterruptedException {
    return queue.take().getRight();
  }

  @Override
  public Message poll(long timeout, TimeUnit unit) throws InterruptedException {
    Pair<Integer, Message> pair = queue.poll(timeout, unit);
    if (pair == null) {
      return null;
    }
    return pair.getRight();
  }

  @Override
  public int remainingCapacity() {
    return queue.remainingCapacity();
  }

  @Override
  public int drainTo(Collection<? super Message> c) {
    return queue.drainTo(new ForwardingCollection(c));
  }

  @Override
  public int drainTo(Collection<? super Message> c, int maxElements) {
    return queue.drainTo(new ForwardingCollection(c), maxElements);
  }

  @Override
  public void clear() {
    queue.clear();
  }

  @Override
  public boolean remove(Object o) {
    for (Pair<Integer, Message> pair : queue) {
      if (pair.getRight().equals(o)) {
        return queue.remove(pair);
      }
    }
    return false;
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public Iterator<Message> iterator() {
    return Streams.stream(queue.iterator()).sorted(comparator).map(Pair::getRight).iterator();
  }

  @Override
  public List<Message> toList() {
    List<Message> list = new ArrayList<>(size());
    forEach(list::add);
    return list;
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
    terminated.set(true);
  }

  @Override
  public boolean isTerminated() {
    return terminated.get();
  }
}
