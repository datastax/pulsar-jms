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
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;

public class MessagePriorityGrowableArrayBlockingQueue extends GrowableArrayBlockingQueue<Message> {

  static int getPriority(Message m) {
    Integer priority = PulsarMessage.readJMSPriority(m);
    return priority == null ? PulsarMessage.DEFAULT_PRIORITY : priority;
  }

  private final PriorityBlockingQueue<Message> queue;
  private final AtomicBoolean terminated = new AtomicBoolean(false);

  private static final Comparator<Message> comparator =
      (o1, o2) -> {
        int priority1 = getPriority(o1);
        int priority2 = getPriority(o2);
        return Integer.compare(priority2, priority1);
      };

  public MessagePriorityGrowableArrayBlockingQueue() {
    this(10);
  }

  public MessagePriorityGrowableArrayBlockingQueue(int initialCapacity) {
    queue =
        new PriorityBlockingQueue<>(initialCapacity, comparator);
  }

  @Override
  public Message remove() {
    return queue.remove();
  }

  @Override
  public Message poll() {
    return queue.poll();
  }

  @Override
  public Message element() {
    return queue.element();
  }

  @Override
  public Message peek() {
    return queue.peek();
  }

  @Override
  public boolean offer(Message e) {
    return queue.offer(e);
  }

  @Override
  public void put(Message e) {
    queue.put(e);
  }

  @Override
  public boolean add(Message e) {
    return queue.add(e);
  }

  @Override
  public boolean offer(Message e, long timeout, TimeUnit unit) {
    return queue.offer(e, timeout, unit);
  }

  @Override
  public Message take() throws InterruptedException {
    return queue.take();
  }

  @Override
  public Message poll(long timeout, TimeUnit unit) throws InterruptedException {
    return queue.poll(timeout, unit);
  }

  @Override
  public int remainingCapacity() {
    return queue.remainingCapacity();
  }

  @Override
  public int drainTo(Collection<? super Message> c) {
    return queue.drainTo(c);
  }

  @Override
  public int drainTo(Collection<? super Message> c, int maxElements) {
    return queue.drainTo(c, maxElements);
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
    return queue.iterator();
  }

  @Override
  public List<Message> toList() {
    List<Message> list = new ArrayList<>(size());
    forEach(list::add);
    return list;
  }

  @Override
  public void forEach(Consumer<? super Message> action) {
    queue.stream().sorted(comparator).forEach(action);
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
