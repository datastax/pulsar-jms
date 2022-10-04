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

import com.datastax.oss.pulsar.jms.selectors.SelectorSupport;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionMode;

@Slf4j
final class PulsarQueueBrowser implements QueueBrowser {

  private static final int BROWSER_READ_TIMEOUT = 1000;

  private final PulsarSession session;
  private final PulsarQueue queue;
  private final String subscriptionName;
  private final List<Reader<?>> readers;
  private final SelectorSupport selectorSupport;
  private final Map<String, SelectorSupport> selectorSupportOnSubscriptions = new HashMap<>();
  private final boolean useServerSideFiltering;

  public PulsarQueueBrowser(PulsarSession session, Queue queue, String selector)
      throws JMSException {
    session.checkNotClosed();
    this.session = session;
    this.useServerSideFiltering = session.getFactory().isUseServerSideFiltering();
    this.queue = (PulsarQueue) queue;
    this.readers =
        session
            .getFactory()
            .createReadersForBrowser(this.queue, session.getOverrideConsumerConfiguration());
    log.info("created {} readers for {}", readers.size(), this.queue);
    // we are reading messages and it is always safe to apply selectors
    // on the client side
    this.selectorSupport = SelectorSupport.build(selector, true);
    this.subscriptionName = session.getFactory().getQueueSubscriptionName(this.queue);
  }

  /**
   * Gets the queue associated with this queue browser.
   *
   * @return the queue
   * @throws JMSException if the JMS provider fails to get the queue associated with this browser
   *     due to some internal error.
   */
  @Override
  public Queue getQueue() throws JMSException {
    return queue;
  }

  /**
   * Gets this queue browser's message selector expression.
   *
   * @return this queue browser's message selector, or null if no message selector exists for the
   *     message consumer (that is, if the message selector was not set or was set to null or the
   *     empty string)
   * @throws JMSException if the JMS provider fails to get the message selector for this browser due
   *     to some internal error.
   */
  @Override
  public String getMessageSelector() throws JMSException {
    String selector = internalGetMessageSelector();
    String selectorOnSubscription = internalGetMessageSelectorFromSubscription();
    if (selectorOnSubscription == null) {
      return selector;
    }
    if (selector == null) {
      return selectorOnSubscription;
    }
    return "(" + selectorOnSubscription + ") AND (" + selector + ")";
  }

  private synchronized String internalGetMessageSelector() {
    return selectorSupport != null ? selectorSupport.getSelector() : null;
  }

  private synchronized String internalGetMessageSelectorFromSubscription() {
    if (!useServerSideFiltering || selectorSupportOnSubscriptions.isEmpty()) {
      return null;
    }
    SelectorSupport next = selectorSupportOnSubscriptions.values().iterator().next();
    return next != null ? next.getSelector() : null;
  }

  public synchronized SelectorSupport getSelectorSupportOnSubscription(String topicName)
      throws JMSException {
    if (!useServerSideFiltering) {
      return null;
    }
    if (!selectorSupportOnSubscriptions.containsKey(topicName)) {
      String selector =
          session
              .getFactory()
              .downloadServerSideFilter(topicName, subscriptionName, SubscriptionMode.Durable);
      selectorSupportOnSubscriptions.put(topicName, SelectorSupport.build(selector, true));
    }
    return selectorSupportOnSubscriptions.get(topicName);
  }

  private class MessageEnumeration implements Enumeration {
    private final Reader reader;

    public MessageEnumeration(Reader reader) {
      this.reader = reader;
    }

    PulsarMessage nextMessage;
    boolean finished;

    @Override
    public boolean hasMoreElements() {
      ensureNext();
      return !finished;
    }

    @Override
    public Object nextElement() {
      if (!hasMoreElements()) {
        throw new NoSuchElementException();
      }
      PulsarMessage res = nextMessage;
      nextMessage = null;
      return res;
    }

    private void ensureNext() {
      Utils.runtimeException(
          () -> {
            while (!finished && nextMessage == null) {
              if (!reader.hasMessageAvailable()) {
                finished = true;
                return;
              } else {
                nextMessage =
                    PulsarMessage.decode(
                        null, reader.readNext(BROWSER_READ_TIMEOUT, TimeUnit.MILLISECONDS));
                if (nextMessage == null) {
                  finished = true;
                  return;
                }
                if (selectorSupport != null && !selectorSupport.matches(nextMessage)) {
                  log.debug("skip non matching message {}", nextMessage);
                  nextMessage = null;
                } else {
                  SelectorSupport selectorSupportOnSubscription =
                      getSelectorSupportOnSubscription(
                          nextMessage.getReceivedPulsarMessage().getTopicName());
                  if (selectorSupportOnSubscription != null
                      && !selectorSupportOnSubscription.matches(nextMessage)) {
                    log.debug("skip non matching message {}", nextMessage);
                    nextMessage = null;
                  } else {
                    return;
                  }
                }
              }
            }
          });
    }
  }

  /**
   * Gets an enumeration for browsing the current queue messages in the order they would be
   * received.
   *
   * @return an enumeration for browsing the messages
   * @throws JMSException if the JMS provider fails to get the enumeration for this browser due to
   *     some internal error.
   */
  @Override
  public Enumeration getEnumeration() throws JMSException {
    if (readers.size() == 1) {
      return new MessageEnumeration(readers.get(0));
    }
    List<MessageEnumeration> enumerations =
        readers.stream().map(MessageEnumeration::new).collect(Collectors.toList());
    return new CompositeEnumeration(enumerations);
  }

  /**
   * Closes the {@code QueueBrowser}.
   *
   * <p>Since a provider may allocate some resources on behalf of a QueueBrowser outside the Java
   * virtual machine, clients should close them when they are not needed. Relying on garbage
   * collection to eventually reclaim these resources may not be timely enough.
   *
   * @throws JMSException if the JMS provider fails to close this browser due to some internal
   *     error.
   */
  @Override
  public void close() throws JMSException {
    for (Reader reader : readers) {
      try {
        reader.close();
      } catch (IOException err) {
      }
    }
    session.removeBrowser(this);
    for (Reader reader : readers) {
      session.getFactory().removeReader(reader);
    }
  }
}
