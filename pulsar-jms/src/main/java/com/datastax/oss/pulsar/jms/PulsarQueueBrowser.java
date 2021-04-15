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
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Reader;

@Slf4j
final class PulsarQueueBrowser implements QueueBrowser {
  private final PulsarSession session;
  private final PulsarQueue queue;
  private final Reader<byte[]> reader;
  private final SelectorSupport selectorSupport;

  public PulsarQueueBrowser(PulsarSession session, Queue queue, String selector)
      throws JMSException {
    session.checkNotClosed();
    this.session = session;
    this.queue = (PulsarQueue) queue;
    this.reader = session.getFactory().createReaderForBrowser(this.queue);
    this.selectorSupport = SelectorSupport.build(selector);
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
    return selectorSupport != null ? selectorSupport.getSelector() : null;
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
    return new Enumeration() {
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
                      PulsarMessage.decode(null, reader.readNext(1000, TimeUnit.MILLISECONDS));
                  if (nextMessage == null) {
                    log.info("no message received from browser in time");
                    finished = true;
                    return;
                  }
                  if (selectorSupport != null && !selectorSupport.matches(nextMessage)) {
                    log.info("skip non matching message {}", nextMessage);
                    nextMessage = null;
                  } else {
                    return;
                  }
                }
              }
            });
      }
    };
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
    try {
      reader.close();
    } catch (IOException err) {
    }
    session.getFactory().removeReader(reader);
  }
}
