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

import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;

public class PulsarMessageTemporaryProducer extends PulsarMessageProducer {

  public PulsarMessageTemporaryProducer(PulsarSession session, Destination defaultDestination)
      throws JMSException {
    super(session, defaultDestination);
  }

  /**
   * Closes the message producer.
   *
   * <p>Since a provider may allocate some resources on behalf of a {@code MessageProducer} outside
   * the Java virtual machine, clients should close them when they are not needed. Relying on
   * garbage collection to eventually reclaim these resources may not be timely enough.
   *
   * <p>This method must not return until any incomplete asynchronous send operations for this
   * <code>MessageProducer</code> have been completed and any <code>CompletionListener</code>
   * callbacks have returned. Incomplete sends should be allowed to complete normally unless an
   * error occurs.
   *
   * <p>A <code>CompletionListener</code> callback method must not call <code>close</code> on its
   * own <code>MessageProducer</code>. Doing so will cause an <code>IllegalStateException</code> to
   * be thrown.
   *
   * @throws IllegalStateException this method has been called by a <code>CompletionListener</code>
   *     callback method on its own <code>MessageProducer</code>
   * @throws JMSException if the JMS provider fails to close the producer due to some internal
   *     error.
   */
  @Override
  public void close() throws JMSException {
    Utils.checkNotOnMessageProducer(session, this);
    session.closeTemporaryProducerForDestination(defaultDestination);
    closed = true;
  }
}
