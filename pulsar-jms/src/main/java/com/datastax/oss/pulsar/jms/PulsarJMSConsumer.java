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
package io.streamnative.oss.pulsar.jms;

import javax.jms.JMSConsumer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageListener;

public class PulsarJMSConsumer implements JMSConsumer {

  private final PulsarMessageConsumer pulsarMessageConsumer;

  public PulsarJMSConsumer(PulsarMessageConsumer pulsarMessageConsumer) {
    this.pulsarMessageConsumer = pulsarMessageConsumer;
  }

  public PulsarMessageConsumer asPulsarMessageConsumer() {
    return pulsarMessageConsumer;
  }

  @Override
  public String getMessageSelector() {
    return Utils.runtimeException(() -> pulsarMessageConsumer.getMessageSelector());
  }

  @Override
  public MessageListener getMessageListener() throws JMSRuntimeException {
    return Utils.runtimeException(() -> pulsarMessageConsumer.getMessageListener());
  }

  @Override
  public void setMessageListener(MessageListener listener) throws JMSRuntimeException {
    Utils.runtimeException(() -> pulsarMessageConsumer.setMessageListener(listener));
  }

  @Override
  public Message receive() {
    return Utils.runtimeException(() -> pulsarMessageConsumer.receive());
  }

  @Override
  public Message receive(long timeout) {
    return Utils.runtimeException(() -> pulsarMessageConsumer.receive(timeout));
  }

  @Override
  public Message receiveNoWait() {
    return Utils.runtimeException(() -> pulsarMessageConsumer.receiveNoWait());
  }

  @Override
  public void close() {
    Utils.runtimeException(() -> pulsarMessageConsumer.close());
  }

  @Override
  public <T> T receiveBody(Class<T> c) {
    return Utils.runtimeException(
        () -> {
          Message msg = pulsarMessageConsumer.receiveWithTimeoutAndValidateType(Long.MAX_VALUE, c);
          return msg == null ? null : msg.getBody(c);
        });
  }

  @Override
  public <T> T receiveBody(Class<T> c, long timeout) {
    return Utils.runtimeException(
        () -> {
          Message msg = pulsarMessageConsumer.receiveWithTimeoutAndValidateType(timeout, c);
          return msg == null ? null : msg.getBody(c);
        });
  }

  @Override
  public <T> T receiveBodyNoWait(Class<T> c) {
    return Utils.runtimeException(
        () -> {
          Message msg = pulsarMessageConsumer.receiveWithTimeoutAndValidateType(1, c);
          return msg == null ? null : msg.getBody(c);
        });
  }
}
