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
package com.datastax.oss.pulsar.jms.messages;

import com.datastax.oss.pulsar.jms.PulsarMessage;
import jakarta.jms.JMSException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

public final class PulsarSimpleMessage extends PulsarMessage {
  @Override
  public void clearBody() throws JMSException {}

  @Override
  public <T> T getBody(Class<T> c) throws JMSException {
    return null;
  }

  @Override
  public boolean isBodyAssignableTo(Class c) throws JMSException {
    return false;
  }

  @Override
  protected void prepareForSend(TypedMessageBuilder<byte[]> producer) throws JMSException {
    // null value
    producer.value(null);
  }

  @Override
  protected String messageType() {
    return "header";
  }

  public String toString() {
    return "SimpleMessage{" + properties + "}";
  }
}
