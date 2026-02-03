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
import com.datastax.oss.pulsar.jms.Utils;
import java.nio.charset.StandardCharsets;
import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.jms.TextMessage;
import org.apache.pulsar.client.api.TypedMessageBuilder;

public final class PulsarTextMessage extends PulsarMessage implements TextMessage {
  private String text;

  public PulsarTextMessage(byte[] payload) {
    if (payload == null) {
      this.text = null;
    } else {
      this.text = new String(payload, StandardCharsets.UTF_8);
    }
  }

  public PulsarTextMessage(String text) {
    this.text = text;
  }

  @Override
  protected String messageType() {
    return "text";
  }

  @Override
  public boolean isBodyAssignableTo(Class c) {
    return c == String.class;
  }

  @Override
  public void clearBody() throws JMSException {
    this.text = null;
  }

  @Override
  public <T> T getBody(Class<T> c) throws JMSException {
    return Utils.invoke(() -> c.cast(text));
  }

  @Override
  protected void prepareForSend(TypedMessageBuilder<byte[]> producer) throws JMSException {
    if (text == null) {
      producer.value(null);
    } else {
      producer.value(text.getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * Sets the string containing this message's data.
   *
   * @param string the {@code String} containing the message's data
   * @throws JMSException if the JMS provider fails to set the text due to some internal error.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  @Override
  public void setText(String string) throws JMSException {
    this.text = string;
  }

  /**
   * Gets the string containing this message's data. The default value is null.
   *
   * @return the {@code String} containing the message's data
   * @throws JMSException if the JMS provider fails to get the text due to some internal error.
   */
  @Override
  public String getText() throws JMSException {
    return text;
  }

  @Override
  public String toString() {
    return "PulsarTextMessage{" + text + "," + properties + "}";
  }
}
