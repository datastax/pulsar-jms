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
package io.streamnative.oss.pulsar.jms.messages;

import io.streamnative.oss.pulsar.jms.PulsarMessage;
import io.streamnative.oss.pulsar.jms.Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.ObjectMessage;
import org.apache.pulsar.client.api.TypedMessageBuilder;

public final class PulsarObjectMessage extends PulsarMessage implements ObjectMessage {

  private Serializable object;

  public PulsarObjectMessage(Serializable object) throws JMSException {
    this(encode(object)); // clone object
  }

  public PulsarObjectMessage(byte[] originalMessage) throws JMSException {
    this.object = decode(originalMessage);
  }

  private static Serializable decode(byte[] originalMessage) throws JMSException {
    if (originalMessage == null) {
      return null;
    }
    try {
      ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(originalMessage));
      return (Serializable) input.readUnshared();
    } catch (Exception err) {
      throw Utils.handleException(err);
    }
  }

  public PulsarObjectMessage() {}

  @Override
  protected String messageType() {
    return "object";
  }

  @Override
  public boolean isBodyAssignableTo(Class c) throws JMSException {
    return c.isAssignableFrom(Serializable.class) || (object != null && c.isInstance(object));
  }

  @Override
  public void clearBody() throws JMSException {
    this.writable = true;
    this.object = null;
  }

  @Override
  public <T> T getBody(Class<T> c) throws JMSException {
    if (object == null) {
      return null;
    }
    return Utils.invoke(() -> c.cast(object));
  }

  @Override
  protected void prepareForSend(TypedMessageBuilder<byte[]> producer) throws JMSException {
    byte[] encoded = encode(object);
    producer.value(encoded);
  }

  private static byte[] encode(Object object) throws JMSException {
    if (object == null) {
      return null;
    }
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutputStream oo = new ObjectOutputStream(out);
      oo.writeUnshared(object);
      oo.flush();
      oo.close();
      return out.toByteArray();
    } catch (Exception err) {
      throw Utils.handleException(err);
    }
  }

  /**
   * Sets the serializable object containing this message's data. It is important to note that an
   * {@code ObjectMessage} contains a snapshot of the object at the time {@code setObject()} is
   * called; subsequent modifications of the object will have no effect on the {@code ObjectMessage}
   * body.
   *
   * @param object the message's data
   * @throws JMSException if the JMS provider fails to set the object due to some internal error.
   * @throws MessageFormatException if object serialization fails.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  @Override
  public void setObject(Serializable object) throws JMSException {
    checkWritable();
    this.object = decode(encode(object)); // clone
  }

  /**
   * Gets the serializable object containing this message's data. The default value is null.
   *
   * @return the serializable object containing this message's data
   * @throws JMSException if the JMS provider fails to get the object due to some internal error.
   * @throws MessageFormatException if object deserialization fails.
   */
  @Override
  public Serializable getObject() throws JMSException {
    return object;
  }

  @Override
  public String toString() {
    if (object == null) {
      return "PulsarObjectMessage{null," + properties + "}";
    } else {
      return "PulsarObjectMessage{" + object + "," + object.getClass() + "," + properties + "}";
    }
  }
}
