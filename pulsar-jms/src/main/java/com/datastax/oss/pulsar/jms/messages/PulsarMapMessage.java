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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotWriteableException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.TypedMessageBuilder;

@SuppressFBWarnings({"EI_EXPOSE_REP2", "EI_EXPOSE_REP"})
public final class PulsarMapMessage extends PulsarMessage implements MapMessage {

  private final Map<String, Object> map = new HashMap<>();

  public PulsarMapMessage() {
    writable = true;
  }

  public PulsarMapMessage(Map<String, Object> body) throws MessageFormatException {
    this(body, true);
  }

  public PulsarMapMessage(Map<String, Object> body, boolean validate)
      throws MessageFormatException {
    this();
    if (body != null) {
      map.putAll(body);
      if (validate) {
        for (Object value : body.values()) {
          validateWritableObject(value);
        }
      }
    }
  }

  @Override
  protected String messageType() {
    return "map";
  }

  public PulsarMapMessage(byte[] payload) throws JMSException {
    writable = false;
    if (payload != null) {
      try {
        ByteArrayInputStream in = new ByteArrayInputStream(payload);
        ObjectInputStream input = new ObjectInputStream(in);
        int size = input.readInt();
        for (int i = 0; i < size; i++) {
          String key = input.readUTF();
          Object value = input.readUnshared();
          map.put(key, value);
        }
      } catch (Exception err) {
        throw handleExceptionAccordingToMessageSpecs(err);
      }
    }
  }

  @Override
  public void clearBody() throws JMSException {
    writable = true;
    map.clear();
  }

  @Override
  public <T> T getBody(Class<T> c) throws JMSException {
    if (c == Map.class) {
      return (T) map;
    }
    throw new MessageFormatException("only java.util.Map is supported");
  }

  @Override
  public boolean isBodyAssignableTo(Class c) throws JMSException {
    return c == Map.class;
  }

  @Override
  protected void prepareForSend(TypedMessageBuilder<byte[]> producer) throws JMSException {
    if (map.isEmpty()) {
      producer.value(null);
      return;
    }
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutputStream oo = new ObjectOutputStream(out);
      oo.writeInt(map.size());
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        oo.writeUTF(entry.getKey()); // already not null and not empty
        oo.writeUnshared(entry.getValue());
      }
      oo.flush();
      oo.close();
      producer.value(out.toByteArray());
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Returns the {@code boolean} value with the specified name.
   *
   * @param name the name of the {@code boolean}
   * @return the {@code boolean} value with the specified name
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public boolean getBoolean(String name) throws JMSException {
    Object value = map.get(name);
    if (value == null) {
      throw new NullPointerException("Invalid null value (" + map + ")");
    }
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    allowOnlyStrings(value);
    try {
      return Boolean.parseBoolean(value.toString());
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Returns the {@code byte} value with the specified name.
   *
   * @param name the name of the {@code byte}
   * @return the {@code byte} value with the specified name
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public byte getByte(String name) throws JMSException {
    Object value = map.get(name);
    if (value == null) {
      throw new NullPointerException("Invalid null value");
    }
    if (value instanceof Byte) {
      return (Byte) value;
    }
    allowOnlyStrings(value);
    try {
      return Byte.parseByte(value.toString());
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Returns the {@code short} value with the specified name.
   *
   * @param name the name of the {@code short}
   * @return the {@code short} value with the specified name
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public short getShort(String name) throws JMSException {
    Object value = map.get(name);
    if (value == null) {
      throw new NullPointerException("Invalid null value");
    }
    if ((value instanceof Byte) || (value instanceof Short)) {
      return ((Number) value).shortValue();
    }
    allowOnlyStrings(value);
    try {
      return Short.parseShort(value.toString());
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Returns the Unicode character value with the specified name.
   *
   * @param name the name of the Unicode character
   * @return the Unicode character value with the specified name
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public char getChar(String name) throws JMSException {
    return Utils.invoke(() -> (Character) map.get(name));
  }

  /**
   * Returns the {@code int} value with the specified name.
   *
   * @param name the name of the {@code int}
   * @return the {@code int} value with the specified name
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public int getInt(String name) throws JMSException {
    Object value = map.get(name);
    if (value == null) {
      throw new NullPointerException("Invalid null value");
    }
    if ((value instanceof Integer) || (value instanceof Short) || (value instanceof Byte)) {
      return ((Number) value).intValue();
    }
    allowOnlyStrings(value);
    try {
      return Integer.parseInt(value.toString());
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Returns the {@code long} value with the specified name.
   *
   * @param name the name of the {@code long}
   * @return the {@code long} value with the specified name
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public long getLong(String name) throws JMSException {
    Object value = map.get(name);
    if (value == null) {
      throw new NullPointerException("Invalid null value");
    }
    if ((value instanceof Integer)
        || (value instanceof Short)
        || (value instanceof Byte)
        || (value instanceof Long)) {
      return ((Number) value).longValue();
    }
    allowOnlyStrings(value);
    try {
      return Long.parseLong(value.toString());
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Returns the {@code float} value with the specified name.
   *
   * @param name the name of the {@code float}
   * @return the {@code float} value with the specified name
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public float getFloat(String name) throws JMSException {
    Object value = map.get(name);
    if (value == null) {
      throw new NullPointerException("Invalid null value");
    }
    if (value instanceof Float) {
      return (Float) value;
    }
    allowOnlyStrings(value);
    try {
      return Float.parseFloat(value.toString());
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Returns the {@code double} value with the specified name.
   *
   * @param name the name of the {@code double}
   * @return the {@code double} value with the specified name
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public double getDouble(String name) throws JMSException {
    Object value = map.get(name);
    if (value == null) {
      throw new NullPointerException("Invalid null value");
    }
    if ((value instanceof Double) || (value instanceof Float)) {
      return ((Number) value).doubleValue();
    }
    allowOnlyStrings(value);
    try {
      return Double.parseDouble(value.toString());
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  private void allowOnlyStrings(Object value) throws MessageFormatException {
    if (!(value instanceof String)) {
      throw new MessageFormatException("Invalid conversion");
    }
  }

  /**
   * Returns the {@code String} value with the specified name.
   *
   * @param name the name of the {@code String}
   * @return the {@code String} value with the specified name; if there is no item by this name, a
   *     null value is returned
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public String getString(String name) throws JMSException {
    Object value = map.get(name);
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return (String) value;
    }
    if ((value instanceof Number) || (value instanceof Boolean) || (value instanceof Character)) {
      return value.toString();
    }
    throw new MessageFormatException("Unsupported conversion");
  }

  /**
   * Returns the byte array value with the specified name.
   *
   * @param name the name of the byte array
   * @return a copy of the byte array value with the specified name; if there is no item by this
   *     name, a null value is returned.
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  @Override
  public byte[] getBytes(String name) throws JMSException {
    return Utils.invoke(() -> (byte[]) map.get(name));
  }

  /**
   * Returns the value of the object with the specified name.
   *
   * <p>This method can be used to return, in objectified format, an object in the Java programming
   * language ("Java object") that had been stored in the Map with the equivalent {@code setObject}
   * method call, or its equivalent primitive <code>set<I>type</I></code> method.
   *
   * <p>Note that byte values are returned as {@code byte[]}, not {@code Byte[]}.
   *
   * @param name the name of the Java object
   * @return a copy of the Java object value with the specified name, in objectified format (for
   *     example, if the object was set as an {@code int}, an {@code Integer} is returned); if there
   *     is no item by this name, a null value is returned
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   */
  @Override
  public Object getObject(String name) throws JMSException {
    return Utils.invoke(() -> map.get(name));
  }

  /**
   * Returns an {@code Enumeration} of all the names in the {@code MapMessage} object.
   *
   * @return an enumeration of all the names in this {@code MapMessage}
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   */
  @Override
  public Enumeration getMapNames() throws JMSException {
    return Collections.enumeration(map.keySet());
  }

  /**
   * Sets a {@code boolean} value with the specified name into the Map.
   *
   * @param name the name of the {@code boolean}
   * @param value the {@code boolean} value to set in the Map
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  @Override
  public void setBoolean(String name, boolean value) throws JMSException {
    checkWritableProperty(name);
    map.put(name, value);
  }

  /**
   * Sets a {@code byte} value with the specified name into the Map.
   *
   * @param name the name of the {@code byte}
   * @param value the {@code byte} value to set in the Map
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  @Override
  public void setByte(String name, byte value) throws JMSException {
    checkWritableProperty(name);
    map.put(name, value);
  }

  /**
   * Sets a {@code short} value with the specified name into the Map.
   *
   * @param name the name of the {@code short}
   * @param value the {@code short} value to set in the Map
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  @Override
  public void setShort(String name, short value) throws JMSException {
    checkWritableProperty(name);
    map.put(name, value);
  }

  /**
   * Sets a Unicode character value with the specified name into the Map.
   *
   * @param name the name of the Unicode character
   * @param value the Unicode character value to set in the Map
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  @Override
  public void setChar(String name, char value) throws JMSException {
    checkWritableProperty(name);
    map.put(name, value);
  }

  /**
   * Sets an {@code int} value with the specified name into the Map.
   *
   * @param name the name of the {@code int}
   * @param value the {@code int} value to set in the Map
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  @Override
  public void setInt(String name, int value) throws JMSException {
    checkWritableProperty(name);
    map.put(name, value);
  }

  /**
   * Sets a {@code long} value with the specified name into the Map.
   *
   * @param name the name of the {@code long}
   * @param value the {@code long} value to set in the Map
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  @Override
  public void setLong(String name, long value) throws JMSException {
    checkWritableProperty(name);
    map.put(name, value);
  }

  /**
   * Sets a {@code float} value with the specified name into the Map.
   *
   * @param name the name of the {@code float}
   * @param value the {@code float} value to set in the Map
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  @Override
  public void setFloat(String name, float value) throws JMSException {
    checkWritableProperty(name);
    map.put(name, value);
  }

  /**
   * Sets a {@code double} value with the specified name into the Map.
   *
   * @param name the name of the {@code double}
   * @param value the {@code double} value to set in the Map
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  @Override
  public void setDouble(String name, double value) throws JMSException {
    checkWritableProperty(name);
    map.put(name, value);
  }

  /**
   * Sets a {@code String} value with the specified name into the Map.
   *
   * @param name the name of the {@code String}
   * @param value the {@code String} value to set in the Map
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  @Override
  public void setString(String name, String value) throws JMSException {
    checkWritableProperty(name);
    map.put(name, value);
  }

  /**
   * Sets a byte array value with the specified name into the Map.
   *
   * @param name the name of the byte array
   * @param value the byte array value to set in the Map; the array is copied so that the value for
   *     {@code name} will not be altered by future modifications
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws IllegalArgumentException if the name is null, or if the name is an empty string.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  @Override
  public void setBytes(String name, byte[] value) throws JMSException {
    checkWritableProperty(name);
    map.put(name, value);
  }

  /**
   * Sets a portion of the byte array value with the specified name into the Map.
   *
   * @param name the name of the byte array
   * @param value the byte array value to set in the Map
   * @param offset the initial offset within the byte array
   * @param length the number of bytes to use
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  @Override
  public void setBytes(String name, byte[] value, int offset, int length) throws JMSException {
    checkWritableProperty(name);
    if (offset == 0 && length == value.length) {
      map.put(name, value);
    } else {
      byte[] copy = new byte[length];
      System.arraycopy(value, offset, copy, 0, length);
      map.put(name, copy);
    }
  }

  /**
   * Sets an object value with the specified name into the Map.
   *
   * <p>This method works only for the objectified primitive object types ({@code Integer}, {@code
   * Double}, {@code Long}&nbsp;...), {@code String} objects, and byte arrays.
   *
   * @param name the name of the Java object
   * @param value the Java object value to set in the Map
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws IllegalArgumentException if the name is null or if the name is an empty string.
   * @throws MessageFormatException if the object is invalid.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  @Override
  public void setObject(String name, Object value) throws JMSException {
    checkWritableProperty(name);
    validateWritableObject(value);
    map.put(name, value);
  }

  /**
   * Indicates whether an item exists in this {@code MapMessage} object.
   *
   * @param name the name of the item to test
   * @return true if the item exists
   * @throws JMSException if the JMS provider fails to determine if the item exists due to some
   *     internal error.
   */
  @Override
  public boolean itemExists(String name) throws JMSException {
    return map.containsKey(name);
  }
}
