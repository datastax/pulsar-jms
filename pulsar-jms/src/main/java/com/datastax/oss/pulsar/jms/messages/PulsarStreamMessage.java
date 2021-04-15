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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import javax.jms.StreamMessage;
import org.apache.pulsar.client.api.TypedMessageBuilder;

public final class PulsarStreamMessage extends PulsarMessage implements StreamMessage {

  @Override
  protected String messageType() {
    return "stream";
  }

  protected void checkType(byte type, byte expected) throws JMSException {
    if (type != expected) {
      throw new MessageFormatException(
          "Invalid type " + typeToString(type) + ", expected " + typeToString(expected));
    }
  }

  @Override
  public <T> T getBody(Class<T> c) throws JMSException {
    throw new MessageFormatException("getBody not available on StreamMessage");
  }

  protected void writeDataType(byte dataType) throws IOException {
    dataOutputStream.writeByte(dataType);
  }

  protected void writeArrayLen(int len) throws IOException {
    dataOutputStream.writeInt(len);
  }

  protected int readArrayLen() throws IOException {
    return dataInputStream.readInt();
  }

  protected byte readDataType() throws IOException {
    return dataInputStream.readByte();
  }

  /**
   * Reads a byte array field from the stream message into the specified {@code byte[]} object (the
   * read buffer).
   *
   * <p>To read the field value, {@code readBytes} should be successively called until it returns a
   * value less than the length of the read buffer. The value of the bytes in the buffer following
   * the last byte read is undefined.
   *
   * <p>If {@code readBytes} returns a value equal to the length of the buffer, a subsequent {@code
   * readBytes} call must be made. If there are no more bytes to be read, this call returns -1.
   *
   * <p>If the byte array field value is null, {@code readBytes} returns -1.
   *
   * <p>If the byte array field value is empty, {@code readBytes} returns 0.
   *
   * <p>Once the first {@code readBytes} call on a {@code byte[]} field value has been made, the
   * full value of the field must be read before it is valid to read the next field. An attempt to
   * read the next field before that has been done will throw a {@code MessageFormatException}.
   *
   * <p>To read the byte field value into a new {@code byte[]} object, use the {@code readObject}
   * method.
   *
   * @param value the buffer into which the data is read
   * @return the total number of bytes read into the buffer, or -1 if there is no more data because
   *     the end of the byte field has been reached
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageEOFException if unexpected end of message stream has been reached.
   * @throws MessageFormatException if this type conversion is invalid.
   * @throws MessageNotReadableException if the message is in write-only mode.
   * @see #readObject()
   */
  public int readBytes(byte[] value) throws JMSException {
    if (value == null) {
      return -1;
    }
    if (value.length == 0) {
      return 0;
    }
    return readBytes(value, value.length);
  }

  private void resetStreamAtMark() throws JMSException {
    try {
      dataInputStream.reset();
    } catch (IOException err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Reads an object from the stream message.
   *
   * <p>This method can be used to return, in objectified format, an object in the Java programming
   * language ("Java object") that has been written to the stream with the equivalent {@code
   * writeObject} method call, or its equivalent primitive <code>write<I>type</I></code> method.
   *
   * <p>Note that byte values are returned as {@code byte[]}, not {@code Byte[]}.
   *
   * <p>An attempt to call {@code readObject} to read a byte field value into a new {@code byte[]}
   * object before the full value of the byte field has been read will throw a {@code
   * MessageFormatException}.
   *
   * @return a Java object from the stream message, in objectified format (for example, if the
   *     object was written as an {@code int}, an {@code Integer} is returned)
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageEOFException if unexpected end of message stream has been reached.
   * @throws MessageFormatException if this type conversion is invalid.
   * @throws MessageNotReadableException if the message is in write-only mode.
   * @see #readBytes(byte[] value)
   */
  @Override
  public Object readObject() throws JMSException {
    checkReadable();
    if (remainingByteArrayLen != Integer.MIN_VALUE) {
      throw new MessageFormatException("You must complete the readBytes operation");
    }
    try {
      dataInputStream.mark(100);
      byte dataType = readDataType();
      switch (dataType) {
        case TYPE_NULL:
          return null;
        case TYPE_BOOLEAN:
          return dataInputStream.readBoolean();
        case TYPE_DOUBLE:
          return dataInputStream.readDouble();
        case TYPE_FLOAT:
          return dataInputStream.readFloat();
        case TYPE_INT:
          return dataInputStream.readInt();
        case TYPE_LONG:
          return dataInputStream.readLong();
        case TYPE_SHORT:
          return dataInputStream.readShort();
        case TYPE_STRING:
          return dataInputStream.readUTF();
        case TYPE_BYTE:
          return dataInputStream.readByte();
        case TYPE_CHAR:
          return dataInputStream.readChar();
        case TYPE_BYTES:
          int len = readArrayLen();
          byte[] buffer = new byte[len];
          dataInputStream.read(buffer);
          return buffer;
        default:
          throw new MessageFormatException("Wrong data type: " + dataType);
      }
    } catch (Exception err) {
      resetStreamAtMark();
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  protected ByteArrayOutputStream stream;
  protected byte[] originalMessage;
  protected DataInputStream dataInputStream;
  protected DataOutputStream dataOutputStream;
  // support for readBytes
  protected int remainingByteArrayLen = Integer.MIN_VALUE;

  protected static final byte TYPE_BOOLEAN = 1;
  protected static final byte TYPE_STRING = 2;
  protected static final byte TYPE_INT = 3;
  protected static final byte TYPE_SHORT = 4;
  protected static final byte TYPE_LONG = 5;
  protected static final byte TYPE_FLOAT = 6;
  protected static final byte TYPE_DOUBLE = 7;
  protected static final byte TYPE_BYTE = 8;
  protected static final byte TYPE_CHAR = 9;
  protected static final byte TYPE_BYTES = 10;
  protected static final byte TYPE_NULL = 11;

  private static String typeToString(byte type) {
    switch (type) {
      case TYPE_BOOLEAN:
        return "boolean";
      case TYPE_STRING:
        return "string";
      case TYPE_INT:
        return "int";
      case TYPE_SHORT:
        return "short";
      case TYPE_LONG:
        return "long";
      case TYPE_FLOAT:
        return "float";
      case TYPE_DOUBLE:
        return "double";
      case TYPE_BYTE:
        return "byte";
      case TYPE_CHAR:
        return "char";
      case TYPE_BYTES:
        return "bytes";
      case TYPE_NULL:
        return "null";
      default:
        return "?" + type;
    }
  }

  public PulsarStreamMessage(byte[] payload) throws JMSException {
    try {
      this.dataInputStream = new DataInputStream(new ByteArrayInputStream(payload));
      this.originalMessage = payload;
      this.stream = null;
      this.dataOutputStream = null;
      writable = false;
    } catch (Exception err) {
      throw Utils.handleException(err);
    }
  }

  public PulsarStreamMessage() {
    try {
      this.dataInputStream = null;
      this.stream = new ByteArrayOutputStream();
      this.dataOutputStream = new DataOutputStream(stream);
      this.originalMessage = null;
      this.writable = true;
    } catch (Exception err) {
      throw new RuntimeException(err);
    }
  }

  /**
   * Returns whether the message body is capable of being assigned to the specified type. If this
   * method returns true then a subsequent call to the method {@code getBody} on the same message
   * with the same type argument would not throw a MessageFormatException.
   *
   * <p>If the message is a {@code StreamMessage} then false is always returned. If the message is a
   * {@code ObjectMessage} and object deserialization fails then false is returned. If the message
   * has no body then any type may be specified and true is returned.
   *
   * @param c The specified type <br>
   *     If the message is a {@code TextMessage} then this method will only return true if this
   *     parameter is set to {@code String.class} or another type to which a {@code String} is
   *     assignable. <br>
   *     If the message is a {@code ObjectMessage} then this method will only return true if this
   *     parameter is set to {@code java.io.Serializable.class} or another class to which the body
   *     is assignable. <br>
   *     If the message is a {@code MapMessage} then this method will only return true if this
   *     parameter is set to {@code java.util.Map.class} (or {@code java.lang.Object.class}). <br>
   *     If the message is a {@code BytesMessage} then this this method will only return true if
   *     this parameter is set to {@code byte[].class} (or {@code java.lang.Object.class}). <br>
   *     If the message is a {@code TextMessage}, {@code ObjectMessage}, {@code MapMessage} or
   *     {@code BytesMessage} and the message has no body, then the above does not apply and this
   *     method will return true irrespective of the value of this parameter.<br>
   *     If the message is a {@code Message} (but not one of its subtypes) then this method will
   *     return true irrespective of the value of this parameter.
   * @return whether the message body is capable of being assigned to the specified type
   * @throws JMSException if the JMS provider fails to return a value due to some internal error.
   */
  @Override
  public boolean isBodyAssignableTo(Class c) throws JMSException {
    return byte[].class == c;
  }

  @Override
  protected void prepareForSend(TypedMessageBuilder<byte[]> producer) throws JMSException {
    try {
      if (stream != null) {
        // write mode
        dataOutputStream.flush();
        dataOutputStream.close();
        producer.value(stream.toByteArray());
      } else {
        // read mode
        producer.value(originalMessage);
      }
    } catch (Exception err) {
      throw Utils.handleException(err);
    }
  }

  /**
   * Reads a {@code boolean} from the stream message.
   *
   * @return the {@code boolean} value read
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageEOFException if unexpected end of message stream has been reached.
   * @throws MessageFormatException if this type conversion is invalid.
   * @throws MessageNotReadableException if the message is in write-only mode.
   */
  public boolean readBoolean() throws JMSException {
    checkReadable();
    try {
      byte dataType = readDataType();
      switch (dataType) {
        case TYPE_BOOLEAN:
          return dataInputStream.readBoolean();
        case TYPE_STRING:
          return Boolean.parseBoolean(dataInputStream.readUTF());
        default:
          // fail
          checkType(dataType, TYPE_BOOLEAN);
          return false;
      }
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Reads a {@code byte} value from the stream message.
   *
   * @return the next byte from the stream message as a 8-bit {@code byte}
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageEOFException if unexpected end of message stream has been reached.
   * @throws MessageFormatException if this type conversion is invalid.
   * @throws MessageNotReadableException if the message is in write-only mode.
   */
  public byte readByte() throws JMSException {
    checkReadable();
    try {
      dataInputStream.mark(2);
      byte dataType = readDataType();
      switch (dataType) {
        case TYPE_STRING:
          return Byte.parseByte(dataInputStream.readUTF());
        case TYPE_BYTE:
          return dataInputStream.readByte();
        default:
          // failing
          checkType(dataType, TYPE_BYTE);
          return 0;
      }
    } catch (Exception err) {
      resetStreamAtMark();
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Reads a 16-bit integer from the stream message.
   *
   * @return a 16-bit integer from the stream message
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageEOFException if unexpected end of message stream has been reached.
   * @throws MessageFormatException if this type conversion is invalid.
   * @throws MessageNotReadableException if the message is in write-only mode.
   */
  public short readShort() throws JMSException {
    checkReadable();
    try {
      dataInputStream.mark(5);
      byte dataType = readDataType();
      switch (dataType) {
        case TYPE_SHORT:
          return dataInputStream.readShort();
        case TYPE_STRING:
          return Short.parseShort(dataInputStream.readUTF());
        case TYPE_BYTE:
          return dataInputStream.readByte();
        default:
          // failing
          checkType(dataType, TYPE_SHORT);
          return 0;
      }
    } catch (Exception err) {
      resetStreamAtMark();
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Reads a Unicode character value from the stream message.
   *
   * @return a Unicode character from the stream message
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageEOFException if unexpected end of message stream has been reached.
   * @throws MessageFormatException if this type conversion is invalid
   * @throws MessageNotReadableException if the message is in write-only mode.
   */
  public char readChar() throws JMSException {
    checkReadable();
    try {
      dataInputStream.mark(2);
      byte dataType = readDataType();
      switch (dataType) {
        case TYPE_CHAR:
          return dataInputStream.readChar();
        case TYPE_NULL:
          throw new NullPointerException("invalid conversion");
        default:
          // failing
          checkType(dataType, TYPE_CHAR);
          return 0;
      }
    } catch (NullPointerException err) {
      throw err;
    } catch (Exception err) {
      resetStreamAtMark();
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Reads a 32-bit integer from the stream message.
   *
   * @return a 32-bit integer value from the stream message, interpreted as an {@code int}
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageEOFException if unexpected end of message stream has been reached.
   * @throws MessageFormatException if this type conversion is invalid.
   * @throws MessageNotReadableException if the message is in write-only mode.
   */
  public int readInt() throws JMSException {
    checkReadable();
    try {
      byte dataType = readDataType();
      switch (dataType) {
        case TYPE_INT:
          return dataInputStream.readInt();
        case TYPE_SHORT:
          return dataInputStream.readShort();
        case TYPE_STRING:
          return Integer.parseInt(dataInputStream.readUTF());
        case TYPE_BYTE:
          return dataInputStream.readByte();
        default:
          // failing
          checkType(dataType, TYPE_INT);
          return 0;
      }
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Reads a 64-bit integer from the stream message.
   *
   * @return a 64-bit integer value from the stream message, interpreted as a {@code long}
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageEOFException if unexpected end of message stream has been reached.
   * @throws MessageFormatException if this type conversion is invalid.
   * @throws MessageNotReadableException if the message is in write-only mode.
   */
  public long readLong() throws JMSException {
    checkReadable();
    try {
      dataInputStream.mark(9);
      byte dataType = readDataType();
      switch (dataType) {
        case TYPE_INT:
          return dataInputStream.readInt();
        case TYPE_LONG:
          return dataInputStream.readLong();
        case TYPE_SHORT:
          return dataInputStream.readShort();
        case TYPE_STRING:
          return Integer.parseInt(dataInputStream.readUTF());
        case TYPE_BYTE:
          return dataInputStream.readByte();
        default:
          // failing
          checkType(dataType, TYPE_LONG);
          return 0;
      }
    } catch (Exception err) {
      resetStreamAtMark();
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Reads a {@code float} from the stream message.
   *
   * @return a {@code float} value from the stream message
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageEOFException if unexpected end of message stream has been reached.
   * @throws MessageFormatException if this type conversion is invalid.
   * @throws MessageNotReadableException if the message is in write-only mode.
   */
  public float readFloat() throws JMSException {
    checkReadable();
    try {
      dataInputStream.mark(9);
      byte dataType = readDataType();
      switch (dataType) {
        case TYPE_FLOAT:
          return dataInputStream.readFloat();
        case TYPE_STRING:
          return Float.parseFloat(dataInputStream.readUTF());
        default:
          // failing
          checkType(dataType, TYPE_FLOAT);
          return 0;
      }
    } catch (Exception err) {
      resetStreamAtMark();
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Reads a {@code double} from the stream message.
   *
   * @return a {@code double} value from the stream message
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageEOFException if unexpected end of message stream has been reached.
   * @throws MessageFormatException if this type conversion is invalid.
   * @throws MessageNotReadableException if the message is in write-only mode.
   */
  public double readDouble() throws JMSException {
    checkReadable();
    try {
      dataInputStream.mark(9);
      byte dataType = readDataType();
      switch (dataType) {
        case TYPE_FLOAT:
          return dataInputStream.readFloat();
        case TYPE_DOUBLE:
          return dataInputStream.readDouble();
        case TYPE_STRING:
          return Double.parseDouble(dataInputStream.readUTF());
        default:
          // failing
          checkType(dataType, TYPE_DOUBLE);
          return 0;
      }
    } catch (Exception err) {
      resetStreamAtMark();
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Reads a {@code String} from the stream message.
   *
   * @return a Unicode string from the stream message
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageEOFException if unexpected end of message stream has been reached.
   * @throws MessageFormatException if this type conversion is invalid.
   * @throws MessageNotReadableException if the message is in write-only mode.
   */
  public String readString() throws JMSException {
    checkReadable();
    try {
      dataInputStream.mark(100);
      byte dataType = readDataType();
      switch (dataType) {
        case TYPE_NULL:
          return null;
        case TYPE_STRING:
          return dataInputStream.readUTF();
        case TYPE_BOOLEAN:
          return Boolean.toString(dataInputStream.readBoolean());
        case TYPE_CHAR:
          return Character.toString(dataInputStream.readChar());
        case TYPE_BYTE:
          return Byte.toString(dataInputStream.readByte());
        case TYPE_SHORT:
          return Short.toString(dataInputStream.readShort());
        case TYPE_LONG:
          return Long.toString(dataInputStream.readLong());
        case TYPE_INT:
          return Integer.toString(dataInputStream.readInt());
        case TYPE_FLOAT:
          return Float.toString(dataInputStream.readFloat());
        case TYPE_DOUBLE:
          return Double.toString(dataInputStream.readDouble());
        default:
          throw new MessageFormatException(
              "Cannot read a string out of a " + typeToString(dataType));
      }

    } catch (Exception err) {
      resetStreamAtMark();
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Writes a {@code boolean} to the stream message. The value {@code true} is written as the value
   * {@code (byte)1}; the value {@code false} is written as the value {@code (byte)0}.
   *
   * @param value the {@code boolean} value to be written
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  public void writeBoolean(boolean value) throws JMSException {
    checkWritable();
    try {
      writeDataType(TYPE_BOOLEAN);
      dataOutputStream.writeBoolean(value);
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Writes a {@code byte} to the stream message.
   *
   * @param value the {@code byte} value to be written
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  public void writeByte(byte value) throws JMSException {
    checkWritable();
    try {
      writeDataType(TYPE_BYTE);
      dataOutputStream.writeByte(value);
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Writes a {@code short} to the stream message.
   *
   * @param value the {@code short} value to be written
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  public void writeShort(short value) throws JMSException {
    checkWritable();
    try {
      writeDataType(TYPE_SHORT);
      dataOutputStream.writeShort(value);
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Writes a {@code char} to the stream message.
   *
   * @param value the {@code char} value to be written
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  public void writeChar(char value) throws JMSException {
    checkWritable();
    try {
      writeDataType(TYPE_CHAR);
      dataOutputStream.writeChar(value);
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Writes an {@code int} to the stream message.
   *
   * @param value the {@code int} value to be written
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  public void writeInt(int value) throws JMSException {
    checkWritable();
    try {
      writeDataType(TYPE_INT);
      dataOutputStream.writeInt(value);
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Writes a {@code long} to the stream message.
   *
   * @param value the {@code long} value to be written
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  public void writeLong(long value) throws JMSException {
    checkWritable();
    try {
      writeDataType(TYPE_LONG);
      dataOutputStream.writeLong(value);
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Writes a {@code float} to the stream message.
   *
   * @param value the {@code float} value to be written
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  public void writeFloat(float value) throws JMSException {
    checkWritable();
    try {
      writeDataType(TYPE_FLOAT);
      dataOutputStream.writeFloat(value);
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Writes a {@code double} to the stream message.
   *
   * @param value the {@code double} value to be written
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  public void writeDouble(double value) throws JMSException {
    checkWritable();
    try {
      writeDataType(TYPE_DOUBLE);
      dataOutputStream.writeDouble(value);
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Writes a {@code String} to the stream message.
   *
   * @param value the {@code String} value to be written
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  public void writeString(String value) throws JMSException {
    checkWritable();
    try {
      if (value == null) {
        writeDataType(TYPE_NULL);
        return;
      }
      writeDataType(TYPE_STRING);
      dataOutputStream.writeUTF(value);
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Writes a byte array field to the stream message.
   *
   * <p>The byte array {@code value} is written to the message as a byte array field. Consecutively
   * written byte array fields are treated as two distinct fields when the fields are read.
   *
   * @param value the byte array value to be written
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  public void writeBytes(byte[] value) throws JMSException {
    writeBytes(value, 0, value.length);
  }

  /**
   * Writes a portion of a byte array as a byte array field to the stream message.
   *
   * <p>The a portion of the byte array {@code value} is written to the message as a byte array
   * field. Consecutively written byte array fields are treated as two distinct fields when the
   * fields are read.
   *
   * @param value the byte array value to be written
   * @param offset the initial offset within the byte array
   * @param length the number of bytes to use
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  public void writeBytes(byte[] value, int offset, int length) throws JMSException {
    checkWritable();
    if (value == null) {
      throw new NullPointerException();
    }
    try {
      writeDataType(TYPE_BYTES);
      writeArrayLen(length);
      dataOutputStream.write(value, offset, length);
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Writes an object to the stream message.
   *
   * <p>This method works only for the objectified primitive object types ({@code Integer}, {@code
   * Double}, {@code Long}&nbsp;...), {@code String} objects, and byte arrays.
   *
   * @param value the Java object to be written
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws MessageFormatException if the object is invalid.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  public void writeObject(Object value) throws JMSException {
    checkWritable();
    try {
      if (value == null) {
        writeDataType(TYPE_NULL);
        return;
      }
      // see also validateWritableObject
      if (value instanceof Integer) {
        writeInt((Integer) value);
      } else if (value instanceof String) {
        writeUTF((String) value);
      } else if (value instanceof Short) {
        writeShort((Short) value);
      } else if (value instanceof Long) {
        writeLong((Long) value);
      } else if (value instanceof Double) {
        writeDouble((Double) value);
      } else if (value instanceof Float) {
        writeFloat((Float) value);
      } else if (value instanceof Byte) {
        writeByte((Byte) value);
      } else if (value instanceof Character) {
        writeChar((Character) value);
      } else if (value instanceof Boolean) {
        writeBoolean((Boolean) value);
      } else if (value instanceof byte[]) {
        writeBytes((byte[]) value);
      } else {
        throw new MessageFormatException("Unsupported type " + value.getClass());
      }
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Clears out the message body. Clearing a message's body does not clear its header values or
   * property entries.
   *
   * <p>If this message body was read-only, calling this method leaves the message body in the same
   * state as an empty body in a newly created message.
   *
   * @throws JMSException if the JMS provider fails to clear the message body due to some internal
   *     error.
   */
  @Override
  public void clearBody() throws JMSException {
    remainingByteArrayLen = Integer.MIN_VALUE;
    this.writable = true;
    try {
      if (stream != null) {
        this.dataInputStream = new DataInputStream(new ByteArrayInputStream(stream.toByteArray()));
        this.stream = null;
        this.dataOutputStream = null;
      } else {
        this.stream = new ByteArrayOutputStream();
        this.dataOutputStream = new DataOutputStream(stream);
        this.originalMessage = null;
        this.dataInputStream = null;
      }
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Puts the message body in read-only mode and repositions the stream to the beginning.
   *
   * @throws JMSException if the JMS provider fails to reset the message due to some internal error.
   * @throws MessageFormatException if the message has an invalid format.
   */
  public void reset() throws JMSException {
    remainingByteArrayLen = Integer.MIN_VALUE;
    this.writable = false;
    try {
      if (stream != null) {
        this.dataOutputStream.flush();
        this.originalMessage = stream.toByteArray();
        this.dataInputStream = new DataInputStream(new ByteArrayInputStream(originalMessage));
        this.stream = null;
        this.dataOutputStream = null;
      } else {
        this.dataInputStream = new DataInputStream(new ByteArrayInputStream(originalMessage));
      }
    } catch (Exception err) {
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Gets the number of bytes of the message body when the message is in read-only mode. The value
   * returned can be used to allocate a byte array. The value returned is the entire length of the
   * message body, regardless of where the pointer for reading the message is currently located.
   *
   * @return number of bytes in the message
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageNotReadableException if the message is in write-only mode.
   * @since JMS 1.1
   */
  public long getBodyLength() throws JMSException {
    checkReadable();
    return originalMessage.length;
  }

  /**
   * Reads an unsigned 8-bit number from the bytes message stream.
   *
   * @return the next byte from the bytes message stream, interpreted as an unsigned 8-bit number
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageEOFException if unexpected end of bytes stream has been reached.
   * @throws MessageNotReadableException if the message is in write-only mode.
   */
  public int readUnsignedByte() throws JMSException {
    checkReadable();
    try {
      dataInputStream.mark(2);
      checkType(readDataType(), TYPE_BYTE);
      return dataInputStream.readUnsignedByte();
    } catch (Exception err) {
      resetStreamAtMark();
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Reads an unsigned 16-bit number from the bytes message stream.
   *
   * @return the next two bytes from the bytes message stream, interpreted as an unsigned 16-bit
   *     integer
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageEOFException if unexpected end of bytes stream has been reached.
   * @throws MessageNotReadableException if the message is in write-only mode.
   */
  public int readUnsignedShort() throws JMSException {
    checkReadable();
    try {
      dataInputStream.mark(5);
      checkType(readDataType(), TYPE_SHORT);
      return dataInputStream.readUnsignedShort();
    } catch (Exception err) {
      resetStreamAtMark();
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Reads a string that has been encoded using a modified UTF-8 format from the bytes message
   * stream.
   *
   * <p>For more information on the UTF-8 format, see "File System Safe UCS Transformation Format
   * (FSS_UTF)", X/Open Preliminary Specification, X/Open Company Ltd., Document Number: P316. This
   * information also appears in ISO/IEC 10646, Annex P.
   *
   * @return a Unicode string from the bytes message stream
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageEOFException if unexpected end of bytes stream has been reached.
   * @throws MessageNotReadableException if the message is in write-only mode.
   */
  public String readUTF() throws JMSException {
    return readString();
  }

  /**
   * Reads a portion of the bytes message stream.
   *
   * <p>If the length of array {@code value} is less than the number of bytes remaining to be read
   * from the stream, the array should be filled. A subsequent call reads the next increment, and so
   * on.
   *
   * <p>If the number of bytes remaining in the stream is less than the length of array {@code
   * value}, the bytes should be read into the array. The return value of the total number of bytes
   * read will be less than the length of the array, indicating that there are no more bytes left to
   * be read from the stream. The next read of the stream returns -1.
   *
   * <p>If {@code length} is negative, or {@code length} is greater than the length of the array
   * {@code value}, then an {@code IndexOutOfBoundsException} is thrown. No bytes will be read from
   * the stream for this exception case.
   *
   * @param value the buffer into which the data is read
   * @param length the number of bytes to read; must be less than or equal to {@code value.length}
   * @return the total number of bytes read into the buffer, or -1 if there is no more data because
   *     the end of the stream has been reached
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageNotReadableException if the message is in write-only mode.
   */
  public int readBytes(byte[] value, int length) throws JMSException {
    if (length < 0 || length > value.length) {
      throw new IndexOutOfBoundsException();
    }
    checkReadable();
    try {
      if (remainingByteArrayLen == Integer.MIN_VALUE) { // start of field
        dataInputStream.mark(length);
        checkType(readDataType(), TYPE_BYTES);
        remainingByteArrayLen = readArrayLen();
        if (value == null) {
          return -1;
        }
        int read = dataInputStream.read(value, 0, length);
        remainingByteArrayLen = remainingByteArrayLen - read;
        return read;
      } else if (remainingByteArrayLen > 0) {
        if (value == null) {
          return -1;
        }
        int read = dataInputStream.read(value, 0, length);
        remainingByteArrayLen = remainingByteArrayLen - read;
        return read;
      } else {
        // end of field
        remainingByteArrayLen = Integer.MIN_VALUE;
        return -1;
      }
    } catch (EOFException err) {
      return -1;
    } catch (Exception err) {
      resetStreamAtMark();
      throw handleExceptionAccordingToMessageSpecs(err);
    }
  }

  /**
   * Writes a string to the bytes message stream using UTF-8 encoding in a machine-independent
   * manner.
   *
   * <p>For more information on the UTF-8 format, see "File System Safe UCS Transformation Format
   * (FSS_UTF)", X/Open Preliminary Specification, X/Open Company Ltd., Document Number: P316. This
   * information also appears in ISO/IEC 10646, Annex P.
   *
   * @param value the {@code String} value to be written
   * @throws JMSException if the JMS provider fails to write the message due to some internal error.
   * @throws MessageNotWriteableException if the message is in read-only mode.
   */
  public void writeUTF(String value) throws JMSException {
    writeString(value);
  }
}
