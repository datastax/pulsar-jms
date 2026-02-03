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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

public final class PulsarBytesMessage extends PulsarMessage implements BytesMessage {

  /**
   * Used by JMSProducer
   *
   * @param payload
   * @return
   * @throws JMSException
   */
  public PulsarBytesMessage fill(byte[] payload) throws JMSException {
    if (payload != null) {
      this.writeBytes(payload);
    }
    return this;
  }

  protected ByteArrayOutputStream stream;
  protected byte[] originalMessage;
  protected DataInputStream dataInputStream;
  protected DataOutputStream dataOutputStream;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public PulsarBytesMessage(byte[] payload) throws JMSException {
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

  public PulsarBytesMessage() {
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
      return dataInputStream.readBoolean();
    } catch (Exception err) {
      throw handleException(err);
    }
  }

  protected static JMSException handleException(Throwable t) throws JMSException {
    if (t instanceof EOFException) {
      throw new MessageEOFException(t + "");
    }
    throw Utils.handleException(t);
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
      return dataInputStream.readByte();
    } catch (Exception err) {
      throw handleException(err);
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
      return dataInputStream.readShort();
    } catch (Exception err) {
      throw handleException(err);
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
      return dataInputStream.readChar();
    } catch (Exception err) {
      throw handleException(err);
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
      return dataInputStream.readInt();
    } catch (Exception err) {
      throw handleException(err);
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
      return dataInputStream.readLong();
    } catch (Exception err) {
      throw handleException(err);
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
      return dataInputStream.readFloat();
    } catch (Exception err) {
      throw handleException(err);
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
      return dataInputStream.readDouble();
    } catch (Exception err) {
      throw handleException(err);
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
      return dataInputStream.readUTF();
    } catch (Exception err) {
      throw handleException(err);
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
      dataOutputStream.writeBoolean(value);
    } catch (Exception err) {
      throw handleException(err);
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
      dataOutputStream.writeByte(value);
    } catch (Exception err) {
      throw handleException(err);
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
      dataOutputStream.writeShort(value);
    } catch (Exception err) {
      throw handleException(err);
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
      dataOutputStream.writeChar(value);
    } catch (Exception err) {
      throw handleException(err);
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
      dataOutputStream.writeInt(value);
    } catch (Exception err) {
      throw handleException(err);
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
      dataOutputStream.writeLong(value);
    } catch (Exception err) {
      throw handleException(err);
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
      dataOutputStream.writeFloat(value);
    } catch (Exception err) {
      throw handleException(err);
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
      dataOutputStream.writeDouble(value);
    } catch (Exception err) {
      throw handleException(err);
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
      dataOutputStream.writeUTF(value);
    } catch (Exception err) {
      throw handleException(err);
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
    checkWritable();
    if (value == null) {
      throw new NullPointerException();
    }
    try {
      dataOutputStream.write(value);
    } catch (Exception err) {
      throw handleException(err);
    }
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
      dataOutputStream.write(value, offset, length);
    } catch (Exception err) {
      throw handleException(err);
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
    if (value == null) {
      throw new NullPointerException("null not allowed here");
    }
    try {
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
      throw handleException(err);
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
      throw handleException(err);
    }
  }

  /**
   * Puts the message body in read-only mode and repositions the stream to the beginning.
   *
   * @throws JMSException if the JMS provider fails to reset the message due to some internal error.
   * @throws MessageFormatException if the message has an invalid format.
   */
  public void reset() throws JMSException {
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
      throw handleException(err);
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
      return dataInputStream.readUnsignedByte();
    } catch (Exception err) {
      throw handleException(err);
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
      return dataInputStream.readUnsignedShort();
    } catch (Exception err) {
      throw handleException(err);
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
    checkReadable();
    if (value == null) {
      return -1;
    }
    try {
      return dataInputStream.read(value, 0, length);
    } catch (Exception err) {
      throw handleException(err);
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

  @Override
  protected String messageType() {
    return "bytes";
  }

  @Override
  public <T> T getBody(Class<T> c) throws JMSException {
    if (c != byte[].class) {
      throw new MessageFormatException("only class byte[]");
    }
    reset();
    try {
      if (originalMessage != null) {
        return (T) originalMessage;
      }
      return (T) stream.toByteArray();
    } finally {
      reset();
    }
  }

  /**
   * Reads a byte array from the bytes message stream.
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
   * @param value the buffer into which the data is read
   * @return the total number of bytes read into the buffer, or -1 if there is no more data because
   *     the end of the stream has been reached
   * @throws JMSException if the JMS provider fails to read the message due to some internal error.
   * @throws MessageNotReadableException if the message is in write-only mode.
   */
  @Override
  public int readBytes(byte[] value) throws JMSException {
    checkReadable();
    if (value == null) {
      return -1;
    }
    try {
      return dataInputStream.read(value);
    } catch (Exception err) {
      throw handleException(err);
    }
  }
}
