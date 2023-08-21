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
/*
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

import static org.junit.jupiter.api.Assertions.*;

import javax.jms.*;
import org.junit.jupiter.api.Test;

public class PulsarStreamMessageTest {

  @Test
  public final void nullBooleanTest() throws JMSException {
    PulsarStreamMessage msg = new PulsarStreamMessage();
    msg.writeObject(null);
    msg.reset();
    Boolean result = msg.readBoolean();
    assertEquals(Boolean.FALSE, result);
  }

  @Test
  public final void readNullAsStringTest() throws JMSException {
    PulsarStreamMessage msg = new PulsarStreamMessage();
    msg.writeObject(null);
    msg.reset();
    String result = msg.readString();
    assertNull(result);
  }

  /* When the message is first created, the body of the message is in write-only mode.
   */
  @Test
  public final void writeOnlyModeAfterCreateTest() {
    PulsarStreamMessage msg = new PulsarStreamMessage();
    Throwable thrown = assertThrows(MessageNotReadableException.class, () -> msg.readString());

    assertEquals("not readable", thrown.getMessage());
  }

  @Test
  public final void writeOnlyModeAfterClearBodyTest() throws JMSException {
    PulsarStreamMessage msg = new PulsarStreamMessage();
    msg.writeString("Hello");
    msg.clearBody();
    Throwable thrown = assertThrows(MessageNotReadableException.class, () -> msg.readString());

    assertEquals("not readable", thrown.getMessage());
  }

  /*
   * After the first call to reset has been made, the message body is in read-only mode.
   */
  @Test
  public final void readOnlyModeAfterResetTest() throws JMSException {
    PulsarStreamMessage msg = new PulsarStreamMessage();
    msg.reset();
    Throwable thrown =
        assertThrows(MessageNotWriteableException.class, () -> msg.writeString("Hello"));
    assertEquals("not writable", thrown.getMessage());
  }

  @Test
  public final void messageFormatExceptionTest() throws JMSException {
    PulsarStreamMessage msg = new PulsarStreamMessage();
    msg.writeDouble(Double.valueOf(12.34d));
    msg.reset();
    Throwable thrown = assertThrows(MessageFormatException.class, () -> msg.readInt());
    assertEquals("Invalid type double, expected int", thrown.getMessage());
  }

  @Test
  public final void noMoreBytesToReadTest() throws JMSException {
    PulsarStreamMessage msg = new PulsarStreamMessage();
    msg.writeBytes("Hello".getBytes());
    msg.reset();
    int result = msg.readBytes(new byte[10]);
    assertEquals(5, result);

    result = msg.readBytes(new byte[10]);
    assertEquals(-1, result);
  }
}
