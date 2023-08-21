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

import javax.jms.JMSException;
import org.junit.jupiter.api.Test;

public class PulsarMessagePropertyIdentifiersTest {

  /** valid identifiers */
  private static final String[] VALID_IDENTIFIERS = {
    "A",
    "_A",
    "$A",
    "a1",
    "_a1",
    "$a1",
    "a_a",
    "a$a",
    "\u0041\u0061\u0391\u0430\u0301",
    "JMSXGroupID",
    "Country.name"
  };

  /** Invalid identifiers */
  private static final String[] INVALID_IDENTIFIERS = {
    null, "1", "+1", "-1", "'1'", ".a.b", "\u0030",
  };

  private static final String[] RESERVED_IDENTIFIERS = {
    "NULL", "TRUE", "FALSE", "NOT", "AND", "OR", "BETWEEN", "LIKE", "IN", "IS", "ESCAPE"
  };

  /** Reserved identifiers */
  private static final String[] JMS_RESERVED_IDENTIFIERS = {
    "JMS_ConsumerTXID",
    "JMS_RcvTimestamp",
    "JMS__State",
    "JMSXUserID",
    "JMSXAppID",
    "JMSXDeliveryCount",
    "JMSXProducerTXID",
    "JMSXConsumerTXID",
    "JMSXRcvTimestamp",
    "JMSXState"
  };

  @Test
  public void validIdentifiersTest() throws JMSException {
    for (String id : VALID_IDENTIFIERS) {
      PulsarSimpleMessage message = new PulsarSimpleMessage();
      message.setObjectProperty(id, "true");
    }
  }

  @Test
  public void invalidIdentifiersTest() throws JMSException {
    for (String id : INVALID_IDENTIFIERS) {
      PulsarSimpleMessage message = new PulsarSimpleMessage();
      Throwable thrown =
          assertThrows(IllegalArgumentException.class, () -> message.setObjectProperty(id, "true"));
      assertEquals("Invalid map key " + id, thrown.getMessage());
    }
  }

  @Test
  public void reservedIdentifiersTest() {
    for (String id : RESERVED_IDENTIFIERS) {
      PulsarSimpleMessage message = new PulsarSimpleMessage();
      Throwable thrown =
          assertThrows(IllegalArgumentException.class, () -> message.setObjectProperty(id, "true"));
      assertEquals("Invalid map key " + id, thrown.getMessage());
    }
  }

  @Test
  public void jmsReservedIdentifiersTest() {
    for (String id : JMS_RESERVED_IDENTIFIERS) {
      PulsarSimpleMessage message = new PulsarSimpleMessage();
      Throwable thrown =
          assertThrows(IllegalArgumentException.class, () -> message.setObjectProperty(id, "true"));
      assertEquals("Invalid map key " + id, thrown.getMessage());
    }
  }
}
