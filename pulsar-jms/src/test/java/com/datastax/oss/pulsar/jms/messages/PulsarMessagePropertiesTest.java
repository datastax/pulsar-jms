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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.jms.JMSException;
import org.junit.jupiter.api.Test;

public class PulsarMessagePropertiesTest {

  Boolean[] BOOLEANS = {Boolean.TRUE, Boolean.FALSE};
  Byte[] BYTES = {Byte.MIN_VALUE, Byte.MAX_VALUE};
  Short[] SHORTS = {Short.MIN_VALUE, Short.MAX_VALUE};
  Integer[] INTS = {Integer.MIN_VALUE, Integer.MAX_VALUE};
  Long[] LONGS = {Long.MIN_VALUE, Long.MAX_VALUE};
  Float[] FLOATS = {Float.MIN_VALUE, Float.MAX_VALUE, Float.NaN};
  Double[] DOUBLES = {Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN};
  String[] STRINGS = {"a", "b", "aaa", "bbb", "A", "B"};
  Object[][] ALL_VALUES = {BOOLEANS, BYTES, SHORTS, INTS, LONGS, FLOATS, DOUBLES, STRINGS};

  @Test
  public final void testObjectPropertyTypes() throws JMSException {
    PulsarSimpleMessage message = new PulsarSimpleMessage();
    final String name = "test";

    for (int i = 0; i < ALL_VALUES.length; ++i) {
      Object[] values = ALL_VALUES[i];
      for (int j = 0; j < values.length; ++j) {
        Object value = values[j];
        message.setObjectProperty(name, value);

        // verify that the type is the same as that set
        assertEquals(value.getClass(), message.getObjectProperty(name).getClass());

        // test that set/get values are equal
        if (value instanceof Float && ((Float) value).isNaN()) {
          assertTrue(((Float) message.getObjectProperty(name)).isNaN());
        } else if (value instanceof Double && ((Double) value).isNaN()) {
          assertTrue(((Double) message.getObjectProperty(name)).isNaN());
        } else {
          assertEquals(value, message.getObjectProperty(name));
        }
      }
    }
  }
}
