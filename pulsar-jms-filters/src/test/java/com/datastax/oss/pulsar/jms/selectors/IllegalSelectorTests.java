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
package com.datastax.oss.pulsar.jms.selectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import org.junit.jupiter.api.Test;

public class IllegalSelectorTests {

  private static final String[] ILLEGAL_AND_SELECTORS = {"and", "true and", "and true"};

  private static final String[] ILLEGAL_BETWEEN_SELECTORS = {
    "two between '1' and '3'",
    "one between false and true",
    "'b' between 'a' and 'c'",
    "between 1 and 3",
    "not between 1 and 3",
    "2 between 1, 3",
    "2 between and 3"
  }; // "JMSMessageID between 1 and 10"

  private static final String[] ILLEGAL_BOOLEAN_SELECTORS = {
    "false < true", "false > true", "false <= true", "false >= true"
  };

  private static final String[] ILLEGAL_FLOAT_SELECTORS = {
    "1.0", "-1.0", "2.0 < '3.0'", "1a.0 = 1a.0"
  }; // "1.0 <> false"

  private static final String[] ILLEGAL_IN_SELECTORS = {
    "age in (21, 22, 23)",
    "Country in 'France', 'UK'",
    "Country in ('France', 'UK'",
    "in Country ('France', 'UK')",
    "Country in ('France' 'UK')",
    "Country in (France)",
    "Country in ()"
  };

  private static final String[] ILLEGAL_JMS_PRIORITY_SELECTORS = {
    "JMSPriority >= '0'", "'0' <= JMSPriority"
  };

  private static final String[] ILLEGAL_JMS_TIMESTAMP_SELECTORS = {
    "JMSTimestamp >= '0'", "'0' <= JMSTimestamp", "JMSTimestamp >= '2001/1/1 0:0'"
  };

  private static final String[] ILLEGAL_OR_SELECTORS = {"or", "true or", "or true"};

  private static final String[] ILLEGAL_STRING_SELECTORS = {
    "'abc' < 'abc'",
    "dummy < 'abc'",
    "'abc' < dummy",
    "'abc' > 'abc'",
    "dummy > 'abc'",
    "'abc' < dummy",
    "'abc' <= 'abc'",
    "dummy <= 'abc'",
    "'abc' <= dummy",
    "'abc' >= 'abc'",
    "dummy >= 'abc'",
    "'abc' >= dummy",
    "'abc'",
    "'abc' = 'abc",
    "'abc' = abc'",
    "'abc = 'abc'",
    "'abc'''' = 'abc'",
    "\"abc\" = \"abc\""
  };

  @Test
  public final void illegalAndSelectorsTest() throws JMSException {
    for (String selector : ILLEGAL_AND_SELECTORS) {
      Throwable thrown =
          assertThrows(InvalidSelectorException.class, () -> SelectorSupport.build(selector, true));
      assertEquals(selector, thrown.getMessage());
    }
  }

  @Test
  public final void illegalBetweenSelectorsTest() throws JMSException {
    for (String selector : ILLEGAL_BETWEEN_SELECTORS) {
      Throwable thrown =
          assertThrows(InvalidSelectorException.class, () -> SelectorSupport.build(selector, true));
      assertEquals(selector, thrown.getMessage());
    }
  }

  @Test
  public final void illegalBooleanSelectorsTest() throws JMSException {
    for (String selector : ILLEGAL_BOOLEAN_SELECTORS) {
      Throwable thrown =
          assertThrows(InvalidSelectorException.class, () -> SelectorSupport.build(selector, true));
      assertEquals(selector, thrown.getMessage());
    }
  }

  @Test
  public final void illegalFloatSelectorsTest() throws JMSException {
    for (String selector : ILLEGAL_FLOAT_SELECTORS) {
      Throwable thrown =
          assertThrows(InvalidSelectorException.class, () -> SelectorSupport.build(selector, true));
      assertEquals(selector, thrown.getMessage());
    }
  }

  @Test
  public final void illegalInSelectorsTest() throws JMSException {
    for (String selector : ILLEGAL_IN_SELECTORS) {
      Throwable thrown =
          assertThrows(InvalidSelectorException.class, () -> SelectorSupport.build(selector, true));
      assertEquals(selector, thrown.getMessage());
    }
  }

  @Test
  public final void illegalJMSPrioritySelectorsTest() throws JMSException {
    for (String selector : ILLEGAL_JMS_PRIORITY_SELECTORS) {
      Throwable thrown =
          assertThrows(InvalidSelectorException.class, () -> SelectorSupport.build(selector, true));
      assertEquals(selector, thrown.getMessage());
    }
  }

  @Test
  public final void illegalJMSTimestampSelectorsTest() throws JMSException {
    for (String selector : ILLEGAL_JMS_TIMESTAMP_SELECTORS) {
      Throwable thrown =
          assertThrows(InvalidSelectorException.class, () -> SelectorSupport.build(selector, true));
      assertEquals(selector, thrown.getMessage());
    }
  }

  @Test
  public final void illegalOrSelectorsTest() throws JMSException {
    for (String selector : ILLEGAL_OR_SELECTORS) {
      Throwable thrown =
          assertThrows(InvalidSelectorException.class, () -> SelectorSupport.build(selector, true));
      assertEquals(selector, thrown.getMessage());
    }
  }

  @Test
  public final void illegalStringSelectorsTest() throws JMSException {
    for (String selector : ILLEGAL_STRING_SELECTORS) {
      Throwable thrown =
          assertThrows(InvalidSelectorException.class, () -> SelectorSupport.build(selector, true));
      assertEquals(selector, thrown.getMessage());
    }
  }
}
