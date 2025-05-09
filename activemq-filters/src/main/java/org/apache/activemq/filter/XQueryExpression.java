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
package org.apache.activemq.filter;

import jakarta.jms.JMSException;

/** Used to evaluate an XQuery Expression in a JMS selector. */
public final class XQueryExpression implements BooleanExpression {
  private final String xpath;

  XQueryExpression(String xpath) {
    super();
    this.xpath = xpath;
  }

  public Object evaluate(MessageEvaluationContext message) throws JMSException {
    return Boolean.FALSE;
  }

  public String toString() {
    return "XQUERY " + ConstantExpression.encodeString(xpath);
  }

  /**
   * @param message
   * @return true if the expression evaluates to Boolean.TRUE.
   * @throws JMSException
   */
  public boolean matches(MessageEvaluationContext message) throws JMSException {
    Object object = evaluate(message);
    return object != null && object == Boolean.TRUE;
  }
}
