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

import javax.jms.JMSException;
import javax.jms.Message;
import lombok.AllArgsConstructor;
import org.apache.activemq.ActiveMQMessageTransformation;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.selector.SelectorParser;

@AllArgsConstructor
public final class SelectorSupport {

  private final BooleanExpression expression;
  private final String selector;

  public static SelectorSupport build(String selector) throws JMSException {
    if (selector == null || selector.isEmpty()) {
      return null;
    }
    BooleanExpression parse = SelectorParser.parse(selector);
    return new SelectorSupport(parse, selector);
  }

  public boolean matches(Message message) throws JMSException {
    MessageEvaluationContext context = new MessageEvaluationContext();
    ActiveMQMessage activeMQMessage = ActiveMQMessageTransformation.transformMessage(message, null);
    context.setMessageReference(activeMQMessage);
    return expression.matches(context);
  }

  @Override
  public String toString() {
    return "Selector=" + selector + ", exp=" + expression;
  }
}
