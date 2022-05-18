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

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.jms.Destination;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.Message;
import lombok.AllArgsConstructor;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.selector.SelectorParser;

@AllArgsConstructor
public final class SelectorSupport {

  private final BooleanExpression expression;
  private final String selector;

  public static SelectorSupport build(String selector, boolean enabled) throws JMSException {
    if (selector == null || selector.isEmpty()) {
      return null;
    }
    if (!enabled) {
      throw new InvalidSelectorException(
          "Client-Side selectors are not enabled, please set enableClientSideEmulation=true");
    }
    BooleanExpression parse = SelectorParser.parse(selector);
    return new SelectorSupport(parse, selector);
  }

  public boolean matches(
      Map<String, Object> messageProperties,
      String jmsMessageId,
      String jmsCorrelationId,
      Destination jmsReplyTo,
      Destination jmsDestination,
      int jmsDeliveryMode,
      String jmsType,
      long jmsExpiration,
      int jmsPriority,
      long jmsTimestamp)
      throws JMSException {
    // convert anything that can be used by the selector
    // https://github.com/apache/activemq/blob/d54d046b8a8f2e9e5c0a28e1f8c7634b3c8b18e4/activemq-client/src/main/java/org/apache/activemq/filter/PropertyExpression.java#L35

    // check the docs at https://docs.oracle.com/javaee/1.4/api/javax/jms/Message.html

    // in particular:
    // Message header field references are restricted to JMSDeliveryMode, JMSPriority, JMSMessageID,
    // JMSTimestamp,
    // JMSCorrelationID, and JMSType. JMSMessageID, JMSCorrelationID, and JMSType values may be null
    // and if so are treated as a NULL value.
    MessageEvaluationContext context =
        new MessageEvaluationContext() {
          @Override
          public boolean isDropped() {
            // this method would trigger churn
            return false;
          }
        };

    ActiveMQMessage toMessage =
        new ActiveMQMessage() {

          // this methods in ActiveMQMessage use synchronization and also do lot of useless
          // computations
          @Override
          public int incrementReferenceCount() {
            return 0;
          }

          @Override
          public int decrementReferenceCount() {
            return 0;
          }

          @Override
          public int getSize() {
            return 0;
          }

          @Override
          public int getReferenceCount() {
            return 0;
          }

          // saving CPU cycles here, PropertyExpression calls this method for non-system properties
          // https://github.com/apache/activemq/blob/d54d046b8a8f2e9e5c0a28e1f8c7634b3c8b18e4/activemq-client/src/main/java/org/apache/activemq/filter/PropertyExpression.java#L226
          @Override
          public Object getProperty(String name) {
            return messageProperties.get(name);
          }

          @Override
          public Map<String, Object> getProperties() {
            throw new UnsupportedOperationException("not supported - getProperties");
          };
        };
    // the is no need to call toMessage.setProperties()
    toMessage.setJMSMessageID(jmsMessageId);
    toMessage.setJMSCorrelationID(jmsCorrelationId);
    toMessage.setJMSReplyTo(ActiveMQDestination.transform(jmsReplyTo));
    toMessage.setJMSDestination(ActiveMQDestination.transform(jmsDestination));
    toMessage.setJMSDeliveryMode(jmsDeliveryMode);
    toMessage.setJMSType(jmsType);
    toMessage.setJMSExpiration(jmsExpiration);
    toMessage.setJMSPriority(jmsPriority);
    toMessage.setJMSTimestamp(jmsTimestamp);
    context.setMessageReference(toMessage);
    return expression.matches(context);
  }

  public boolean matches(Message fromMessage) throws JMSException {
    // do not use ActiveMQMessageTransformation.transformMessage because it would copy the contents
    // of the message
    // selectors apply only to properties and headers
    Enumeration propertyNames = fromMessage.getPropertyNames();
    Map<String, Object> properties = new HashMap<>();
    while (propertyNames.hasMoreElements()) {
      String name = propertyNames.nextElement().toString();
      Object obj = fromMessage.getObjectProperty(name);
      properties.put(name, obj);
    }
    return matches(
        properties,
        fromMessage.getJMSMessageID(),
        fromMessage.getJMSCorrelationID(),
        fromMessage.getJMSReplyTo(),
        fromMessage.getJMSDestination(),
        fromMessage.getJMSDeliveryMode(),
        fromMessage.getJMSType(),
        fromMessage.getJMSExpiration(),
        fromMessage.getJMSPriority(),
        fromMessage.getJMSTimestamp());
  }

  @Override
  public String toString() {
    return "Selector=" + selector + ", exp=" + expression;
  }

  public String getSelector() {
    return selector;
  }
}
