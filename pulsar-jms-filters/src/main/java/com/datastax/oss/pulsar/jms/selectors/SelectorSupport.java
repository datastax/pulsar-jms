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
import java.util.function.Function;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.Message;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.selector.SelectorParser;

@AllArgsConstructor
@Slf4j
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
    if (log.isDebugEnabled()) {
      log.debug("parsed {} as {}", selector, parse);
    }
    return new SelectorSupport(parse, selector);
  }

  private static class PropertiesCache implements Function<String, Object> {
    final Function<String, Object> messagePropertiesAccessor;
    private static final Object CACHED_NULL = new Object();

    final Map<String, Object> cache = new HashMap<>();

    public PropertiesCache(Function<String, Object> messagePropertiesAccessor) {
      this.messagePropertiesAccessor = messagePropertiesAccessor;
    }

    @Override
    public Object apply(String s) {
      Object result = cache.get(s);
      if (result == null) {
        result = messagePropertiesAccessor.apply(s);
        if (result == null) {
          cache.put(s, CACHED_NULL);
        } else {
          cache.put(s, result);
        }
        return result;
      }
      if (result == CACHED_NULL) {
        return null;
      }
      return result;
    }
  }

  public boolean matches(Function<String, Object> messagePropertiesAccessor) throws JMSException {

    // this cache is important in order to be able to not parse Message Metadata more than once
    // for complex selectors that refer more times to the same property
    PropertiesCache messageProperties = new PropertiesCache(messagePropertiesAccessor);

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

          @Override
          public String getJMSMessageID() {
            return (String) messageProperties.apply("JMSMessageID");
          }

          @Override
          public MessageId getMessageId() {
            return new MessageId(getJMSMessageID());
          }

          @Override
          public Destination getJMSReplyTo() {
            return (Destination) messageProperties.apply("JMSReplyTo");
          }

          @Override
          public ActiveMQDestination getReplyTo() {
            return (ActiveMQDestination) messageProperties.apply("JMSReplyTo");
          }

          @Override
          public ActiveMQDestination getOriginalDestination() {
            return (ActiveMQDestination) messageProperties.apply("JMSDestination");
          }

          @Override
          public String getJMSCorrelationID() {
            return (String) messageProperties.apply("JMSCorrelationID");
          }

          @Override
          public String getCorrelationId() {
            return (String) messageProperties.apply("JMSCorrelationID");
          }

          @Override
          public long getJMSTimestamp() {
            return (long) messageProperties.apply("JMSTimestamp");
          }

          @Override
          public long getTimestamp() {
            return (long) messageProperties.apply("JMSTimestamp");
          }

          @Override
          public String getGroupID() {
            return (String) messageProperties.apply("JMSXGroupID");
          }

          @Override
          public int getGroupSequence() {
            return (int) messageProperties.apply("JMSXGroupSeq");
          }

          @Override
          public boolean isRedelivered() {
            // not supported
            return false;
          }

          @Override
          public long getJMSExpiration() {
            return (long) messageProperties.apply("JMSExpiration");
          }

          @Override
          public long getExpiration() {
            return (long) messageProperties.apply("JMSExpiration");
          }

          @Override
          public int getJMSPriority() {
            return (int) messageProperties.apply("JMSPriority");
          }

          @Override
          public byte getPriority() {
            return (byte) ((int) messageProperties.apply("JMSPriority"));
          }

          @Override
          public int getJMSDeliveryMode() {
            return (int) messageProperties.apply("JMSDeliveryMode");
          }

          @Override
          public boolean isPersistent() {
            return ((int) messageProperties.apply("JMSDeliveryMode")) == DeliveryMode.PERSISTENT;
          }

          @Override
          public String getJMSType() {
            return (String) messageProperties.apply("JMSType");
          }

          @Override
          public String getType() {
            return (String) messageProperties.apply("JMSType");
          }

          @Override
          public Destination getJMSDestination() {
            return (Destination) messageProperties.apply("JMSDestination");
          }

          // saving CPU cycles here, PropertyExpression calls this method for non-system properties
          // https://github.com/apache/activemq/blob/d54d046b8a8f2e9e5c0a28e1f8c7634b3c8b18e4/activemq-client/src/main/java/org/apache/activemq/filter/PropertyExpression.java#L226
          @Override
          public Object getProperty(String name) {
            return messageProperties.apply(name);
          }

          @Override
          public Map<String, Object> getProperties() {
            throw new UnsupportedOperationException("not supported - getProperties");
          };
        };
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
    Function<String, Object> getProperty =
        (name) -> {
          try {
            switch (name) {
              case "JMSMessageID":
                return fromMessage.getJMSMessageID();
              case "JMSCorrelationID":
                return fromMessage.getJMSCorrelationID();
              case "JMSReplyTo":
                return fromMessage.getJMSReplyTo();
              case "JMSDestination":
                return fromMessage.getJMSDestination();
              case "JMSDeliveryMode":
                return fromMessage.getJMSDeliveryMode();
              case "JMSType":
                return fromMessage.getJMSType();
              case "JMSExpiration":
                return fromMessage.getJMSExpiration();
              case "JMSPriority":
                return fromMessage.getJMSPriority();
              case "JMSTimestamp":
                return fromMessage.getJMSTimestamp();
              default:
                return properties.get(name);
            }
          } catch (JMSException err) {
            throw new RuntimeException(err);
          }
        };
    return matches(getProperty);
  }

  @Override
  public String toString() {
    return "Selector=" + selector + ", exp=" + expression;
  }

  public String getSelector() {
    return selector;
  }
}
