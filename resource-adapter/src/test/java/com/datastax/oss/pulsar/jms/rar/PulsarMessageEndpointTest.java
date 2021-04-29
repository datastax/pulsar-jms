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
package com.datastax.oss.pulsar.jms.rar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import com.datastax.oss.pulsar.jms.PulsarMessage;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAResource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@Slf4j
public class PulsarMessageEndpointTest {

  static class DummyEndpoint implements MessageListener, MessageEndpoint {

    List<Message> receivedMessages = new ArrayList<>();
    AtomicInteger beforeDeliveryCount = new AtomicInteger();
    AtomicInteger afterDeliveryCount = new AtomicInteger();
    AtomicInteger releaseCount = new AtomicInteger();
    XAResource resource;

    protected void commitOrRollback(XAResource resource) {
      try {
        resource.prepare(null);
        resource.commit(null, true);
      } catch (Exception err) {
        throw new RuntimeException(err);
      }
    }

    public void setResource(XAResource resource) {
      this.resource = resource;
    }

    protected void processMessage(Message message) {
      receivedMessages.add(message);
    }

    @Override
    public void onMessage(Message message) {
      processMessage(message);
      commitOrRollback(resource);
    }

    @Override
    public void beforeDelivery(Method method) throws NoSuchMethodException, ResourceException {
      beforeDeliveryCount.incrementAndGet();
    }

    @Override
    public void afterDelivery() throws ResourceException {
      afterDeliveryCount.incrementAndGet();
    }

    @Override
    public void release() {
      releaseCount.incrementAndGet();
    }
  }

  @Test
  public void testDeliverMessage() throws Exception {
    DummyEndpoint listener = new DummyEndpoint();
    PulsarMessage pulsarMessage = mock(PulsarMessage.class);
    AtomicReference<Throwable> expectedError = new AtomicReference();
    testDeliverMessage(listener, pulsarMessage, expectedError::set);
    assertSame(pulsarMessage, listener.receivedMessages.get(0));
    assertEquals(1, listener.beforeDeliveryCount.get());
    assertEquals(1, listener.afterDeliveryCount.get());
    assertEquals(1, listener.releaseCount.get());
    verify(pulsarMessage, times(1)).acknowledge();
    verify(pulsarMessage, times(0)).negativeAck();
    assertNull(expectedError.get());
  }

  @Test
  public void testOnMessageError() throws Exception {
    DummyEndpoint listener =
        new DummyEndpoint() {
          @Override
          public void processMessage(Message message) {
            super.processMessage(message);
            throw new RuntimeException();
          }
        };
    PulsarMessage pulsarMessage = mock(PulsarMessage.class);
    AtomicReference<Throwable> expectedError = new AtomicReference();
    testDeliverMessage(listener, pulsarMessage, expectedError::set);

    assertEquals(1, listener.beforeDeliveryCount.get());
    assertEquals(1, listener.afterDeliveryCount.get());
    assertEquals(1, listener.releaseCount.get());
    verify(pulsarMessage, times(0)).acknowledge();
    verify(pulsarMessage, times(0)).negativeAck();
    assertNotNull(expectedError.get());
  }

  @Test
  public void testOnContainerRollback() throws Exception {
    DummyEndpoint listener =
        new DummyEndpoint() {
          @Override
          protected void commitOrRollback(XAResource resource) {
            try {
              resource.rollback(null);
            } catch (Throwable t) {
              throw new RuntimeException(t);
            }
          }
        };
    PulsarMessage pulsarMessage = mock(PulsarMessage.class);
    AtomicReference<Throwable> expectedError = new AtomicReference();
    testDeliverMessage(listener, pulsarMessage, expectedError::set);

    assertEquals(1, listener.beforeDeliveryCount.get());
    assertEquals(1, listener.afterDeliveryCount.get());
    assertEquals(1, listener.releaseCount.get());
    verify(pulsarMessage, times(0)).acknowledge();
    verify(pulsarMessage, times(1)).negativeAck();
    assertNull(expectedError.get());
  }

  @Test
  public void testOnBeforeDeliveryError() throws Exception {
    DummyEndpoint listener =
        new DummyEndpoint() {
          @Override
          public void beforeDelivery(Method method)
              throws NoSuchMethodException, ResourceException {
            super.beforeDelivery(method);
            throw new RuntimeException();
          }
        };
    PulsarMessage pulsarMessage = mock(PulsarMessage.class);
    AtomicReference<Throwable> expectedError = new AtomicReference();
    testDeliverMessage(listener, pulsarMessage, expectedError::set);

    assertEquals(1, listener.beforeDeliveryCount.get());
    assertEquals(0, listener.afterDeliveryCount.get());
    assertEquals(1, listener.releaseCount.get());
    verify(pulsarMessage, times(0)).acknowledge();
    verify(pulsarMessage, times(0)).negativeAck();
    assertNotNull(expectedError.get());
  }

  @Test
  public void testOnAfterDeliveryError() throws Exception {
    DummyEndpoint listener =
        new DummyEndpoint() {
          @Override
          public void afterDelivery() throws ResourceException {
            super.afterDelivery();
            throw new RuntimeException();
          }
        };
    PulsarMessage pulsarMessage = mock(PulsarMessage.class);
    AtomicReference<Throwable> expectedError = new AtomicReference();
    testDeliverMessage(listener, pulsarMessage, expectedError::set);

    assertEquals(1, listener.beforeDeliveryCount.get());
    assertEquals(1, listener.afterDeliveryCount.get());
    assertEquals(1, listener.releaseCount.get());
    verify(pulsarMessage, times(1)).acknowledge();
    verify(pulsarMessage, times(0)).negativeAck();
    assertNotNull(expectedError.get());
  }

  private void testDeliverMessage(
      DummyEndpoint listener,
      PulsarMessage message,
      java.util.function.Consumer<Throwable> errorCatcher)
      throws Exception {
    PulsarConnectionFactory pulsarConnectionFactory = mock(PulsarConnectionFactory.class);
    MessageEndpointFactory messageEndpointFactory = mock(MessageEndpointFactory.class);
    JMSContext context = mock(JMSContext.class);
    when(pulsarConnectionFactory.createContext(eq(JMSContext.CLIENT_ACKNOWLEDGE)))
        .thenReturn(context);
    JMSConsumer consumer = mock(JMSConsumer.class);
    when(context.createConsumer(any(Destination.class))).thenReturn(consumer);

    AtomicReference<MessageListener> internalListener = new AtomicReference<>();
    doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                internalListener.set(invocationOnMock.getArgumentAt(0, MessageListener.class));
                return null;
              }
            })
        .when(consumer)
        .setMessageListener(any(MessageListener.class));

    when(messageEndpointFactory.createEndpoint(any(XAResource.class)))
        .thenAnswer(
            new Answer<MessageEndpoint>() {
              @Override
              public MessageEndpoint answer(InvocationOnMock invocationOnMock) throws Throwable {
                XAResource resource =
                    (XAResource) invocationOnMock.getArgumentAt(0, XAResource.class);
                listener.setResource(resource);
                return listener;
              }
            });
    PulsarActivationSpec activationSpec = new PulsarActivationSpec();
    activationSpec.setDestination("MyQueue");
    activationSpec.setDestinationType("queue");

    PulsarMessageEndpoint endpoint =
        new PulsarMessageEndpoint(pulsarConnectionFactory, messageEndpointFactory, activationSpec);
    endpoint.start();

    // simulate Pulsar Client
    try {
      internalListener.get().onMessage(message);
    } catch (Throwable error) {
      errorCatcher.accept(error);
    }
  }
}
