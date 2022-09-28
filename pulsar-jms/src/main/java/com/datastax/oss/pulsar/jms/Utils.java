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
package com.datastax.oss.pulsar.jms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.InvalidClientIDException;
import javax.jms.InvalidClientIDRuntimeException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidDestinationRuntimeException;
import javax.jms.InvalidSelectorException;
import javax.jms.InvalidSelectorRuntimeException;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.JMSSecurityException;
import javax.jms.JMSSecurityRuntimeException;
import javax.jms.MessageFormatException;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageNotWriteableRuntimeException;
import javax.jms.TransactionRolledBackException;
import javax.jms.TransactionRolledBackRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

@Slf4j
public final class Utils {
  private Utils() {}

  public static JMSException handleException(Throwable cause) {
    while (cause instanceof CompletionException) {
      cause = cause.getCause();
    }
    if (cause instanceof JMSException) {
      return (JMSException) cause;
    }
    if (cause instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
    if (cause instanceof ClassCastException) {
      return (JMSException)
          new MessageFormatException("Invalid cast " + cause.getMessage()).initCause(cause);
    }
    if (cause instanceof NumberFormatException) {
      return (JMSException)
          new MessageFormatException("Invalid conversion " + cause.getMessage()).initCause(cause);
    }
    JMSException err = new JMSException(cause + "");
    err.initCause(cause);
    if (cause instanceof Exception) {
      err.setLinkedException((Exception) cause);
    } else {
      err.setLinkedException(new Exception(cause));
    }
    return err;
  }

  public static <T> T get(CompletableFuture<T> future) throws JMSException {
    try {
      return future.get();
    } catch (ExecutionException err) {
      throw handleException(err.getCause());
    } catch (InterruptedException err) {
      throw handleException(err);
    }
  }

  public interface SupplierWithException<T> {
    T run() throws Exception;
  }

  public interface RunnableWithException {
    void run() throws Exception;
  }

  public static <T> T invoke(SupplierWithException<T> code) throws JMSException {
    try {
      return code.run();
    } catch (Throwable err) {
      throw handleException(err);
    }
  }

  public static void invoke(RunnableWithException code) throws JMSException {
    try {
      code.run();
    } catch (Throwable err) {
      throw handleException(err);
    }
  }

  public static void executeMessageListenerInSessionContext(
      PulsarSession session, PulsarMessageConsumer consumer, Runnable code) {
    currentSession.set(new CallbackContext(session, consumer, null));
    try {
      session.executeCriticalOperation(
          () -> {
            code.run();
            return null;
          });
    } catch (IllegalStateException err) {
      log.debug("Ignore error in listener", err);
    } catch (JMSException err) {
      log.error("Unexpected error in listener", err);
    } finally {
      currentSession.remove();
    }
  }

  public static void executeCompletionListenerInSessionContext(
      PulsarSession session, PulsarMessageProducer producer, Runnable code) {
    currentSession.set(new CallbackContext(session, null, producer));
    try {
      session.executeCriticalOperation(
          () -> {
            code.run();
            return null;
          });
    } catch (IllegalStateException err) {
      log.debug("Ignore error in listener", err);
    } catch (JMSException err) {
      log.error("Unexpected error in listener", err);
    } finally {
      currentSession.remove();
    }
  }

  public static boolean isOnMessageListener(PulsarSession session, PulsarMessageConsumer consumer) {
    CallbackContext current = currentSession.get();
    return current != null && current.session == session && current.consumer == consumer;
  }

  public static void checkNotOnMessageListener(PulsarSession session) throws JMSException {
    CallbackContext current = currentSession.get();
    if (current != null && current.session == session && current.consumer != null) {
      throw new IllegalStateException("Cannot call this method inside a listener");
    }
  }

  public static void checkNotOnSessionCallback(PulsarSession session) throws JMSException {
    CallbackContext current = currentSession.get();
    if (current != null && current.session == session) {
      throw new IllegalStateException("Cannot call this method inside a callback");
    }
  }

  public static void checkNotOnMessageProducer(
      PulsarSession session, PulsarMessageProducer producer) throws JMSException {
    CallbackContext current = currentSession.get();
    if (current != null
        && current.session == session
        && ((producer != null && current.producer == producer)
            || // specific producer
            (producer == null && current.producer != null))) // any producer
    {
      throw new IllegalStateException("Cannot call this method inside a CompletionListener");
    }
  }

  private static class CallbackContext {
    final PulsarSession session;
    final PulsarMessageConsumer consumer;
    final PulsarMessageProducer producer;

    private CallbackContext(
        PulsarSession session, PulsarMessageConsumer consumer, PulsarMessageProducer producer) {
      this.session = session;
      this.consumer = consumer;
      this.producer = producer;
    }
  }

  private static ThreadLocal<CallbackContext> currentSession = new ThreadLocal();

  public static void noException(RunnableWithException run) {
    try {
      run.run();
    } catch (Exception err) {
      if (err instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException(err);
    }
  }

  public static <T> T noException(SupplierWithException<T> run) {
    try {
      return run.run();
    } catch (Exception err) {
      if (err instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException(err);
    }
  }

  public static <T> T runtimeException(SupplierWithException<T> run) {
    try {
      return run.run();
    } catch (Exception err) {
      if (err instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throwAsRuntimeException(err);
      return null;
    }
  }

  public static void runtimeException(RunnableWithException run) {
    try {
      run.run();
    } catch (Exception err) {
      if (err instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throwAsRuntimeException(err);
    }
  }

  private static void throwAsRuntimeException(Exception err) {
    if (err instanceof NumberFormatException) {
      throw (MessageFormatRuntimeException)
          new MessageFormatRuntimeException("Illegal value: " + err.getMessage()).initCause(err);
    }
    if (err instanceof IllegalStateException) {
      IllegalStateException jmsException = (IllegalStateException) err;
      throw new IllegalStateRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), err);
    }
    if (err instanceof TransactionRolledBackException) {
      TransactionRolledBackException jmsException = (TransactionRolledBackException) err;
      throw new TransactionRolledBackRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), err);
    }
    if (err instanceof InvalidDestinationException) {
      InvalidDestinationException jmsException = (InvalidDestinationException) err;
      throw new InvalidDestinationRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), err);
    }
    if (err instanceof InvalidClientIDException) {
      InvalidClientIDException jmsException = (InvalidClientIDException) err;
      throw new InvalidClientIDRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), err);
    }
    if (err instanceof InvalidSelectorException) {
      InvalidSelectorException jmsException = (InvalidSelectorException) err;
      throw new InvalidSelectorRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), err);
    }
    if (err instanceof MessageFormatException) {
      MessageFormatException jmsException = (MessageFormatException) err;
      throw new MessageFormatRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), err);
    }
    if (err instanceof MessageNotWriteableException) {
      MessageNotWriteableException jmsException = (MessageNotWriteableException) err;
      throw new MessageNotWriteableRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), err);
    }
    if (err instanceof JMSSecurityException) {
      JMSSecurityException jmsException = (JMSSecurityException) err;
      throw new JMSSecurityRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), err);
    }
    if (err instanceof JMSException) {
      JMSException jmsException = (JMSException) err;
      throw new JMSRuntimeException(jmsException.getMessage(), jmsException.getErrorCode(), err);
    }
    JMSRuntimeException jms = new JMSRuntimeException("Generic error " + err.getMessage());
    jms.initCause(err);
    throw jms;
  }

  private static List deepCopyList(List configuration) {
    return (List) configuration.stream().map(f -> deepCopyObject(f)).collect(Collectors.toList());
  }

  private static Set deepCopySet(Set configuration) {
    return (Set) configuration.stream().map(f -> deepCopyObject(f)).collect(Collectors.toSet());
  }

  public static Object deepCopyObject(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Map) {
      return deepCopyMap((Map) value);
    }
    if (value instanceof List) {
      return deepCopyList((List) value);
    }
    if (value instanceof Set) {
      return deepCopySet((Set) value);
    }
    return value;
  }

  public static Map<String, Object> deepCopyMap(Map<String, Object> configuration) {
    if (configuration == null) {
      return null;
    }
    Map<String, Object> copy = new HashMap<>();
    configuration.forEach(
        (key, value) -> {
          copy.put(key, deepCopyObject(value));
        });
    return copy;
  }

  public static String getAndRemoveString(
      String name, String defaultValue, Map<String, Object> properties) {
    Object value = (Object) properties.remove(name);
    return value != null ? value.toString() : defaultValue;
  }

  public static boolean sameEntryId(MessageId a, MessageId b) {
    // get rid of TopicMessageIdImpl
    MessageIdImpl a1 = MessageIdImpl.convertToMessageIdImpl(a);
    MessageIdImpl b1 = MessageIdImpl.convertToMessageIdImpl(b);
    return a1.getLedgerId() == b1.getLedgerId() && a1.getEntryId() == b1.getEntryId();
  }
}
