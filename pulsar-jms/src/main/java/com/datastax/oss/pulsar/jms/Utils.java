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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
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

@Slf4j
final class Utils {
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

  public static void executeListenerInSessionContext(PulsarSession session, Runnable code) {
    currentSession.set(session);
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

  public static void checkNotOnListener(PulsarSession session) throws JMSException {
    PulsarSession current = currentSession.get();
    if (current == session) {
      throw new IllegalStateException("Cannot call this method inside a listener");
    }
  }

  private static ThreadLocal<PulsarSession> currentSession = new ThreadLocal();

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
          jmsException.getMessage(), jmsException.getErrorCode(), jmsException.getCause());
    }
    if (err instanceof TransactionRolledBackException) {
      TransactionRolledBackException jmsException = (TransactionRolledBackException) err;
      throw new TransactionRolledBackRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), jmsException.getCause());
    }
    if (err instanceof InvalidDestinationException) {
      InvalidDestinationException jmsException = (InvalidDestinationException) err;
      throw new InvalidDestinationRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), jmsException.getCause());
    }
    if (err instanceof InvalidClientIDException) {
      InvalidClientIDException jmsException = (InvalidClientIDException) err;
      throw new InvalidClientIDRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), jmsException.getCause());
    }
    if (err instanceof InvalidSelectorException) {
      InvalidSelectorException jmsException = (InvalidSelectorException) err;
      throw new InvalidSelectorRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), jmsException.getCause());
    }
    if (err instanceof MessageFormatException) {
      MessageFormatException jmsException = (MessageFormatException) err;
      throw new MessageFormatRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), jmsException.getCause());
    }
    if (err instanceof MessageNotWriteableException) {
      MessageNotWriteableException jmsException = (MessageNotWriteableException) err;
      throw new MessageNotWriteableRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), jmsException.getCause());
    }
    if (err instanceof JMSSecurityException) {
      JMSSecurityException jmsException = (JMSSecurityException) err;
      throw new JMSSecurityRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), jmsException.getCause());
    }
    if (err instanceof JMSException) {
      JMSException jmsException = (JMSException) err;
      throw new JMSRuntimeException(
          jmsException.getMessage(), jmsException.getErrorCode(), jmsException.getCause());
    }
    JMSRuntimeException jms = new JMSRuntimeException("Generic error " + err.getMessage());
    jms.initCause(err);
    throw jms;
  }
}
