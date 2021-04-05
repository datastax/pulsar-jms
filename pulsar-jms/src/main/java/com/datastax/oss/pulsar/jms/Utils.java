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
import java.util.concurrent.ExecutionException;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;

final class Utils {
  private Utils() {}

  public static JMSException handleException(Throwable cause) {
    if (cause instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
    JMSException err = new JMSException(cause.getMessage());
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
      throw handleException(err);
    } catch (InterruptedException err) {
      throw handleException(err);
    }
  }

  public interface RunnableWithException<T> {
    T run() throws Exception;
  }

  public static <T> T invoke(RunnableWithException<T> code) throws JMSException {
    try {
      return code.run();
    } catch (Throwable err) {
      throw handleException(err);
    }
  }

  public static void executeListener(PulsarSession session, Runnable code) {
    currentSession.set(session);
    try {
      code.run();
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
}
