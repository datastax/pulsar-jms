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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
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

  /**
   * Map the JMS Priority to a partition.
   *
   * @param priority
   * @param numPartitions
   * @return the partition id
   */
  public static int mapPriorityToPartition(int priority, int numPartitions, boolean linear) {
    if (linear) {
      return mapPriorityToPartitionLinearly(priority, numPartitions);
    } else {
      return mapPriorityToPartitionNonLinearly(priority, numPartitions);
    }
  }

  static int mapPriorityToPartitionLinearly(int priority, int numPartitions) {
    if (numPartitions <= 1) {
      return 0;
    }
    if (numPartitions == 2) {
      if (priority <= PulsarMessage.DEFAULT_PRIORITY) {
        return 0;
      } else {
        return 1;
      }
    }
    if (numPartitions == 3) {
      if (priority < PulsarMessage.DEFAULT_PRIORITY) {
        return 0;
      } else if (priority == PulsarMessage.DEFAULT_PRIORITY) {
        return 1;
      } else {
        return 2;
      }
    }
    if (priority < 0) {
      priority = 0;
    } else if (priority > 9) {
      priority = 9;
    }

    // from 0 to 9
    double bucketSize = numPartitions / 10.0;
    double start = Math.floor(bucketSize * priority);
    double value = (start + ThreadLocalRandom.current().nextDouble(bucketSize));
    int result = (int) Math.floor(value);
    if (result >= numPartitions) {
      return numPartitions - 1;
    }
    return result;
  }

  static int mapPriorityToPartitionNonLinearly(int priority, int numPartitions) {
    if (numPartitions <= 1) {
      return 0;
    }
    if (priority < 0) {
      priority = 0;
    } else if (priority > 9) {
      priority = 9;
    }
    double bucketSize = numPartitions / 4.0;
    if (bucketSize <= 0) {
      bucketSize = 1;
    }
    double bucketStart;
    int slots;

    switch (priority) {
      case 0:
      case 1:
      case 2:
      case 3:
        // low priority, 1/4 of the partitions
        bucketStart = 0;
        slots = 1;
        if (numPartitions <= 3) {
          return 0;
        }
        break;
      case 4:
        // mid-priority, 1/2 of the partitions
        bucketStart = Math.ceil(bucketSize);
        if (numPartitions <= 2) {
          return 0;
        } else if (numPartitions == 3) {
          return 1;
        } else {
          slots = 2;
        }
        break;
      case 5:
      case 6:
      case 7:
      case 8:
      case 9:
        // high priority, 1/4 of the partitions
        if (numPartitions <= 3) {
          return numPartitions - 1;
        }
        bucketStart = Math.ceil(bucketSize * 3);
        slots = 1;
        break;
      default:
        throw new java.lang.IllegalStateException();
    }

    double value = (bucketStart + ThreadLocalRandom.current().nextDouble(bucketSize * slots));
    int result = (int) Math.floor(value);
    if (result >= numPartitions) {
      return numPartitions - 1;
    }
    return result;
  }

  /**
   * Utility to copy some path configurations into temporary files and update the parameterized configuration map
   * with the temporary file paths.
   * Only values for configuration keys that end with "path", case-insensitive, will be modified.
   * If the value is a String and begins with "base64:", case-sensitive, it will be base64 decoded then written to a
   * temp file.
   * If the value is a String and begins with "classpath:", case-sensitive, the file will be copied to a temp file.
   * The temp file permissions will only allow the current user to access the file. The temp file will be
   * deleted on JVM exit.
   * @param configuration - the configuration map to modify in place
   * @return list of paths representing files created by this method
   */
  static List<Path> writeEncodedPathConfigsToTempFiles(Map<String, Object> configuration) {
    List<Path> createdFiles = new ArrayList<>();
    for (Map.Entry<String, Object> entry : configuration.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      if (key.toLowerCase().endsWith("path") && value instanceof String) {
        String originalPath = (String) value;
        String finalPath;
        if (originalPath.startsWith("base64:")) {
          try {
            Path target = createTempFile();
            createdFiles.add(target);
            String encoded = originalPath.replace("\n", "").replace("\r", "").trim();
            encoded = encoded.substring("base64:".length());
            Files.write(target, Base64.getDecoder().decode(encoded));
            finalPath = target.toAbsolutePath().toString();
          } catch (IOException ex) {
            throw new RuntimeException(
                    "Cannot decode base64 " + key + " and create temporary file: " + ex, ex);
          }
        } else if (originalPath.startsWith("classpath:")) {
          try {
            Path target = createTempFile();
            createdFiles.add(target);
            try (InputStream inputStream = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream(originalPath.substring("classpath:".length()))) {
              if (inputStream == null) {
                throw new IllegalArgumentException("File " + originalPath + " not found for " + key);
              }
              Files.copy(inputStream, target);
            }
            finalPath = target.toAbsolutePath().toString();
          } catch (IOException ex) {
            throw new RuntimeException(
                    "Exception writing " + key + " to temporary file: " + ex, ex);
          }
        } else {
          continue;
        }
        log.info("Decoded {} to temporary file {}", key, finalPath);
        entry.setValue(finalPath);
      }
    }
    return createdFiles;
  }

  private static Path createTempFile() throws IOException {
    Path file = Files.createTempFile("pulsar-jms.", ".tmp");
    file.toFile().deleteOnExit();
    Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
    return file;
  }
}
