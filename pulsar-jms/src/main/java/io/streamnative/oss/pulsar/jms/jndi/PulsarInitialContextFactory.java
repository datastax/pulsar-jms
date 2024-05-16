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
package io.streamnative.oss.pulsar.jms.jndi;

import java.util.Hashtable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import lombok.extern.slf4j.Slf4j;

/** Entry point for using embedded JNDI provider. */
@Slf4j
public class PulsarInitialContextFactory implements InitialContextFactory {

  static final Map<Hashtable<?, ?>, PulsarContext> sharedInitialContexts =
      new ConcurrentHashMap<>();

  @Override
  public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
    Objects.requireNonNull(environment);
    boolean shared =
        Boolean.parseBoolean(environment.get(PulsarContext.USE_SHARED_JNDICONTEXT) + "");
    if (shared) {
      PulsarContext context =
          sharedInitialContexts.computeIfAbsent(
              environment,
              e -> {
                PulsarContext result = new PulsarContext(e);
                log.info("Creating shared JNDI {}", result);
                return result;
              });
      context.referenceCount.incrementAndGet();
      return context;
    } else {
      return new PulsarContext(environment);
    }
  }

  static boolean releaseSharedContext(Hashtable environment, PulsarContext context) {
    return sharedInitialContexts.compute(
            environment,
            (e, current) -> {
              if (current == context) {
                int currentCount = context.referenceCount.decrementAndGet();
                if (currentCount == 0) {
                  log.info("Disposing shared JNDI {}", current);
                  return null;
                } else {
                  return current;
                }
              } else {
                return current;
              }
            })
        == null;
  }
}
