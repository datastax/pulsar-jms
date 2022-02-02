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
package com.datastax.oss.pulsar.jms.tests;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

public class PulsarContainer implements AutoCloseable {

  private GenericContainer<?> pulsarContainer;
  private final Network network;

  public PulsarContainer(Network network) {
    this.network = network;
  }

  public void start() throws Exception {
    CountDownLatch pulsarReady = new CountDownLatch(1);
    pulsarContainer =
        new GenericContainer<>("apachepulsar/pulsar:2.8.1")
            .withNetwork(network)
            .withNetworkAliases("pulsar")
            .withCommand(
                "bin/pulsar",
                "standalone",
                "--advertised-address",
                "pulsar",
                "--no-functions-worker",
                "-nss")
            .withLogConsumer(
                (f) -> {
                  String text = f.getUtf8String().trim();
                  if (text.contains("messaging service is ready")) {
                    pulsarReady.countDown();
                  }
                  System.out.println(text);
                });
    pulsarContainer.start();
    assertTrue(pulsarReady.await(1, TimeUnit.MINUTES));
  }

  @Override
  public void close() {
    if (pulsarContainer != null) {
      pulsarContainer.stop();
    }
  }
}
