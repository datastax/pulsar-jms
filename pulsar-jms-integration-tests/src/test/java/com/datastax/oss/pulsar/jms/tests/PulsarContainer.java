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
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

public class PulsarContainer implements AutoCloseable {

  private GenericContainer<?> pulsarContainer;
  private final String dockerImageVersion;
  private boolean transactions;

  public static final int BROKER_PORT = 6650;
  public static final int BROKER_HTTP_PORT = 8080;

  public PulsarContainer(String dockerImageVersion, boolean transactions) {
    this.dockerImageVersion = dockerImageVersion;
    this.transactions = transactions;
  }

  public void start() throws Exception {
    CountDownLatch pulsarReady = new CountDownLatch(1);
    pulsarContainer =
        new GenericContainer<>(dockerImageVersion)
            .withExposedPorts(BROKER_PORT, BROKER_HTTP_PORT)
            .withCommand("bin/pulsar", "standalone", "--no-functions-worker", "-nss")
            .withLogConsumer(
                (f) -> {
                  String text = f.getUtf8String().trim();
                  if (text.contains("messaging service is ready")) {
                    pulsarReady.countDown();
                  }
                  System.out.println(text);
                });
    pulsarContainer.withClasspathResourceMapping(
        transactions ? "standalone_transactions.conf" : "standalone.conf",
        "/pulsar/conf/standalone.conf",
        BindMode.READ_ONLY);

    pulsarContainer.start();

    assertTrue(pulsarReady.await(1, TimeUnit.MINUTES));
    if (transactions) {
      pulsarContainer.execInContainer(
          "/pulsar/bin/pulsar", "initialize-transaction-coordinator-metadata");
      Thread.sleep(5000);
    }
  }

  public String getPulsarBrokerUrl() {
    return String.format(
        "pulsar://%s:%s", pulsarContainer.getHost(), pulsarContainer.getMappedPort(BROKER_PORT));
  }

  public String getHttpServiceUrl() {
    return String.format(
        "http://%s:%s", pulsarContainer.getHost(), pulsarContainer.getMappedPort(BROKER_HTTP_PORT));
  }

  @Override
  public void close() {
    if (pulsarContainer != null) {
      pulsarContainer.stop();
    }
  }
}
