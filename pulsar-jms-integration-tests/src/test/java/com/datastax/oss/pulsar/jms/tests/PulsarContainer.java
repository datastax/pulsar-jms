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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

@Slf4j
public class PulsarContainer implements AutoCloseable {

  private GenericContainer<?> pulsarContainer;
  private final String dockerImageVersion;
  private boolean transactions;
  private boolean serverSideSelectors;
  private Path tmpFile;
  public static final int BROKER_PORT = 6650;
  public static final int BROKER_HTTP_PORT = 8080;

  public PulsarContainer(
      String dockerImageVersion, boolean transactions, boolean serverSideSelectors) {
    this.dockerImageVersion = dockerImageVersion;
    this.transactions = transactions;
    this.serverSideSelectors = serverSideSelectors;
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

    String filename = "/standalone.conf";
    String content =
        new BufferedReader(
                new InputStreamReader(PulsarContainer.class.getResourceAsStream(filename)))
            .lines()
            .collect(Collectors.joining("\n"));
    if (transactions) {
      content = content + "\ntransactionCoordinatorEnabled=true\n";
    }

    if (serverSideSelectors) {
      content = content + "\nentryFilterNames=jms\n";
      content = content + "\nentryFiltersDirectory=/pulsar/filters\n";
    }

    tmpFile = Files.createTempFile("jms_standalone_test", ".conf");
    Files.write(tmpFile, content.getBytes(StandardCharsets.UTF_8));

    pulsarContainer.withFileSystemBind(
        tmpFile.toFile().getAbsolutePath(), "/pulsar/conf/standalone.conf", BindMode.READ_ONLY);

    pulsarContainer.withClasspathResourceMapping(
        "secret-key.key", "/pulsar/conf/secret-key.key", BindMode.READ_ONLY);

    if (serverSideSelectors) {
      Path files = Paths.get("target/classes/filters");
      // verify that the .nar file has been copied
      assertTrue(
          Files.list(files).anyMatch(p -> p.getFileName().toString().endsWith(".nar")),
          "Cannot find the .nar file in " + files.toAbsolutePath());
      pulsarContainer.withFileSystemBind(
          "target/classes/filters", "/pulsar/filters", BindMode.READ_ONLY);
    }

    pulsarContainer.withClasspathResourceMapping(
        "admin-token.jwt", "/pulsar/conf/admin-token.jwt", BindMode.READ_ONLY);

    pulsarContainer.withClasspathResourceMapping(
        "client.conf", "/pulsar/conf/client.conf", BindMode.READ_ONLY);

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
  public void close() throws Exception {
    if (pulsarContainer != null) {
      pulsarContainer.stop();
    }
    if (tmpFile != null) {
      Files.deleteIfExists(tmpFile);
    }
  }
}
