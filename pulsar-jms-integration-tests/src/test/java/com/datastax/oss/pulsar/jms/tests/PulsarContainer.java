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

import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.admin.PulsarAdmin;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.admin.PulsarAdminBuilder;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.impl.auth.AuthenticationToken;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Slf4j
public class PulsarContainer implements AutoCloseable {

  private GenericContainer<?> pulsarContainer;
  private final String dockerImageVersion;
  private boolean transactions;
  private boolean serverSideSelectors;
  private boolean enableAuthentication;
  private final Path tempDir;
  public static final int BROKER_PORT = 6650;
  public static final int BROKER_HTTP_PORT = 8080;

  public PulsarContainer(
      String dockerImageVersion,
      boolean transactions,
      boolean serverSideSelectors,
      boolean enableAuthentication,
      Path tempDir) {
    this.dockerImageVersion = dockerImageVersion;
    this.transactions = transactions;
    this.serverSideSelectors = serverSideSelectors;
    this.enableAuthentication = enableAuthentication;
    this.tempDir = tempDir;
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
                  log.debug(text);
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

    content = content + "\nauthenticationEnabled=" + enableAuthentication + "\n";
    content = content + "\nauthorizationEnabled=" + enableAuthentication + "\n";

    Path tmpFile = tempDir.resolve("standalone.conf");
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

    // wait for system to be ready
    PulsarAdminBuilder pulsarAdminBuilder =
        PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl());
    if (enableAuthentication) {
      String token =
          IOUtils.toString(
              DockerTest.class.getResourceAsStream("/token.jwt"), StandardCharsets.UTF_8);
      pulsarAdminBuilder.authentication(
          AuthenticationToken.class.getName(), "token:" + token.trim());
    }

    try (PulsarAdmin admin = pulsarAdminBuilder.build(); ) {
      Awaitility.await()
          .until(
              () -> {
                try {
                  List<String> tenants = admin.tenants().getTenants();
                  log.info("tenants={}", tenants);
                  if (!tenants.contains("public")) {
                    return false;
                  }
                  List<String> namespaces = admin.namespaces().getNamespaces("public");
                  log.info("namespaces={}", namespaces);
                  return !namespaces.isEmpty();
                } catch (Exception error) {
                  log.info("cannot get tenants/namespaces", error);
                  return false;
                }
              });
    }

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
  }
}
