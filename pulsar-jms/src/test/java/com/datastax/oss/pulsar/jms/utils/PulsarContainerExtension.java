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
package com.datastax.oss.pulsar.jms.utils;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Slf4j
public class PulsarContainerExtension implements BeforeAllCallback, AfterAllCallback {
  public static final String PULSAR_IMAGE = "datastax/lunastreaming:3.1_3.1";
  private PulsarContainer pulsarContainer;
  private Consumer<PulsarContainerExtension> onContainerReady;
  private Map<String, String> env = new HashMap<>();

  private Network network;

  private PulsarAdmin admin;

  public PulsarContainerExtension() {
    env.put("PULSAR_PREFIX_acknowledgmentAtBatchIndexLevelEnabled", "true");
    env.put("PULSAR_PREFIX_entryFiltersDirectory", "/pulsar/filters");
    env.put("PULSAR_PREFIX_entryFilterNames", "jms");
    env.put("PULSAR_PREFIX_maxConsumerMetadataSize", (1024 * 1024) + "");
    env.put("PULSAR_PREFIX_transactionCoordinatorEnabled", "true");
    env.put("PULSAR_PREFIX_brokerDeleteInactivePartitionedTopicMetadataEnabled", "false");
    env.put("PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled", "false");
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) {
    if (admin != null) {
      admin.close();
    }
    if (pulsarContainer != null) {
      pulsarContainer.close();
    }
    if (network != null) {
      network.close();
    }
  }

  @Override
  @SneakyThrows
  public void beforeAll(ExtensionContext extensionContext) {
    network = Network.newNetwork();
    CountDownLatch pulsarReady = new CountDownLatch(1);
    log.info("ENV: {}", env);
    pulsarContainer =
        new PulsarContainer(
                DockerImageName.parse(PULSAR_IMAGE)
                    .asCompatibleSubstituteFor("apachepulsar/pulsar"))
            .withNetwork(network)
            .withEnv(env)
            .withLogConsumer(
                (f) -> {
                  String text = f.getUtf8String().trim();
                  if (text.contains("messaging service is ready")) {
                    pulsarReady.countDown();
                  }
                  log.info(text);
                })
            .withCopyFileToContainer(
                MountableFile.forHostPath("target/classes/filters"), "/pulsar/filters")
            .withCopyFileToContainer(
                MountableFile.forHostPath("target/classes/interceptors"), "/pulsar/interceptors");
    // start Pulsar and wait for it to be ready to accept requests
    pulsarContainer.start();
    assertTrue(pulsarReady.await(1, TimeUnit.MINUTES));
    admin =
        PulsarAdmin.builder()
            .serviceHttpUrl("http://localhost:" + pulsarContainer.getMappedPort(8080))
            .build();
    if (onContainerReady != null) {
      onContainerReady.accept(this);
    }
  }

  public PulsarContainerExtension withOnContainerReady(
      Consumer<PulsarContainerExtension> onContainerReady) {
    this.onContainerReady = onContainerReady;
    return this;
  }

  public PulsarContainerExtension withEnv(String key, String value) {
    this.env.put(key, value);
    return this;
  }

  public PulsarContainerExtension withEnv(Map<String, String> env) {
    this.env.putAll(env);
    return this;
  }

  protected void onContainerReady() {}

  public String getBrokerUrl() {
    return pulsarContainer.getPulsarBrokerUrl();
  }

  public String getHttpServiceUrl() {
    return pulsarContainer.getHttpServiceUrl();
  }

  public PulsarContainer getPulsarContainer() {
    return pulsarContainer;
  }

  public Map<String, Object> buildJMSConnectionProperties() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("webServiceUrl", getHttpServiceUrl());
    properties.put("brokerServiceUrl", getBrokerUrl());
    return properties;
  }

  public PulsarAdmin getAdmin() {
    return admin;
  }
}
