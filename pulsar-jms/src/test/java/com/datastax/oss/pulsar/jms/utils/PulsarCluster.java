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

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.PulsarTransactionCoordinatorMetadataSetup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;

/** Pulsar cluster. */
public class PulsarCluster implements AutoCloseable {
  private final PulsarService service;
  private final BookKeeperCluster bookKeeperCluster;

  public PulsarCluster(Path tempDir) throws Exception {
    this(tempDir, true);
  }

  public PulsarCluster(Path tempDir, boolean allowAutoTopicCreation) throws Exception {
    this(tempDir, allowAutoTopicCreation, true);
  }

  public PulsarCluster(Path tempDir, boolean allowAutoTopicCreation, boolean enableTransactions)
      throws Exception {
    this.bookKeeperCluster = new BookKeeperCluster(tempDir, PortManager.nextFreePort());
    ServiceConfiguration config = new ServiceConfiguration();
    config.setZookeeperServers(bookKeeperCluster.getZooKeeperAddress());
    config.setClusterName("localhost");
    config.setManagedLedgerDefaultEnsembleSize(1);
    config.setManagedLedgerDefaultWriteQuorum(1);
    config.setManagedLedgerDefaultAckQuorum(1);
    config.setBrokerServicePort(Optional.of(PortManager.nextFreePort()));
    config.setAllowAutoTopicCreation(allowAutoTopicCreation);
    config.setWebSocketServiceEnabled(false);
    config.setSystemTopicEnabled(true);
    config.setBookkeeperNumberOfChannelsPerBookie(1);
    config.setBookkeeperExplicitLacIntervalInMills(500);
    config.setTransactionCoordinatorEnabled(enableTransactions);
    config.setBookkeeperMetadataServiceUri(bookKeeperCluster.getBookKeeperMetadataURI());
    config.setWebServicePort(Optional.of(PortManager.nextFreePort()));
    config.setBookkeeperUseV2WireProtocol(false);
    config.setEntryFilterNames(Arrays.asList("jms"));
    config.setEntryFiltersDirectory("target/classes/filters");
    service = new PulsarService(config);
  }

  public PulsarService getService() {
    return service;
  }

  public String getAddress() {
    return service.getWebServiceAddress();
  }

  public void start() throws Exception {
    bookKeeperCluster.startBookie();
    service.start();
    service.getAdminClient().clusters().createCluster("localhost", ClusterData.builder().build());
    service
        .getAdminClient()
        .tenants()
        .createTenant(
            "public",
            TenantInfo.builder()
                .adminRoles(Collections.singleton("admin"))
                .allowedClusters(Collections.singleton("localhost"))
                .build());
    service.getAdminClient().namespaces().createNamespace("public/default");

    service
        .getAdminClient()
        .tenants()
        .createTenant(
            "pulsar",
            TenantInfo.builder()
                .adminRoles(Collections.singleton("admin"))
                .allowedClusters(Collections.singleton("localhost"))
                .build());

    if (service.getConfiguration().isTransactionCoordinatorEnabled()) {

      // run initialize-transaction-coordinator-metadata
      PulsarTransactionCoordinatorMetadataSetup.main(
          new String[] {"-c", "localhost", "-cs", bookKeeperCluster.getZooKeeperAddress()});

      // pre-create __transaction_buffer_snapshot for public/default namespace
      service
          .getAdminClient()
          .topics()
          .createNonPartitionedTopic("persistent://public/default/__transaction_buffer_snapshot");
    }
  }

  public void close() throws Exception {
    service.close();
    bookKeeperCluster.close();
  }
}
