/*
 * Copyright 2020 eolivelli.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.jms.utils;

import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;

import java.nio.file.Path;
import java.util.Optional;

/**
 * Pulsar cluster.
 */
public class PulsarCluster implements AutoCloseable {
    private final PulsarService service;
    private final BookKeeperCluster bookKeeperCluster;
    
    public PulsarCluster(Path tempDir) throws Exception {
        this.bookKeeperCluster = new BookKeeperCluster(tempDir, 9876);
        ServiceConfiguration config = new ServiceConfiguration();
        config.setZookeeperServers(bookKeeperCluster.getZooKeeperAddress());
        config.setClusterName("localhost");
        config.setManagedLedgerDefaultEnsembleSize(1);
        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        config.setBrokerServicePort(Optional.of(PortManager.nextFreePort()));
        config.setWebSocketServiceEnabled(false);
        config.setBookkeeperExplicitLacIntervalInMills(500);
        config.setBookkeeperUseV2WireProtocol(false);
        config.setWebServicePort(Optional.of(PortManager.nextFreePort()));
        service = new PulsarService(config);
    }

    public PulsarService getService() {
        return service;
    }

    public String getAddress() {
        return service.getWebServiceAddress();
    }
    
    public void start() throws Exception {
        service.start();
    }
    public void close() throws Exception {
        service.close();
        bookKeeperCluster.close();
    }
}
