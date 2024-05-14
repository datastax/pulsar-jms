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
package io.streamnative.oss.pulsar.jms.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.server.EmbeddedServer;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.util.PortManager;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

@Slf4j
public final class BookKeeperCluster implements AutoCloseable {

  TestingServer zkServer;
  List<EmbeddedServer> bookies = new ArrayList<>();
  Map<String, ServerConfiguration> configurations = new HashMap<>();
  Path path;

  public BookKeeperCluster(Path path, int zkPort) throws Exception {
    InstanceSpec spec = new InstanceSpec(path.toFile(), zkPort, -1, -1, true, -1, 4000, -1);
    zkServer = new TestingServer(spec, true);
    // waiting for ZK to be reachable
    CountDownLatch latch = new CountDownLatch(1);
    ZooKeeper zk =
        new ZooKeeper(
            zkServer.getConnectString(),
            getTimeout(),
            (WatchedEvent event) -> {
              if (event.getState() == KeeperState.SyncConnected) {
                latch.countDown();
              }
            });
    try {
      if (!latch.await(getTimeout(), TimeUnit.MILLISECONDS)) {
        log.info(
            "ZK client did not connect withing {0} seconds, maybe the server did not start up",
            getTimeout());
      }
    } finally {
      zk.close(1000);
    }
    this.path = path;
    log.info("Started ZK cluster at " + getZooKeeperAddress());
  }

  public void startBookie() throws Exception {
    startBookie(true);
  }

  public void startBookie(boolean format) throws Exception {
    if (!bookies.isEmpty() && format) {
      throw new Exception("bookie already started");
    }
    ServerConfiguration conf = new ServerConfiguration();
    conf.setBookiePort(PortManager.nextFreePort());
    conf.setUseHostNameAsBookieID(true);
    conf.setAllowEphemeralPorts(true);
    Path targetDir = path.resolve("bookie_data_" + bookies.size());
    conf.setMetadataServiceUri(getBookKeeperMetadataURI());
    conf.setLedgerDirNames(new String[] {targetDir.toAbsolutePath().toString()});
    conf.setJournalDirName(targetDir.toAbsolutePath().toString());
    // required for chunking test, this is the default value in bookkeeper.conf in Pulsar
    conf.setNettyMaxFrameSizeBytes(5253120);
    conf.setFlushInterval(10000);
    conf.setGcWaitTime(5);
    conf.setJournalFlushWhenQueueEmpty(true);
    //        conf.setJournalBufferedEntriesThreshold(1);
    conf.setAutoRecoveryDaemonEnabled(false);
    conf.setEnableLocalTransport(true);
    conf.setJournalSyncData(false);

    conf.setAllowLoopback(true);
    conf.setProperty("journalMaxGroupWaitMSec", 10); // default 200ms

    try (ZooKeeperClient zkc =
        ZooKeeperClient.newBuilder()
            .connectString(getZooKeeperAddress())
            .sessionTimeoutMs(getTimeout())
            .build()) {

      boolean rootExists = zkc.exists(getPath(), false) != null;

      if (!rootExists) {
        zkc.create(getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    }

    if (format) {
      BookKeeperAdmin.initNewCluster(conf);
      BookKeeperAdmin.format(conf, false, true);
    }

    startBookie(conf);
  }

  public String getZooKeeperAddress() {
    return this.zkServer.getConnectString();
  }

  public int getTimeout() {
    return 40000;
  }

  public String getPath() {
    return "/ledgers";
  }

  @Override
  public void close() throws Exception {
    for (EmbeddedServer bookie : bookies) {
      bookie.getLifecycleComponentStack().close();
    }
    try {
      if (zkServer != null) {
        zkServer.close();
      }
    } catch (Throwable t) {
    }
  }

  public String getBookKeeperMetadataURI() {
    return "zk+null://" + getZooKeeperAddress() + getPath();
  }

  public BookKeeper createClient() throws Exception {
    ClientConfiguration conf = new ClientConfiguration();
    conf.setEnableDigestTypeAutodetection(true);
    conf.setMetadataServiceUri(getBookKeeperMetadataURI());
    return BookKeeper.newBuilder(conf).build();
  }

  public ServerConfiguration getBookieConfiguration(String bookie1) {
    return configurations.get(bookie1);
  }

  void startBookie(ServerConfiguration conf) throws Exception {
    EmbeddedServer bookie = EmbeddedServer.builder(new BookieConfiguration(conf)).build();
    bookie.getLifecycleComponentStack().start();
    bookies.add(bookie);
    configurations.put(bookie.getBookieService().getServer().getBookieId().toString(), conf);
  }

  private static void stampNewCookie(
      Cookie masterCookie, List<File> journalDirectories, List<File> allLedgerDirs)
      throws BookieException, IOException {
    for (File journalDirectory : journalDirectories) {
      System.out.println("STAMPING NEW COOKIE on " + journalDirectory);
      masterCookie.writeToDirectory(journalDirectory);
    }
    for (File dir : allLedgerDirs) {
      System.out.println("STAMPING NEW COOKIE on " + dir);
      masterCookie.writeToDirectory(dir);
    }
  }
}
