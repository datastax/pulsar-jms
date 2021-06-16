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

import static com.google.common.base.Charsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.util.PortManager;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
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
  List<BookieServer> bookies = new ArrayList<>();
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

  public String startBookie() throws Exception {
    return startBookie(true);
  }

  public String startBookie(boolean format) throws Exception {
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

    return startBookie(conf, null);
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
    for (BookieServer bookie : bookies) {
      bookie.shutdown();
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

  public void stopBookie(String bookie1) throws Exception {
    for (Iterator<BookieServer> it = bookies.iterator(); it.hasNext(); ) {
      BookieServer s = it.next();
      if (s.getBookieId().toString().equals(bookie1)) {
        s.shutdown();
        it.remove();
      }
    }
  }

  String startBookie(ServerConfiguration conf, String newCookie) throws Exception {
    if (newCookie != null) {
      Cookie cookie =
          Cookie.readFromRegistrationManager(
                  new RegistrationManagerImpl(newCookie), Bookie.getBookieId(conf))
              .getValue();
      stampNewCookie(
          cookie,
          Arrays.asList(Bookie.getCurrentDirectories(conf.getJournalDirs())),
          Arrays.asList(Bookie.getCurrentDirectories(conf.getLedgerDirs())));
    }
    BookieServer bookie = new BookieServer(conf);
    bookie.start();
    bookies.add(bookie);
    configurations.put(bookie.getBookieId().toString(), conf);
    return bookie.getLocalAddress().toString();
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

  private static class RegistrationManagerImpl implements RegistrationManager {

    private final String newCookie;

    public RegistrationManagerImpl(String newCookie) {
      this.newCookie = newCookie;
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException(
          "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getClusterInstanceId() throws BookieException {
      throw new UnsupportedOperationException(
          "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void registerBookie(BookieId bookieId, boolean readOnly, BookieServiceInfo serviceInfo)
        throws BookieException {
      throw new UnsupportedOperationException(
          "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void unregisterBookie(BookieId bookieId, boolean readOnly) throws BookieException {
      throw new UnsupportedOperationException(
          "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isBookieRegistered(BookieId bookieId) throws BookieException {
      throw new UnsupportedOperationException(
          "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void writeCookie(BookieId bookieId, Versioned<byte[]> cookieData)
        throws BookieException {
      throw new UnsupportedOperationException(
          "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Versioned<byte[]> readCookie(BookieId bookieId) throws BookieException {
      return new Versioned<>(newCookie.getBytes(UTF_8), new LongVersion(0));
    }

    @Override
    public void removeCookie(BookieId bookieId, Version version) throws BookieException {
      throw new UnsupportedOperationException(
          "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean prepareFormat() throws Exception {
      throw new UnsupportedOperationException(
          "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean initNewCluster() throws Exception {
      throw new UnsupportedOperationException(
          "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean format() throws Exception {
      throw new UnsupportedOperationException(
          "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean nukeExistingCluster() throws Exception {
      throw new UnsupportedOperationException(
          "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }
  }
}
