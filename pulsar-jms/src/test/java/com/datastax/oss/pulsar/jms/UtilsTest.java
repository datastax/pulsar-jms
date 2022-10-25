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
package com.datastax.oss.pulsar.jms;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

@Slf4j
public class UtilsTest {

  @Test
  public void mapPriority1Partition() {
    int numPartitions = 1;
    testNonLinearMapping(0, numPartitions, 0, 0);
    testNonLinearMapping(1, numPartitions, 0, 0);
    testNonLinearMapping(2, numPartitions, 0, 0);
    testNonLinearMapping(3, numPartitions, 0, 0);
    testNonLinearMapping(4, numPartitions, 0, 0);
    testNonLinearMapping(5, numPartitions, 0, 0);
    testNonLinearMapping(6, numPartitions, 0, 0);
    testNonLinearMapping(7, numPartitions, 0, 0);
    testNonLinearMapping(8, numPartitions, 0, 0);
    testNonLinearMapping(9, numPartitions, 0, 0);

    testLinearMapping(0, numPartitions, 0, 0);
    testLinearMapping(1, numPartitions, 0, 0);
    testLinearMapping(2, numPartitions, 0, 0);
    testLinearMapping(3, numPartitions, 0, 0);
    testLinearMapping(4, numPartitions, 0, 0);
    testLinearMapping(5, numPartitions, 0, 0);
    testLinearMapping(6, numPartitions, 0, 0);
    testLinearMapping(7, numPartitions, 0, 0);
    testLinearMapping(8, numPartitions, 0, 0);
    testLinearMapping(9, numPartitions, 0, 0);
  }

  @Test
  public void mapPriority2Partitions() {
    int numPartitions = 2;
    testNonLinearMapping(0, numPartitions, 0, 0);
    testNonLinearMapping(1, numPartitions, 0, 0);
    testNonLinearMapping(2, numPartitions, 0, 0);
    testNonLinearMapping(3, numPartitions, 0, 0);
    testNonLinearMapping(4, numPartitions, 0, 0);
    testNonLinearMapping(5, numPartitions, 1, 1);
    testNonLinearMapping(6, numPartitions, 1, 1);
    testNonLinearMapping(7, numPartitions, 1, 1);
    testNonLinearMapping(8, numPartitions, 1, 1);
    testNonLinearMapping(9, numPartitions, 1, 1);

    testLinearMapping(0, numPartitions, 0, 0);
    testLinearMapping(1, numPartitions, 0, 0);
    testLinearMapping(2, numPartitions, 0, 0);
    testLinearMapping(3, numPartitions, 0, 0);
    testLinearMapping(4, numPartitions, 0, 0);
    testLinearMapping(5, numPartitions, 1, 1);
    testLinearMapping(6, numPartitions, 1, 1);
    testLinearMapping(7, numPartitions, 1, 1);
    testLinearMapping(8, numPartitions, 1, 1);
    testLinearMapping(9, numPartitions, 1, 1);
  }

  @Test
  public void mapPriority3Partitions() {
    int numPartitions = 3;
    testNonLinearMapping(0, numPartitions, 0, 0);
    testNonLinearMapping(1, numPartitions, 0, 0);
    testNonLinearMapping(2, numPartitions, 0, 0);
    testNonLinearMapping(3, numPartitions, 0, 0);
    testNonLinearMapping(4, numPartitions, 1, 1);
    testNonLinearMapping(5, numPartitions, 2, 2);
    testNonLinearMapping(6, numPartitions, 2, 2);
    testNonLinearMapping(7, numPartitions, 2, 2);
    testNonLinearMapping(8, numPartitions, 2, 2);
    testNonLinearMapping(9, numPartitions, 2, 2);

    testLinearMapping(0, numPartitions, 0, 0);
    testLinearMapping(1, numPartitions, 0, 0);
    testLinearMapping(2, numPartitions, 0, 0);
    testLinearMapping(3, numPartitions, 0, 0);
    testLinearMapping(4, numPartitions, 1, 1);
    testLinearMapping(5, numPartitions, 2, 2);
    testLinearMapping(6, numPartitions, 2, 2);
    testLinearMapping(7, numPartitions, 2, 2);
    testLinearMapping(8, numPartitions, 2, 2);
    testLinearMapping(9, numPartitions, 2, 2);
  }

  @Test
  public void mapPriority4Partitions() {
    int numPartitions = 4;
    testNonLinearMapping(0, numPartitions, 0, 0);
    testNonLinearMapping(1, numPartitions, 0, 0);
    testNonLinearMapping(2, numPartitions, 0, 0);
    testNonLinearMapping(3, numPartitions, 0, 0);
    testNonLinearMapping(4, numPartitions, 1, 2);
    testNonLinearMapping(5, numPartitions, 3, 3);
    testNonLinearMapping(6, numPartitions, 3, 3);
    testNonLinearMapping(7, numPartitions, 3, 3);
    testNonLinearMapping(8, numPartitions, 3, 3);
    testNonLinearMapping(9, numPartitions, 3, 3);

    testLinearMapping(0, numPartitions, 0, 0);
    testLinearMapping(1, numPartitions, 0, 0);
    testLinearMapping(2, numPartitions, 0, 0);
    testLinearMapping(3, numPartitions, 1, 1);
    testLinearMapping(4, numPartitions, 1, 1);
    testLinearMapping(5, numPartitions, 2, 2);
    testLinearMapping(6, numPartitions, 2, 2);
    testLinearMapping(7, numPartitions, 2, 2);
    testLinearMapping(8, numPartitions, 3, 3);
    testLinearMapping(9, numPartitions, 3, 3);
  }

  @Test
  public void mapPriority5Partitions() {
    int numPartitions = 5;
    testNonLinearMapping(0, numPartitions, 0, 1);
    testNonLinearMapping(1, numPartitions, 0, 1);
    testNonLinearMapping(2, numPartitions, 0, 1);
    testNonLinearMapping(3, numPartitions, 0, 1);
    testNonLinearMapping(4, numPartitions, 2, 4);
    testNonLinearMapping(5, numPartitions, 4, 4);
    testNonLinearMapping(6, numPartitions, 4, 4);
    testNonLinearMapping(7, numPartitions, 4, 4);
    testNonLinearMapping(8, numPartitions, 4, 4);
    testNonLinearMapping(9, numPartitions, 4, 4);

    testLinearMapping(0, numPartitions, 0, 0);
    testLinearMapping(1, numPartitions, 0, 0);
    testLinearMapping(2, numPartitions, 1, 1);
    testLinearMapping(3, numPartitions, 1, 1);
    testLinearMapping(4, numPartitions, 2, 2);
    testLinearMapping(5, numPartitions, 2, 2);
    testLinearMapping(6, numPartitions, 3, 3);
    testLinearMapping(7, numPartitions, 3, 3);
    testLinearMapping(8, numPartitions, 4, 4);
    testLinearMapping(9, numPartitions, 4, 4);
  }

  @Test
  public void mapPriority6Partitions() {
    int numPartitions = 6;
    testNonLinearMapping(0, numPartitions, 0, 1);
    testNonLinearMapping(1, numPartitions, 0, 1);
    testNonLinearMapping(2, numPartitions, 0, 1);
    testNonLinearMapping(3, numPartitions, 0, 1);
    testNonLinearMapping(4, numPartitions, 2, 4);
    testNonLinearMapping(5, numPartitions, 5, 5);
    testNonLinearMapping(6, numPartitions, 5, 5);
    testNonLinearMapping(7, numPartitions, 5, 5);
    testNonLinearMapping(8, numPartitions, 5, 5);
    testNonLinearMapping(9, numPartitions, 5, 5);

    testLinearMapping(0, numPartitions, 0, 0);
    testLinearMapping(1, numPartitions, 0, 0);
    testLinearMapping(2, numPartitions, 1, 1);
    testLinearMapping(3, numPartitions, 1, 1);
    testLinearMapping(4, numPartitions, 2, 2);
    testLinearMapping(5, numPartitions, 3, 3);
    testLinearMapping(6, numPartitions, 3, 3);
    testLinearMapping(7, numPartitions, 4, 4);
    testLinearMapping(8, numPartitions, 4, 4);
    testLinearMapping(9, numPartitions, 5, 5);
  }

  @Test
  public void mapPriority7Partitions() {
    int numPartitions = 7;
    testNonLinearMapping(0, numPartitions, 0, 1);
    testNonLinearMapping(1, numPartitions, 0, 1);
    testNonLinearMapping(2, numPartitions, 0, 1);
    testNonLinearMapping(3, numPartitions, 0, 1);
    testNonLinearMapping(4, numPartitions, 2, 5);
    testNonLinearMapping(5, numPartitions, 6, 6);
    testNonLinearMapping(6, numPartitions, 6, 6);
    testNonLinearMapping(7, numPartitions, 6, 6);
    testNonLinearMapping(8, numPartitions, 6, 6);
    testNonLinearMapping(9, numPartitions, 6, 6);

    testLinearMapping(0, numPartitions, 0, 0);
    testLinearMapping(1, numPartitions, 0, 0);
    testLinearMapping(2, numPartitions, 1, 1);
    testLinearMapping(3, numPartitions, 2, 2);
    testLinearMapping(4, numPartitions, 2, 2);
    testLinearMapping(5, numPartitions, 3, 3);
    testLinearMapping(6, numPartitions, 4, 4);
    testLinearMapping(7, numPartitions, 4, 4);
    testLinearMapping(8, numPartitions, 5, 5);
    testLinearMapping(9, numPartitions, 6, 6);
  }

  @Test
  public void mapPriority8Partitions() {
    int numPartitions = 8;
    testNonLinearMapping(0, numPartitions, 0, 1);
    testNonLinearMapping(1, numPartitions, 0, 1);
    testNonLinearMapping(2, numPartitions, 0, 1);
    testNonLinearMapping(3, numPartitions, 0, 1);
    testNonLinearMapping(4, numPartitions, 2, 5);
    testNonLinearMapping(5, numPartitions, 6, 7);
    testNonLinearMapping(6, numPartitions, 6, 7);
    testNonLinearMapping(7, numPartitions, 6, 7);
    testNonLinearMapping(8, numPartitions, 6, 7);
    testNonLinearMapping(9, numPartitions, 6, 7);

    testLinearMapping(0, numPartitions, 0, 0);
    testLinearMapping(1, numPartitions, 0, 0);
    testLinearMapping(2, numPartitions, 1, 1);
    testLinearMapping(3, numPartitions, 2, 2);
    testLinearMapping(4, numPartitions, 3, 3);
    testLinearMapping(5, numPartitions, 4, 4);
    testLinearMapping(6, numPartitions, 4, 4);
    testLinearMapping(7, numPartitions, 5, 5);
    testLinearMapping(8, numPartitions, 6, 6);
    testLinearMapping(9, numPartitions, 7, 7);
  }

  @Test
  public void mapPriority9Partitions() {
    int numPartitions = 9;
    testNonLinearMapping(0, numPartitions, 0, 2);
    testNonLinearMapping(1, numPartitions, 0, 2);
    testNonLinearMapping(2, numPartitions, 0, 2);
    testNonLinearMapping(3, numPartitions, 0, 2);
    testNonLinearMapping(4, numPartitions, 3, 7);
    testNonLinearMapping(5, numPartitions, 7, 8);
    testNonLinearMapping(6, numPartitions, 7, 8);
    testNonLinearMapping(7, numPartitions, 7, 8);
    testNonLinearMapping(8, numPartitions, 7, 8);
    testNonLinearMapping(9, numPartitions, 7, 8);

    testLinearMapping(0, numPartitions, 0, 0);
    testLinearMapping(1, numPartitions, 0, 0);
    testLinearMapping(2, numPartitions, 1, 1);
    testLinearMapping(3, numPartitions, 2, 2);
    testLinearMapping(4, numPartitions, 3, 3);
    testLinearMapping(5, numPartitions, 4, 4);
    testLinearMapping(6, numPartitions, 5, 5);
    testLinearMapping(7, numPartitions, 6, 6);
    testLinearMapping(8, numPartitions, 7, 7);
    testLinearMapping(9, numPartitions, 8, 8);
  }

  @Test
  public void mapPriority10Partitions() {
    int numPartitions = 10;
    testNonLinearMapping(0, numPartitions, 0, 2);
    testNonLinearMapping(1, numPartitions, 0, 2);
    testNonLinearMapping(2, numPartitions, 0, 2);
    testNonLinearMapping(3, numPartitions, 0, 2);
    testNonLinearMapping(4, numPartitions, 3, 7);
    testNonLinearMapping(5, numPartitions, 8, 9);
    testNonLinearMapping(6, numPartitions, 8, 9);
    testNonLinearMapping(7, numPartitions, 8, 9);
    testNonLinearMapping(8, numPartitions, 8, 9);
    testNonLinearMapping(9, numPartitions, 8, 9);

    testLinearMapping(0, numPartitions, 0, 0);
    testLinearMapping(1, numPartitions, 1, 1);
    testLinearMapping(2, numPartitions, 2, 2);
    testLinearMapping(3, numPartitions, 3, 3);
    testLinearMapping(4, numPartitions, 4, 4);
    testLinearMapping(5, numPartitions, 5, 5);
    testLinearMapping(6, numPartitions, 6, 6);
    testLinearMapping(7, numPartitions, 7, 7);
    testLinearMapping(8, numPartitions, 8, 8);
    testLinearMapping(9, numPartitions, 9, 9);
  }

  @Test
  public void mapPriority18Partitions() {
    int numPartitions = 18;
    testNonLinearMapping(0, numPartitions, 0, 4);
    testNonLinearMapping(1, numPartitions, 0, 4);
    testNonLinearMapping(2, numPartitions, 0, 4);
    testNonLinearMapping(3, numPartitions, 0, 4);
    testNonLinearMapping(4, numPartitions, 5, 13);
    testNonLinearMapping(5, numPartitions, 14, 17);
    testNonLinearMapping(6, numPartitions, 14, 17);
    testNonLinearMapping(7, numPartitions, 14, 17);
    testNonLinearMapping(8, numPartitions, 14, 17);
    testNonLinearMapping(9, numPartitions, 14, 17);

    testLinearMapping(0, numPartitions, 0, 1);
    testLinearMapping(1, numPartitions, 1, 2);
    testLinearMapping(2, numPartitions, 3, 4);
    testLinearMapping(3, numPartitions, 5, 6);
    testLinearMapping(4, numPartitions, 7, 8);
    testLinearMapping(5, numPartitions, 9, 10);
    testLinearMapping(6, numPartitions, 10, 11);
    testLinearMapping(7, numPartitions, 12, 13);
    testLinearMapping(8, numPartitions, 14, 15);
    testLinearMapping(9, numPartitions, 16, 17);
  }

  @Test
  public void mapPriority20Partitions() {
    int numPartitions = 20;
    testNonLinearMapping(0, numPartitions, 0, 4);
    testNonLinearMapping(1, numPartitions, 0, 4);
    testNonLinearMapping(2, numPartitions, 0, 4);
    testNonLinearMapping(3, numPartitions, 0, 4);
    testNonLinearMapping(4, numPartitions, 5, 14);
    testNonLinearMapping(5, numPartitions, 15, 19);
    testNonLinearMapping(6, numPartitions, 15, 19);
    testNonLinearMapping(7, numPartitions, 15, 19);
    testNonLinearMapping(8, numPartitions, 15, 19);
    testNonLinearMapping(9, numPartitions, 15, 19);

    testLinearMapping(0, numPartitions, 0, 1);
    testLinearMapping(1, numPartitions, 2, 3);
    testLinearMapping(2, numPartitions, 4, 5);
    testLinearMapping(3, numPartitions, 6, 7);
    testLinearMapping(4, numPartitions, 8, 9);
    testLinearMapping(5, numPartitions, 10, 11);
    testLinearMapping(6, numPartitions, 12, 13);
    testLinearMapping(7, numPartitions, 14, 15);
    testLinearMapping(8, numPartitions, 16, 17);
    testLinearMapping(9, numPartitions, 18, 19);
  }

  @Test
  public void verifyDecodeBase64EncodedPathConfigsToFilesForJKSTruststore() throws Exception {
    String storePassword = "storePassword";
    String storeType = "jks";
    try (ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream()) {
      KeyStore keystore = KeyStore.getInstance(storeType);
      keystore.load(null, storePassword.toCharArray());
      keystore.store(byteOutputStream, storePassword.toCharArray());
      String encodedKeyStore = "base64:" + Base64.getEncoder().encodeToString(byteOutputStream.toByteArray());

      HashMap<String, Object> config = new HashMap<>();
      config.put("tlsTrustStorePath", encodedKeyStore);
      Utils.writeEncodedPathConfigsToTempFiles(config);

      String path = (String) config.get("tlsTrustStorePath");

      try(FileInputStream inputStream = new FileInputStream(path)) {
        byte[] storedFile = new byte[byteOutputStream.size()];
        IOUtils.readFully(inputStream, storedFile);
        assertArrayEquals(byteOutputStream.toByteArray(), storedFile);
      }
    }
  }

  @Test
  public void verifyByteArrayPathConfigsToFilesForJKSTruststore() throws Exception {
    String storePassword = "storePassword";
    String storeType = "jks";
    try (ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream()) {
      KeyStore keystore = KeyStore.getInstance(storeType);
      keystore.load(null, storePassword.toCharArray());
      keystore.store(byteOutputStream, storePassword.toCharArray());

      HashMap<String, Object> config = new HashMap<>();
      config.put("tlsTrustStorePath", byteOutputStream.toByteArray());
      Utils.writeEncodedPathConfigsToTempFiles(config);

      String path = (String) config.get("tlsTrustStorePath");

      try(FileInputStream inputStream = new FileInputStream(path)) {
        byte[] storedFile = new byte[byteOutputStream.size()];
        IOUtils.readFully(inputStream, storedFile);
        assertArrayEquals(byteOutputStream.toByteArray(), storedFile);
      }
    }
  }

  private static void testNonLinearMapping(int priority, int numPartitions, int from, int to) {
    test(priority, numPartitions, from, to, false);
  }

  private static void testLinearMapping(int priority, int numPartitions, int from, int to) {
    test(priority, numPartitions, from, to, true);
  }

  private static void test(
      int priority, int numPartitions, int from, int to, boolean linearMapping) {
    // run the test many times, because we are using random values
    Set<Integer> values = new HashSet<>();
    for (int i = 0; i < numPartitions * 100; i++) {
      int res = Utils.mapPriorityToPartition(priority, numPartitions, linearMapping);
      values.add(res);
    }
    assertTrue(
        values.contains(from), "value " + from + " was never returned, seen values " + values);
    assertTrue(values.contains(to), "value " + to + " was never returned, seen values " + values);
    for (int value : values) {
      assertRange(value, from, to);
    }
  }

  private static void assertRange(int value, int from, int to) {
    assertTrue(value >= from, "value " + value + " not in range " + from + "-" + to);
    assertTrue(value <= to, "value " + value + " not in range " + from + "-" + to);
  }
}
