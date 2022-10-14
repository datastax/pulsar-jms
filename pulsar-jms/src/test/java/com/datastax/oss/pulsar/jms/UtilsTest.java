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

import static org.junit.jupiter.api.Assertions.assertTrue;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class UtilsTest {

  @Test
  public void mapPriorityFewPartitions() {
    int numPartitions = 3;
    test(1, numPartitions, 0, 0);
    test(2, numPartitions, 0, 0);
    test(3, numPartitions, 0, 0);

    test(4, numPartitions, 1, 1);

    test(5, numPartitions, 2, 2);
    test(6, numPartitions, 2, 2);
    test(7, numPartitions, 2, 2);
    test(8, numPartitions, 2, 2);
    test(9, numPartitions, 2, 2);
  }

  @Test
  public void mapPriority4Partitions() {
    int numPartitions = 4;
    test(1, numPartitions, 0, 0);
    test(2, numPartitions, 0, 0);
    test(3, numPartitions, 0, 0);

    test(4, numPartitions, 1, 2);

    test(5, numPartitions, 3, 3);
    test(6, numPartitions, 3, 3);
    test(7, numPartitions, 3, 3);
    test(8, numPartitions, 3, 3);
    test(9, numPartitions, 3, 3);
  }

  @Test
  public void mapPriority6Partitions() {
    int numPartitions = 6;
    test(1, numPartitions, 0, 1);
    test(2, numPartitions, 0, 1);
    test(3, numPartitions, 0, 1);

    test(4, numPartitions, 2, 4);

    test(5, numPartitions, 5, 5);
    test(6, numPartitions, 5, 5);
    test(7, numPartitions, 5, 5);
    test(8, numPartitions, 5, 5);
    test(9, numPartitions, 5, 5);
  }

  @Test
  public void mapPriority8Partitions() {
    int numPartitions = 8;
    test(1, numPartitions, 0, 1);
    test(2, numPartitions, 0, 1);
    test(3, numPartitions, 0, 1);

    test(4, numPartitions, 2, 5);

    test(5, numPartitions, 6, 7);
    test(6, numPartitions, 6, 7);
    test(7, numPartitions, 6, 7);
    test(8, numPartitions, 6, 7);
    test(9, numPartitions, 6, 7);
  }

  @Test
  public void mapPriorityManyPartitions() {
    int numPartitions = 20;
    test(1, numPartitions, 0, 4);
    test(2, numPartitions, 0, 4);
    test(3, numPartitions, 0, 4);

    test(4, numPartitions, 5, 14);

    test(5, numPartitions, 15, 19);
    test(6, numPartitions, 15, 19);
    test(7, numPartitions, 15, 19);
    test(8, numPartitions, 15, 19);
    test(9, numPartitions, 15, 19);
  }

  private static void test(int priority, int numPartitions, int from, int to) {
    // run the test many times, because we are using random values
    for (int i = 0; i < numPartitions * 5; i++) {
      assertRange(Utils.mapPriorityToPartition(priority, numPartitions), from, to);
    }
  }

  private static void assertRange(int value, int from, int to) {
    assertTrue(value >= from, "value " + value + " not in range " + from + "-" + to);
    assertTrue(value <= to, "value " + value + " not in range " + from + "-" + to);
  }
}
