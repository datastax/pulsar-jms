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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Map;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TopicMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for PriorityGroupPartitionRouter. */
public class PriorityGroupPartitionRouterTest {

  @Mock private Message<?> message;

  @Mock private TopicMetadata metadata;

  private PriorityGroupPartitionRouter router;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    router = new PriorityGroupPartitionRouter();
  }

  // ==================== Basic Priority Routing Tests ====================

  @Test
  public void testLowPriorityRoutesToPartition0() {
    // Priority 0-4 should route to partition 0
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("2");
    when(message.getKey()).thenReturn(null);

    int partition = router.choosePartition(message, metadata);

    assertEquals(0, partition, "Priority 2 should route to partition 0");
  }

  @Test
  public void testHighPriorityRoutesToPartition1() {
    // Priority 5-9 should route to partition 1
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("9");
    when(message.getKey()).thenReturn(null);

    int partition = router.choosePartition(message, metadata);

    assertEquals(1, partition, "Priority 9 should route to partition 1");
  }

  @Test
  public void testAllLowPriorities() {
    // Test all low priorities (0-4)
    when(metadata.numPartitions()).thenReturn(2);
    when(message.getKey()).thenReturn(null);

    for (int priority = 0; priority <= 4; priority++) {
      when(message.hasProperty("JMSPriority")).thenReturn(true);
      when(message.getProperty("JMSPriority")).thenReturn(String.valueOf(priority));

      int partition = router.choosePartition(message, metadata);

      assertEquals(
          0, partition, "Priority " + priority + " should route to partition 0 (LOW partition)");
    }
  }

  @Test
  public void testAllHighPriorities() {
    // Test all high priorities (5-9)
    when(metadata.numPartitions()).thenReturn(2);
    when(message.getKey()).thenReturn(null);

    for (int priority = 5; priority <= 9; priority++) {
      when(message.hasProperty("JMSPriority")).thenReturn(true);
      when(message.getProperty("JMSPriority")).thenReturn(String.valueOf(priority));

      int partition = router.choosePartition(message, metadata);

      assertEquals(
          1, partition, "Priority " + priority + " should route to partition 1 (HIGH partition)");
    }
  }

  // ==================== Boundary Cases Tests ====================

  @Test
  public void testPriority4RoutesToLowPartition() {
    // Priority 4 is the boundary - should go to LOW (0-4 range)
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("4");
    when(message.getKey()).thenReturn(null);

    int partition = router.choosePartition(message, metadata);

    assertEquals(0, partition, "Priority 4 should route to partition 0 (boundary case)");
  }

  @Test
  public void testPriority5RoutesToHighPartition() {
    // Priority 5 is the threshold - should go to HIGH (5-9 range)
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("5");
    when(message.getKey()).thenReturn(null);

    int partition = router.choosePartition(message, metadata);

    assertEquals(1, partition, "Priority 5 should route to partition 1 (threshold)");
  }

  @Test
  public void testMissingPriorityUsesDefault() {
    // Missing priority should default to 4 → LOW partition
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(false);
    when(message.getKey()).thenReturn(null);

    int partition = router.choosePartition(message, metadata);

    assertEquals(0, partition, "Missing priority should default to 4 and route to partition 0");
  }

  @Test
  public void testInvalidPriorityUsesDefault() {
    // Invalid priority should default to 4 → LOW partition
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("invalid");
    when(message.getKey()).thenReturn(null);

    int partition = router.choosePartition(message, metadata);

    assertEquals(0, partition, "Invalid priority should default to 4 and route to partition 0");
  }

  @Test
  public void testNegativePriorityUsesDefault() {
    // Negative priority should default to 4 → LOW partition
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("-1");
    when(message.getKey()).thenReturn(null);

    int partition = router.choosePartition(message, metadata);

    assertEquals(0, partition, "Negative priority should default to 4 and route to partition 0");
  }

  @Test
  public void testPriorityAbove9UsesDefault() {
    // Priority > 9 should default to 4 → LOW partition
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("10");
    when(message.getKey()).thenReturn(null);

    int partition = router.choosePartition(message, metadata);

    assertEquals(0, partition, "Priority > 9 should default to 4 and route to partition 0");
  }

  // ==================== JMSXGroupID Affinity Tests ====================

  @Test
  public void testGroupAffinityMaintained() {
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);

    // First message with group - high priority
    when(message.getProperty("JMSPriority")).thenReturn("9");
    when(message.getKey()).thenReturn("ORDER-123");

    int partition1 = router.choosePartition(message, metadata);
    assertEquals(1, partition1, "First message should route to partition 1 (high priority)");

    // Second message with same group - low priority (should still go to partition 1)
    when(message.getProperty("JMSPriority")).thenReturn("2");
    when(message.getKey()).thenReturn("ORDER-123");

    int partition2 = router.choosePartition(message, metadata);
    assertEquals(
        partition1, partition2, "Same group should maintain affinity despite different priority");
  }

  @Test
  public void testDifferentGroupsCanHaveDifferentPartitions() {
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);

    // Group A - low priority
    when(message.getProperty("JMSPriority")).thenReturn("2");
    when(message.getKey()).thenReturn("GROUP-A");
    int partitionA = router.choosePartition(message, metadata);

    // Group B - high priority
    when(message.getProperty("JMSPriority")).thenReturn("9");
    when(message.getKey()).thenReturn("GROUP-B");
    int partitionB = router.choosePartition(message, metadata);

    assertNotEquals(partitionA, partitionB, "Different groups with different priorities");
  }

  @Test
  public void testEmptyGroupIdTreatedAsNoGroup() {
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("2");
    when(message.getKey()).thenReturn("");

    int partition = router.choosePartition(message, metadata);

    assertEquals(0, partition, "Empty group ID should be treated as no group");
  }

  @Test
  public void testMultipleMessagesWithSameGroup() {
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("7");
    when(message.getKey()).thenReturn("ORDER-456");

    // Send 5 messages with same group
    int firstPartition = router.choosePartition(message, metadata);

    for (int i = 0; i < 4; i++) {
      int partition = router.choosePartition(message, metadata);
      assertEquals(
          firstPartition, partition, "All messages with same group should go to same partition");
    }
  }

  // ==================== Configuration Tests ====================

  @Test
  public void testCustomPriorityThreshold() {
    // Create router with threshold 7
    PriorityGroupPartitionRouter customRouter = new PriorityGroupPartitionRouter(7);

    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getKey()).thenReturn(null);

    // Priority 6 should go to LOW (threshold is 7)
    when(message.getProperty("JMSPriority")).thenReturn("6");
    int partition = customRouter.choosePartition(message, metadata);
    assertEquals(0, partition, "Priority 6 should route to partition 0 with threshold 7");

    // Priority 7 should go to HIGH
    when(message.getProperty("JMSPriority")).thenReturn("7");
    partition = customRouter.choosePartition(message, metadata);
    assertEquals(1, partition, "Priority 7 should route to partition 1 with threshold 7");
  }

  @Test
  public void testInvalidThresholdThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new PriorityGroupPartitionRouter(0),
        "Threshold 0 should throw exception");

    assertThrows(
        IllegalArgumentException.class,
        () -> new PriorityGroupPartitionRouter(10),
        "Threshold 10 should throw exception");

    assertThrows(
        IllegalArgumentException.class,
        () -> new PriorityGroupPartitionRouter(-1),
        "Negative threshold should throw exception");
  }

  @Test
  public void testGetPriorityThreshold() {
    PriorityGroupPartitionRouter defaultRouter = new PriorityGroupPartitionRouter();
    assertEquals(5, defaultRouter.getPriorityThreshold(), "Default threshold should be 5");

    PriorityGroupPartitionRouter customRouter = new PriorityGroupPartitionRouter(7);
    assertEquals(7, customRouter.getPriorityThreshold(), "Custom threshold should be 7");
  }

  // ==================== Statistics Tests ====================

  @Test
  public void testStatisticsTracking() {
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);

    // Send 3 low priority messages
    when(message.getProperty("JMSPriority")).thenReturn("2");
    when(message.getKey()).thenReturn(null);
    router.choosePartition(message, metadata);
    router.choosePartition(message, metadata);
    router.choosePartition(message, metadata);

    // Send 2 high priority messages
    when(message.getProperty("JMSPriority")).thenReturn("9");
    router.choosePartition(message, metadata);
    router.choosePartition(message, metadata);

    Map<String, Object> stats = router.getStats();

    assertEquals(5L, stats.get("totalMessagesRouted"), "Should track total messages");
    assertEquals(0, stats.get("totalGroups"), "Should track groups");

    @SuppressWarnings("unchecked")
    Map<String, Long> partitionCounts = (Map<String, Long>) stats.get("partitionCounts");
    assertEquals(3L, partitionCounts.get("partition0"), "Should track partition 0 count");
    assertEquals(2L, partitionCounts.get("partition1"), "Should track partition 1 count");
  }

  @Test
  public void testGroupStatistics() {
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("5");

    // Send messages with 3 different groups
    when(message.getKey()).thenReturn("GROUP-1");
    router.choosePartition(message, metadata);

    when(message.getKey()).thenReturn("GROUP-2");
    router.choosePartition(message, metadata);

    when(message.getKey()).thenReturn("GROUP-3");
    router.choosePartition(message, metadata);

    // Send another message with GROUP-1 (should apply affinity)
    when(message.getKey()).thenReturn("GROUP-1");
    router.choosePartition(message, metadata);

    Map<String, Object> stats = router.getStats();

    assertEquals(3, stats.get("totalGroups"), "Should track 3 unique groups");
    assertEquals(1L, stats.get("groupAffinityApplied"), "Should track affinity applications");
  }

  @Test
  public void testGetGroupCount() {
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("5");

    assertEquals(0, router.getGroupCount(), "Initial group count should be 0");

    when(message.getKey()).thenReturn("GROUP-1");
    router.choosePartition(message, metadata);
    assertEquals(1, router.getGroupCount(), "Group count should be 1");

    when(message.getKey()).thenReturn("GROUP-2");
    router.choosePartition(message, metadata);
    assertEquals(2, router.getGroupCount(), "Group count should be 2");
  }

  @Test
  public void testClearGroupMappings() {
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("5");
    when(message.getKey()).thenReturn("GROUP-1");

    router.choosePartition(message, metadata);
    assertEquals(1, router.getGroupCount(), "Should have 1 group");

    router.clearGroupMappings();
    assertEquals(0, router.getGroupCount(), "Group count should be 0 after clear");
  }

  @Test
  public void testResetStats() {
    when(metadata.numPartitions()).thenReturn(2);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("5");
    when(message.getKey()).thenReturn(null);

    router.choosePartition(message, metadata);

    Map<String, Object> stats = router.getStats();
    assertEquals(1L, stats.get("totalMessagesRouted"), "Should have 1 message");

    router.resetStats();

    stats = router.getStats();
    assertEquals(0L, stats.get("totalMessagesRouted"), "Stats should be reset");
  }

  // ==================== Multi-Partition Tests (Future) ====================

  @Test
  public void testSinglePartitionAlwaysReturnsZero() {
    when(metadata.numPartitions()).thenReturn(1);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getKey()).thenReturn(null);

    // Test all priorities
    for (int priority = 0; priority <= 9; priority++) {
      when(message.getProperty("JMSPriority")).thenReturn(String.valueOf(priority));
      int partition = router.choosePartition(message, metadata);
      assertEquals(0, partition, "Single partition should always return 0");
    }
  }

  @Test
  public void testThreePartitionDistribution() {
    when(metadata.numPartitions()).thenReturn(3);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getKey()).thenReturn(null);

    // Priority 0-2 → partition 0
    when(message.getProperty("JMSPriority")).thenReturn("1");
    assertEquals(0, router.choosePartition(message, metadata));

    // Priority 3-5 → partition 1
    when(message.getProperty("JMSPriority")).thenReturn("4");
    assertEquals(1, router.choosePartition(message, metadata));

    // Priority 6-9 → partition 2
    when(message.getProperty("JMSPriority")).thenReturn("8");
    assertEquals(2, router.choosePartition(message, metadata));
  }

  @Test
  public void testFivePartitionDistribution() {
    when(metadata.numPartitions()).thenReturn(5);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getKey()).thenReturn(null);

    // Test distribution across 5 partitions
    // 0-1 → 0, 2-3 → 1, 4-5 → 2, 6-7 → 3, 8-9 → 4
    int[] expectedPartitions = {0, 0, 1, 1, 2, 2, 3, 3, 4, 4};

    for (int priority = 0; priority <= 9; priority++) {
      when(message.getProperty("JMSPriority")).thenReturn(String.valueOf(priority));
      int partition = router.choosePartition(message, metadata);
      assertEquals(
          expectedPartitions[priority],
          partition,
          "Priority " + priority + " should route to partition " + expectedPartitions[priority]);
    }
  }

  @Test
  public void testTenPartitionDistribution() {
    when(metadata.numPartitions()).thenReturn(10);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getKey()).thenReturn(null);

    // With 10 partitions, each priority should map to its own partition
    for (int priority = 0; priority <= 9; priority++) {
      when(message.getProperty("JMSPriority")).thenReturn(String.valueOf(priority));
      int partition = router.choosePartition(message, metadata);
      assertEquals(
          priority, partition, "Priority " + priority + " should route to partition " + priority);
    }
  }

  @Test
  public void testGroupAffinityWithMultiplePartitions() {
    when(metadata.numPartitions()).thenReturn(5);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("7");
    when(message.getKey()).thenReturn("ORDER-789");

    // First message establishes partition
    int firstPartition = router.choosePartition(message, metadata);

    // Subsequent messages with same group should go to same partition
    for (int i = 0; i < 10; i++) {
      int partition = router.choosePartition(message, metadata);
      assertEquals(
          firstPartition, partition, "Group affinity should work with multiple partitions");
    }
  }

  @Test
  public void testInvalidPartitionCountDefaultsToZero() {
    when(metadata.numPartitions()).thenReturn(0);
    when(message.hasProperty("JMSPriority")).thenReturn(true);
    when(message.getProperty("JMSPriority")).thenReturn("5");
    when(message.getKey()).thenReturn(null);

    int partition = router.choosePartition(message, metadata);

    assertEquals(0, partition, "Invalid partition count should default to partition 0");
  }
}

// Made with Bob
