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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;

/**
 * A custom MessageRouter implementation that routes messages to partitions based on JMS priority
 * and maintains JMSXGroupID affinity for message ordering.
 *
 * <p>This router supports flexible partition strategies:
 *
 * <ul>
 *   <li><b>2 partitions (Phase 1):</b> LOW (0-4) → partition 0, HIGH (5-9) → partition 1
 *   <li><b>3+ partitions (Future):</b> Priorities distributed evenly across partitions
 * </ul>
 *
 * <p>When a message has a JMSXGroupID (message key), the router ensures all messages with the same
 * group ID are routed to the same partition, maintaining ordering within the group. The partition
 * is determined by the priority of the first message in the group.
 *
 * <p><b>Thread Safety:</b> This class is thread-safe and can be used concurrently by multiple
 * producer threads.
 *
 * <p><b>Configuration:</b>
 *
 * <pre>
 * // Default: 2 partitions with threshold 5
 * MessageRouter router = new PriorityGroupPartitionRouter();
 *
 * // Custom threshold for 2 partitions
 * MessageRouter router = new PriorityGroupPartitionRouter(7);
 *
 * // Future: Support for more partitions (auto-detected from topic metadata)
 * MessageRouter router = new PriorityGroupPartitionRouter();
 * // Will automatically distribute 10 priorities across N partitions
 * </pre>
 *
 * <p><b>Usage Example:</b>
 *
 * <pre>
 * ProducerBuilder<byte[]> builder = pulsarClient.newProducer()
 *     .topic("persistent://public/default/my-topic")
 *     .messageRouter(new PriorityGroupPartitionRouter())
 *     .enableBatching(false);  // Disable batching for priority routing
 * </pre>
 *
 * @see MessageRouter
 * @see PulsarMessage#readJMSPriority(Message)
 * @since 1.0.0
 */
@Slf4j
public class PriorityGroupPartitionRouter implements MessageRouter {

  /** Default priority threshold for 2-partition setup. */
  private static final int DEFAULT_PRIORITY_THRESHOLD = 5;

  /** Default JMS priority when not specified or invalid. */
  private static final int DEFAULT_JMS_PRIORITY = 4;

  /** Number of JMS priority levels (0-9). */
  private static final int JMS_PRIORITY_LEVELS = 10;

  /**
   * The priority threshold for 2-partition setup. Messages with priority >= threshold are routed to
   * partition 1, others to partition 0. Only used when topic has exactly 2 partitions.
   */
  private final int priorityThreshold;

  /**
   * Thread-safe map tracking JMSXGroupID to partition assignments. Once a group is assigned to a
   * partition, all subsequent messages with that group ID will route to the same partition.
   */
  private final Map<String, Integer> groupPartitionMap = new ConcurrentHashMap<>();

  /** Statistics: Total number of messages routed. */
  private final AtomicLong totalMessagesRouted = new AtomicLong(0);

  /** Statistics: Number of messages routed per partition (dynamically sized). */
  private final Map<Integer, AtomicLong> partitionCounts = new ConcurrentHashMap<>();

  /** Statistics: Number of messages with group affinity applied. */
  private final AtomicLong groupAffinityApplied = new AtomicLong(0);

  /**
   * Creates a new PriorityGroupPartitionRouter with the default priority threshold of 5.
   *
   * <p>For 2-partition topics: Messages with priority 0-4 route to partition 0 (LOW), messages with
   * priority 5-9 route to partition 1 (HIGH).
   *
   * <p>For 3+ partition topics: Priorities are distributed evenly across all partitions.
   */
  public PriorityGroupPartitionRouter() {
    this(DEFAULT_PRIORITY_THRESHOLD);
  }

  /**
   * Creates a new PriorityGroupPartitionRouter with a custom priority threshold.
   *
   * <p>The threshold is only used for 2-partition topics. For 3+ partitions, priorities are
   * distributed evenly regardless of the threshold.
   *
   * @param priorityThreshold the priority threshold for 2-partition setup (must be between 1 and
   *     9). Messages with priority >= threshold route to partition 1, others to partition 0.
   * @throws IllegalArgumentException if threshold is not between 1 and 9
   */
  public PriorityGroupPartitionRouter(int priorityThreshold) {
    if (priorityThreshold < 1 || priorityThreshold > 9) {
      throw new IllegalArgumentException(
          "Priority threshold must be between 1 and 9, got: " + priorityThreshold);
    }
    this.priorityThreshold = priorityThreshold;
    log.info(
        "PriorityGroupPartitionRouter initialized with threshold: {} "
            + "(2-partition mode: 0-{} → partition 0, {}-9 → partition 1; "
            + "3+ partitions: even distribution)",
        priorityThreshold,
        priorityThreshold - 1,
        priorityThreshold);
  }

  /**
   * Chooses the partition for a message based on its JMS priority and JMSXGroupID.
   *
   * <p>Routing logic:
   *
   * <ol>
   *   <li>Read JMS priority from message properties (default to 4 if missing/invalid)
   *   <li>Check if message has JMSXGroupID (message key)
   *   <li>If group exists in map, return existing partition (affinity)
   *   <li>If new group or no group, determine partition based on priority and partition count
   *   <li>Store group → partition mapping for future messages
   * </ol>
   *
   * <p><b>Partition Selection Strategy:</b>
   *
   * <ul>
   *   <li><b>2 partitions:</b> Use threshold-based routing (0-4 → 0, 5-9 → 1)
   *   <li><b>3+ partitions:</b> Distribute priorities evenly (priority % numPartitions)
   * </ul>
   *
   * @param message the Pulsar message to route
   * @param metadata topic metadata containing partition information
   * @return the partition index (0 to numPartitions-1)
   */
  @Override
  public int choosePartition(Message<?> message, TopicMetadata metadata) {
    totalMessagesRouted.incrementAndGet();

    int numPartitions = metadata.numPartitions();

    // Read JMS priority from message properties
    int priority = readPriority(message);

    // Read JMSXGroupID from message key
    String groupId = message.getKey();

    // Determine preferred partition based on priority and partition count
    int preferredPartition = determinePartitionByPriority(priority, numPartitions);

    // Apply group affinity if group ID exists
    int selectedPartition;
    if (groupId != null && !groupId.isEmpty()) {
      selectedPartition = getPartitionForGroup(groupId, preferredPartition);
    } else {
      selectedPartition = preferredPartition;
    }

    // Update statistics
    partitionCounts.computeIfAbsent(selectedPartition, k -> new AtomicLong(0)).incrementAndGet();

    if (log.isDebugEnabled()) {
      log.debug(
          "Routed message: priority={}, groupId={}, partition={}/{}, messageId={}",
          priority,
          groupId != null ? groupId : "none",
          selectedPartition,
          numPartitions,
          message.getMessageId());
    }

    return selectedPartition;
  }

  /**
   * Reads the JMS priority from the message properties.
   *
   * @param message the Pulsar message
   * @return the JMS priority (0-9), or DEFAULT_JMS_PRIORITY if not set or invalid
   */
  private int readPriority(Message<?> message) {
    return PulsarMessage.readJMSPriority(message);
  }

  /**
   * Determines the partition based on message priority and number of partitions.
   *
   * <p><b>Strategy:</b>
   *
   * <ul>
   *   <li><b>2 partitions:</b> Threshold-based (priority < threshold → 0, else → 1)
   *   <li><b>3+ partitions:</b> Even distribution (priority * numPartitions / 10)
   * </ul>
   *
   * @param priority the JMS priority (0-9)
   * @param numPartitions the number of partitions in the topic
   * @return the partition index (0 to numPartitions-1)
   */
  private int determinePartitionByPriority(int priority, int numPartitions) {
    if (numPartitions <= 0) {
      log.warn("Invalid partition count: {}, defaulting to partition 0", numPartitions);
      return 0;
    }

    if (numPartitions == 1) {
      // Single partition - no routing needed
      return 0;
    }

    if (numPartitions == 2) {
      // Phase 1: Two-partition threshold-based routing
      return priority < priorityThreshold ? 0 : 1;
    }

    // Future: Multi-partition even distribution
    // Distribute 10 priority levels across N partitions
    // Priority 0-9 maps to partitions 0 to N-1
    // Example with 5 partitions: 0-1→0, 2-3→1, 4-5→2, 6-7→3, 8-9→4
    int partition = (priority * numPartitions) / JMS_PRIORITY_LEVELS;

    // Ensure we don't exceed partition bounds (edge case for priority 9)
    if (partition >= numPartitions) {
      partition = numPartitions - 1;
    }

    return partition;
  }

  /**
   * Gets or assigns a partition for a message group (JMSXGroupID).
   *
   * <p>If the group already exists in the map, returns the existing partition to maintain affinity.
   * If it's a new group, assigns it to the preferred partition and stores the mapping.
   *
   * @param groupId the JMSXGroupID (message key)
   * @param preferredPartition the partition determined by priority
   * @return the partition for this group
   */
  private int getPartitionForGroup(String groupId, int preferredPartition) {
    Integer existingPartition = groupPartitionMap.get(groupId);

    if (existingPartition != null) {
      // Group already exists, use existing partition for affinity
      groupAffinityApplied.incrementAndGet();

      if (log.isTraceEnabled()) {
        log.trace(
            "Group affinity applied: groupId={}, partition={} (preferred was {})",
            groupId,
            existingPartition,
            preferredPartition);
      }

      return existingPartition;
    }

    // New group, assign to preferred partition
    groupPartitionMap.put(groupId, preferredPartition);

    if (log.isDebugEnabled()) {
      log.debug(
          "New group assigned: groupId={}, partition={}, totalGroups={}",
          groupId,
          preferredPartition,
          groupPartitionMap.size());
    }

    return preferredPartition;
  }

  /**
   * Returns statistics about the router's operation.
   *
   * <p>Statistics include:
   *
   * <ul>
   *   <li>totalMessagesRouted: Total number of messages processed
   *   <li>partitionCounts: Map of partition index to message count
   *   <li>totalGroups: Number of unique JMSXGroupIDs tracked
   *   <li>groupAffinityApplied: Number of times existing group mapping was used
   *   <li>priorityThreshold: The configured priority threshold (for 2-partition mode)
   * </ul>
   *
   * @return a map of statistics
   */
  public Map<String, Object> getStats() {
    Map<String, Object> stats = new HashMap<>();
    stats.put("totalMessagesRouted", totalMessagesRouted.get());
    stats.put("totalGroups", groupPartitionMap.size());
    stats.put("groupAffinityApplied", groupAffinityApplied.get());
    stats.put("priorityThreshold", priorityThreshold);

    // Add per-partition counts
    Map<String, Long> partitionStats = new HashMap<>();
    for (Map.Entry<Integer, AtomicLong> entry : partitionCounts.entrySet()) {
      partitionStats.put("partition" + entry.getKey(), entry.getValue().get());
    }
    stats.put("partitionCounts", partitionStats);

    // Calculate distribution percentages
    long total = totalMessagesRouted.get();
    if (total > 0) {
      Map<String, Double> percentages = new HashMap<>();
      for (Map.Entry<Integer, AtomicLong> entry : partitionCounts.entrySet()) {
        double percentage = (entry.getValue().get() * 100.0) / total;
        percentages.put("partition" + entry.getKey() + "Percentage", percentage);
      }
      stats.put("partitionPercentages", percentages);
    }

    return stats;
  }

  /**
   * Returns the number of unique groups currently tracked.
   *
   * @return the number of groups in the partition map
   */
  public int getGroupCount() {
    return groupPartitionMap.size();
  }

  /**
   * Returns the configured priority threshold (used for 2-partition mode).
   *
   * @return the priority threshold
   */
  public int getPriorityThreshold() {
    return priorityThreshold;
  }

  /**
   * Clears all group affinity mappings. This is primarily useful for testing.
   *
   * <p><b>Warning:</b> Calling this in production will break message ordering for existing groups.
   */
  public void clearGroupMappings() {
    int previousSize = groupPartitionMap.size();
    groupPartitionMap.clear();
    log.warn("Cleared {} group affinity mappings", previousSize);
  }

  /**
   * Resets all statistics counters. This is primarily useful for testing.
   *
   * <p><b>Note:</b> This does not clear group mappings, only statistics.
   */
  public void resetStats() {
    totalMessagesRouted.set(0);
    partitionCounts.clear();
    groupAffinityApplied.set(0);
    log.info("Statistics reset");
  }
}

// Made with Bob
