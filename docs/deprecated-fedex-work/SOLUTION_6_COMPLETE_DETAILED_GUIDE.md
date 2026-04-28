
# Solution 6: Complete Detailed Implementation Guide
## Two-Partition Priority Queue with JMSXGroupID and Weighted Selection

**Status**: ✅ Approved by Team  
**Date**: April 2, 2026  
**Version**: 1.0 - Final

---

## 📋 Table of Contents

1. [Executive Summary](#executive-summary)
2. [Problem Statement](#problem-statement)
3. [Solution Architecture](#solution-architecture)
4. [Component Details](#component-details)
5. [Implementation Guide](#implementation-guide)
6. [Configuration](#configuration)
7. [Testing Strategy](#testing-strategy)
8. [Monitoring & Operations](#monitoring--operations)
9. [Performance Analysis](#performance-analysis)
10. [Troubleshooting](#troubleshooting)

---

## 📊 Executive Summary

### What is Solution 6?

**A three-layer priority system that ensures high-priority messages are processed first, even with huge backlogs.**

### The Three Layers

1. **Producer Layer**: Routes messages to LOW (partition 0) or HIGH (partition 1) based on priority
2. **Consumer Layer**: Weighted selection (80% HIGH, 20% LOW) controls what enters receiver queue
3. **Queue Layer**: Priority queue sorts messages in receiver queue (9→0)

### Expected Results

- **Priority Accuracy**: 95-98% (vs current 60-70%)
- **Throughput Impact**: -10% (45K vs 50K msgs/sec)
- **Latency Improvement**: -60% for high-priority (80ms vs 200ms)
- **Implementation Time**: 11 weeks

### Key Benefits

✅ Solves backlog problem (high-priority never stuck)  
✅ Simple architecture (2 partitions vs 10)  
✅ Message group support (JMSXGroupID)  
✅ Aging prevents starvation  
✅ Backward compatible (feature flag)  

---

## 🎯 Problem Statement

### Current Behavior

**Scenario**: Send 50 low-priority (4) messages, then 50 high-priority (9) messages

**Expected**: All 50 high-priority → All 50 low-priority  
**Actual**: ~37 low → mixed → remaining low  
**Accuracy**: 60-70%

### Root Cause

```
┌─────────────────────────────────────────────────────────────┐
│                    THE PROBLEM                               │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  1. Producer sends 50 low-priority messages                 │
│     → All go to topic (single partition or mixed)           │
│                                                               │
│  2. Consumer pre-fetches 50 messages into receiver queue    │
│     → Queue fills with low-priority messages                │
│     → Receiver Queue: [P4, P4, P4, ...(50 messages)]       │
│                                                               │
│  3. Producer sends 50 high-priority messages                │
│     → High-priority messages arrive at broker               │
│     → But receiver queue already full of low-priority!      │
│                                                               │
│  4. Consumer processes from receiver queue                   │
│     → Processes all 50 low-priority first                   │
│     → High-priority stuck waiting in broker                 │
│                                                               │
│  Result: High-priority delayed by low-priority backlog ❌   │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### Why This Happens

1. **Pre-fetching**: Consumer fetches messages ahead of time (receiverQueueSize=1000)
2. **Arrival Order**: Messages fetched in arrival order, not priority order
3. **Queue Full**: Once queue is full of low-priority, high-priority can't enter
4. **Local Sorting**: Priority queue only sorts what's already in queue

---

## 🏗️ Solution Architecture

### Complete Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         PRODUCER SIDE                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  JMS Producer / Pulsar Producer / Pulsar Function                │
│         │                                                         │
│         │ Sets JMSPriority (0-9)                                 │
│         │ Sets JMSXGroupID (optional)                            │
│         ▼                                                         │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  PriorityGroupPartitionRouter                            │   │
│  │                                                           │   │
│  │  Logic:                                                   │   │
│  │  1. Read JMSPriority from message                        │   │
│  │  2. Read JMSXGroupID (if present)                        │   │
│  │  3. Determine partition:                                 │   │
│  │     - Priority 0-4 → Partition 0 (LOW)                   │   │
│  │     - Priority 5-9 → Partition 1 (HIGH)                  │   │
│  │  4. If JMSXGroupID present:                              │   │
│  │     - Maintain group affinity                            │   │
│  │     - Same group → same partition                        │   │
│  └──────────────────────────────────────────────────────────┘   │
│         │                                                         │
│         ├─────────────────────┬─────────────────────┐           │
│         ▼                     ▼                     ▼           │
│  ┌─────────────┐       ┌─────────────┐      ┌─────────────┐   │
│  │ Priority 0  │       │ Priority 5  │      │ Priority 9  │   │
│  │ Priority 1  │       │ Priority 6  │      │             │   │
│  │ Priority 2  │       │ Priority 7  │      │             │   │
│  │ Priority 3  │       │ Priority 8  │      │             │   │
│  │ Priority 4  │       │             │      │             │   │
│  └─────────────┘       └─────────────┘      └─────────────┘   │
│         │                     │                     │           │
│         └──────────┬──────────┴──────────┬──────────┘           │
│                    ▼                     ▼                       │
│         ┌──────────────────┐  ┌──────────────────┐             │
│         │  Partition 0     │  │  Partition 1     │             │
│         │  (LOW)           │  │  (HIGH)          │             │
│         │  Priority 0-4    │  │  Priority 5-9    │             │
│         └──────────────────┘  └──────────────────┘             │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         CONSUMER SIDE                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  PriorityMultiTopicsConsumerImpl                           │ │
│  │  (Extends Pulsar's MultiTopicsConsumerImpl)               │ │
│  │                                                             │ │
│  │  ┌──────────────────────────────────────────────────────┐ │ │
│  │  │  LAYER 1: Weighted Partition Selector                │ │ │
│  │  │                                                        │ │ │
│  │  │  Logic:                                               │ │ │
│  │  │  1. Check if LOW partition aged                      │ │ │
│  │  │     - Time-based: unread >5 minutes                  │ │ │
│  │  │     - Count-based: skipped >100 times                │ │ │
│  │  │     - If aged: boost LOW to 100%                     │ │ │
│  │  │                                                        │ │ │
│  │  │  2. Normal weighted selection:                       │ │ │
│  │  │     - 80% chance: read from Partition 1 (HIGH)       │ │ │
│  │  │     - 20% chance: read from Partition 0 (LOW)        │ │ │
│  │  │                                                        │ │ │
│  │  │  3. Fetch messages from selected partition           │ │ │
│  │  │                                                        │ │ │
│  │  │  Result: Controls WHAT enters receiver queue         │ │ │
│  │  └──────────────────────────────────────────────────────┘ │ │
│  │                              │                              │ │
│  │                              ▼                              │ │
│  │  ┌──────────────────────────────────────────────────────┐ │ │
│  │  │  LAYER 2: Receiver Queue (Local Memory Buffer)      │ │ │
│  │  │  Size: 50 messages (configurable)                   │ │ │
│  │  │                                                       │ │ │
│  │  │  With 80/20 weighted selection:                     │ │ │
│  │  │  - ~40 messages from Partition 1 (HIGH)             │ │ │
│  │  │  - ~10 messages from Partition 0 (LOW)              │ │ │
│  │  │                                                       │ │ │
│  │  │  Purpose: Performance (pre-fetch for speed)         │ │ │
│  │  └──────────────────────────────────────────────────────┘ │ │
│  │                              │                              │ │
│  │                              ▼                              │ │
│  │  ┌──────────────────────────────────────────────────────┐ │ │
│  │  │  LAYER 3: MessagePriorityGrowableArrayBlockingQueue │ │ │
│  │  │  (Priority Queue - sorts messages)                   │ │ │
│  │  │                                                       │ │ │
│  │  │  Sorting Logic:                                      │ │ │
│  │  │  1. Priority DESC (9 → 8 → 7 → ... → 0)            │ │ │
│  │  │  2. MessageId ASC (FIFO within same priority)       │ │ │
│  │  │                                                       │ │ │
│  │  │  Example queue state:                                │ │ │
│  │  │  [P9, P9, P9, ...(40), P2, P2, ...(10)]            │ │ │
│  │  │   ↑ Next message delivered                           │ │ │
│  │  │                                                       │ │ │
│  │  │  Purpose: Final sorting (ensures priority order)    │ │ │
│  │  └──────────────────────────────────────────────────────┘ │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              │                                    │
│                              ▼                                    │
│                      Application Code                             │
│                      (Receives highest priority first)            │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### How the Three Layers Work Together

**Layer 1: Weighted Partition Selector**
- **Purpose**: Control WHAT enters the receiver queue
- **Mechanism**: 80% reads from HIGH partition, 20% from LOW
- **Result**: Receiver queue contains mostly high-priority messages

**Layer 2: Receiver Queue**
- **Purpose**: Performance (pre-fetch messages for speed)
- **Mechanism**: Local memory buffer (50 messages)
- **Result**: Fast message delivery (no network wait)

**Layer 3: Priority Queue**
- **Purpose**: Final sorting (ensure priority order)
- **Mechanism**: Sorts by priority DESC, then MessageId ASC
- **Result**: Application always gets highest priority message

---

## 🔧 Component Details

### Component 1: PriorityGroupPartitionRouter (Producer Side)

**File**: `pulsar-jms/src/main/java/com/datastax/oss/pulsar/jms/PriorityGroupPartitionRouter.java`

```java
package com.datastax.oss.pulsar.jms;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Routes messages to partitions based on priority and JMSXGroupID.
 * 
 * Routing Logic:
 * - Priority 0-4 → Partition 0 (LOW)
 * - Priority 5-9 → Partition 1 (HIGH)
 * - Messages with same JMSXGroupID → Same partition (group affinity)
 */
public class PriorityGroupPartitionRouter implements MessageRouter {
    
    private static final Logger log = LoggerFactory.getLogger(PriorityGroupPartitionRouter.class);
    
    // Partition constants
    private static final int LOW_PARTITION = 0;   // Priority 0-4
    private static final int HIGH_PARTITION = 1;  // Priority 5-9
    
    // Priority threshold (configurable)
    private final int priorityThreshold;
    
    // Group affinity tracking
    private final Map<String, Integer> groupPartitionMap = new ConcurrentHashMap<>();
    
    public PriorityGroupPartitionRouter() {
        this(5); // Default threshold
    }
    
    public PriorityGroupPartitionRouter(int priorityThreshold) {
        this.priorityThreshold = priorityThreshold;
        log.info("PriorityGroupPartitionRouter initialized with threshold: {}", priorityThreshold);
    }
    
    @Override
    public int choosePartition(Message<?> message, TopicMetadata metadata) {
        // 1. Read priority from message
        int priority = readPriority(message);
        
        // 2. Read JMSXGroupID if present
        String groupId = readGroupId(message);
        
        // 3. Determine target partition based on priority
        int targetPartition = (priority >= priorityThreshold) 
            ? HIGH_PARTITION 
            : LOW_PARTITION;
        
        // 4. If JMSXGroupID present, ensure group affinity
        if (groupId != null && !groupId.isEmpty()) {
            return getPartitionForGroup(groupId, targetPartition);
        }
        
        log.debug("Routed message: priority={}, partition={}", priority, targetPartition);
        return targetPartition;
    }
    
    /**
     * Read priority from message properties
     */
    private int readPriority(Message<?> message) {
        // Try JMSPriority property
        if (message.hasProperty("JMSPriority")) {
            try {
                return Integer.parseInt(message.getProperty("JMSPriority"));
            } catch (NumberFormatException e) {
                log.warn("Invalid JMSPriority value: {}", message.getProperty("JMSPriority"));
            }
        }
        
        // Default priority
        return 4;
    }
    
    /**
     * Read JMSXGroupID from message properties
     */
    private String readGroupId(Message<?> message) {
        return message.getProperty("JMSXGroupID");
    }
    
    /**
     * Ensure message group affinity while respecting priority
     * If group was previously routed to a partition, keep it there
     */
    private int getPartitionForGroup(String groupId, int preferredPartition) {
        // Check if this group has been seen before
        Integer existingPartition = groupPartitionMap.get(groupId);
        
        if (existingPartition != null) {
            // Group already assigned to a partition
            log.debug("Group {} already assigned to partition {}", groupId, existingPartition);
            return existingPartition;
        }
        
        // New group - assign to preferred partition based on priority
        groupPartitionMap.put(groupId, preferredPartition);
        log.info("Assigned group {} to partition {}", groupId, preferredPartition);
        return preferredPartition;
    }
    
    /**
     * Get statistics for monitoring
     */
    public Map<String, Object> getStats() {
        return Map.of(
            "totalGroups", groupPartitionMap.size(),
            "groupsInLowPartition", groupPartitionMap.values().stream()
                .filter(p -> p == LOW_PARTITION).count(),
            "groupsInHighPartition", groupPartitionMap.values().stream()
                .filter(p -> p == HIGH_PARTITION).count()
        );
    }
}
```

### Component 2: PriorityMultiTopicsConsumerImpl (Consumer Side)

**File**: `pulsar-jms/src/main/java/com/datastax/oss/pulsar/jms/PriorityMultiTopicsConsumerImpl.java`

```java
package com.datastax.oss.pulsar.jms;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Extended MultiTopicsConsumerImpl with weighted partition selection.
 * 
 * Features:
 * - Weighted selection: 80% HIGH partition, 20% LOW partition
 * - Aging mechanism: Prevents LOW partition starvation
 * - Global receiver queue: Maintains existing architecture
 */
public class PriorityMultiTopicsConsumerImpl<T> extends MultiTopicsConsumerImpl<T> {
    
    private static final Logger log = LoggerFactory.getLogger(PriorityMultiTopicsConsumerImpl.class);
    
    // Partition constants
    private static final int LOW_PARTITION = 0;
    private static final int HIGH_PARTITION = 1;
    
    // Components
    private final PartitionWeightSelector weightSelector;
    private final AgingTracker agingTracker;
    
    // Configuration (from connection factory)
    private final double highPartitionWeight;  // Default: 0.80
    private final double lowPartitionWeight;   // Default: 0.20
    private final long agingTimeThreshold;     // Default: 5 minutes
    private final int agingCountThreshold;     // Default: 100 skips
    
    public PriorityMultiTopicsConsumerImpl(
            PulsarClientImpl client,
            ConsumerConfigurationData<T> conf,
            ExecutorService listenerExecutor,
            CompletableFuture<Consumer<T>> subscribeFuture,
            Schema<T> schema,
            ConsumerInterceptors<T> interceptors,
            boolean createTopicIfDoesNotExist) {
        
        super(client, conf, listenerExecutor, subscribeFuture, schema, 
              interceptors, createTopicIfDoesNotExist);
        
        // Initialize configuration
        this.highPartitionWeight = conf.getProperty("highPartitionWeight", 0.80);
        this.lowPartitionWeight = conf.getProperty("lowPartitionWeight", 0.20);
        this.agingTimeThreshold = conf.getProperty("agingTimeThreshold", 300000L); // 5 min
        this.agingCountThreshold = conf.getProperty("agingCountThreshold", 100);
        
        // Initialize components
        this.weightSelector = new PartitionWeightSelector();
        this.agingTracker = new AgingTracker(agingTimeThreshold, agingCountThreshold);
        
        log.info("PriorityMultiTopicsConsumerImpl initialized: " +
                "highWeight={}, lowWeight={}, agingTime={}ms, agingCount={}",
                highPartitionWeight, lowPartitionWeight, agingTimeThreshold, agingCountThreshold);
    }
    
    @Override
    protected Message<T> internalReceive(long timeout, TimeUnit unit) 
            throws PulsarClientException {
        
        // 1. Check if LOW partition has aged (needs priority boost)
        if (agingTracker.isPartitionAged(LOW_PARTITION)) {
            log.warn("LOW partition aged - temporarily boosting to 100%");
            Message<T> message = receiveFromPartition(LOW_PARTITION, timeout, unit);
            if (message != null) {
                agingTracker.recordSelection(LOW_PARTITION);
                return message;
            }
        }
        
        // 2. Normal weighted selection
        int selectedPartition = weightSelector.selectPartition(
            HIGH_PARTITION, highPartitionWeight,
            LOW_PARTITION, lowPartitionWeight
        );
        
        // 3. Try to receive from selected partition
        Message<T> message = receiveFromPartition(selectedPartition, timeout, unit);
        
        // 4. Track selection for aging
        if (message != null) {
            agingTracker.recordSelection(selectedPartition);
        }
        
        return message;
    }
    
    /**
     * Receive message from specific partition
     */
    private Message<T> receiveFromPartition(int partition, long timeout, TimeUnit unit) {
        // Get consumer for specific partition
        ConsumerImpl<T> consumer = consumers.get(partition);
        if (consumer == null) {
            log.warn("No consumer found for partition {}", partition);
            return null;
        }
        
        try {
            // Receive from that partition's queue
            return consumer.receive((int) timeout, unit);
        } catch (PulsarClientException e) {
            log.error("Error receiving from partition {}", partition, e);
            return null;
        }
    }
    
    /**
     * Get statistics for monitoring
     */
    public Map<String, Object> getStats() {
        return Map.of(
            "weightSelector", weightSelector.getStats(),
            "agingTracker", agingTracker.getStats()
        );
    }
}
```

### Component 3: PartitionWeightSelector

**File**: `pulsar-jms/src/main/java/com/datastax/oss/pulsar/jms/PartitionWeightSelector.java`

```java
package com.datastax.oss.pulsar.jms;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Selects partition based on configured weights.
 * 
 * Example: 80% HIGH, 20% LOW
 * - 80 out of 100 selections will be HIGH partition
 * - 20 out of 100 selections will be LOW partition
 */
public class PartitionWeightSelector {
    
    private final Random random = new Random();
    
    // Statistics
    private final AtomicLong highPartitionSelections = new AtomicLong(0);
    private final AtomicLong lowPartitionSelections = new AtomicLong(0);
    
    /**
     * Select partition based on weights
     * 
     * @param highPartition The high-priority partition number
     * @param highWeight Weight for high partition (0.0 to 1.0)
     * @param lowPartition The low-priority partition number
     * @param lowWeight Weight for low partition (0.0 to 1.0)
     * @return Selected partition number
     */
    public int selectPartition(
            int highPartition, double highWeight,
            int lowPartition, double lowWeight) {
        
        // Generate random number between 0.0 and 1.0
        double rand = random.nextDouble();
        
        // Normalize weights
        double totalWeight = highWeight + lowWeight;
        double normalizedHighWeight = highWeight / totalWeight;
        
        // Select based on weight
        if (rand < normalizedHighWeight) {
            highPartitionSelections.incrementAndGet();
            return highPartition;
        } else {
            lowPartitionSelections.incrementAndGet();
            return lowPartition;
        }
    }
    
    /**
     * Get selection statistics
     */
    public Map<String, Object> getStats() {
        long high = highPartitionSelections.get();
        long low = lowPartitionSelections.get();
        long total = high + low;
        
        return Map.of(
            "highPartitionSelections", high,
            "lowPartitionSelections", low,
            "totalSelections", total,
            "highPartitionPercentage", total > 0 ? (high * 100.0 / total) : 0.0,
            "lowPartitionPercentage", total > 0 ? (low * 100.0 / total) : 0.0
        );
    }
}
```

### Component 4: AgingTracker

**File**: `pulsar-jms/src/main/java/com/datastax/oss/pulsar/jms/AgingTracker.java`

```java
package com.datastax.oss.pulsar.jms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks partition aging to prevent starvation.
 * 
 * Aging Triggers:
 * - Time-based: Partition unread for >5 minutes
 * - Count-based: Partition skipped >100 times
 * 
 * When aged, partition gets temporary 100% priority boost.
 */
public class AgingTracker {
    
    private static final Logger log = LoggerFactory.getLogger(AgingTracker.class);
    
    private final long timeThreshold;  // milliseconds
    private final int countThreshold;  // number of skips
    
    private final Map<Integer, PartitionStats> partitionStats = new ConcurrentHashMap<>();
    
    public AgingTracker(long timeThreshold, int countThreshold) {
        this.timeThreshold = timeThreshold;
        this.countThreshold = countThreshold;
        log.info("AgingTracker initialized: timeThreshold={}ms, countThreshold={}",
                timeThreshold, countThreshold);
    }
    
    /**
     * Statistics for a partition
     */
    public static class PartitionStats {
        private long lastReadTimestamp;
        private int skipCount;
        private long totalMessagesRead;
        private long totalAgingEvents;
        
        public PartitionStats() {
            this.lastReadTimestamp = System.currentTimeMillis();
            this.skipCount = 0;
            this.totalMessagesRead = 0;
            this.totalAgingEvents = 0;
        }
    }
    
    /**
     * Check if partition has aged and needs priority boost
     */
    public boolean isPartitionAged(int partition) {
        PartitionStats stats = partitionStats.computeIfAbsent(
            partition, k -> new PartitionStats()
        );
        
        long timeSinceLastRead = System.currentTimeMillis() - stats.lastReadTimestamp;
        
        // Time-based aging: unread for >threshold
        if (timeSinceLastRead > timeThreshold) {
            log.warn("Partition {} aged by time: {}ms since last read (threshold: {}ms)",
                    partition, timeSinceLastRead, timeThreshold);
            stats.totalAgingEvents++;
            return true;
        }
        
        // Count-based aging: skipped >threshold times
        if (stats.skipCount > countThreshold) {
            log.warn("Partition {} aged by count: {} skips (threshold: {})",
                    partition, stats.skipCount, countThreshold);
            stats.totalAgingEvents++;
            return true;
        }
        
        return false;
    }
    
    /**
     * Record that a partition was selected for reading
     */
    public void recordSelection(int selectedPartition) {
        // Update selected partition stats
        PartitionStats selectedStats = partitionStats.computeIfAbsent(
            selectedPartition, k -> new PartitionStats()
        );
        selectedStats.lastReadTimestamp = System.currentTimeMillis();
        selectedStats.skipCount = 0;  // Reset skip count
        selectedStats.totalMessagesRead++;
        
        // Increment skip count for non-selected partitions
        for (Map.Entry<Integer, PartitionStats> entry : partitionStats.entrySet()) {
            if (entry.getKey() != selectedPartition) {
                entry.getValue().skipCount++;
            }
        }
    }
    
    /**
     * Get aging statistics for monitoring
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        for (Map.Entry<Integer, PartitionStats> entry : partitionStats.entrySet()) {
            PartitionStats ps = entry.getValue();
            long timeSinceLastRead = System.currentTimeMillis() - ps.lastReadTimestamp;
            
            stats.put("partition" + entry.getKey(), Map.of(
                "timeSinceLastRead", timeSinceLastRead,
                "skipCount", ps.skipCount,
                "totalMessagesRead", ps.totalMessagesRead,
                "totalAgingEvents", ps.totalAgingEvents,
                "isAged", timeSinceLastRead > timeThreshold || ps.skipCount > countThreshold
            ));
        }
        return stats;
    }
}
```

---

## 🚀 Implementation Guide

### Phase 1: Producer-Side Routing (Weeks 1-2)

**Goal**: Implement partition routing based on priority

#### Tasks

1. **Create PriorityGroupPartitionRouter class**
   ```bash
   # Create file
   touch pulsar-jms/src/main/java/com/datastax/oss/pulsar/jms/PriorityGroupPartitionRouter.java
   
   # Implement routing logic (see Component 1 above)
   ```

2. **Integrate with PulsarMessageProducer**
   ```java
   // In PulsarConnectionFactory.java
   public MessageProducer createProducer(Destination destination) {
       ProducerBuilder<byte[]> builder = pulsarClient.newProducer()
           .topic(topicName)
           .messageRouter(new PriorityGroupPartitionRouter())  // ← Add this
           .enableBatching(false);  // Disable batching for priority
       
       return new PulsarMessageProducer(session, builder.create(), destination);
   }
   ```

3. **Add configuration properties**
   ```properties
   # In connection factory config
   jms.enableJMSPriority=true
   jms.priorityPartitionCount=2
   jms.priorityThreshold=5
   jms.enableMessageGroups=true
   ```

4. **Create unit tests**
   ```java
   @Test
   public void testPriorityRouting() {
       PriorityGroupPartitionRouter router = new PriorityGroupPartitionRouter();
       
       // Test low priority → partition 0
       Message lowMsg = createMessage(2);
       assertEquals(0, router.choosePartition(lowMsg, metadata));
       
       // Test high priority → partition 1
       Message highMsg = createMessage(9);
       assertEquals(1, router.choosePartition(highMsg, metadata));
   }
   ```

#### Acceptance Criteria
- [ ] Priority 0-4 routes to partition 0
- [ ] Priority 5-9 routes to partition 1
- [ ] JMSXGroupID affinity maintained
- [ ] All unit tests pass

---

### Phase 2: Consumer-Side Extension (Weeks 3-4)

**Goal**: Implement weighted partition selection

#### Tasks

1. **Create PriorityMultiTopicsConsumerImpl class**
   ```bash
   touch pulsar-jms/src/main/java/com/datastax/oss/pulsar/jms/PriorityMultiTopicsConsumerImpl.java
   ```

2. **Create PartitionWeightSelector class**
   ```bash
   touch pulsar-jms/src/main/java/com/datastax/oss/pulsar/jms/PartitionWeightSelector.java
   ```

3. **Integrate with PulsarConnectionFactory**
   ```java
   // In PulsarConnectionFactory.java
   public MessageConsumer createConsumer(Destination destination) {
       boolean useExtendedConsumer = config.getBoolean("jms.useExtendedConsumer", false);
       
       if (useExtendedConsumer) {
           ConsumerBuilder<byte[]> builder = pulsarClient.newConsumer()
               .topic(topicName)
               .subscriptionName(subscriptionName)
               .receiverQueueSize(50)  // Reduced from 1000
               .consumerImpl(PriorityMultiTopicsConsumerImpl.class);
           
           return new PulsarMessageConsumer(session, builder.subscribe(), destination);
       }
       // ... standard consumer
   }
   ```

4. **Add configuration properties**
   ```properties
   jms.useExtendedConsumer=true
   jms.priority.highPartitionWeight=0.80
   jms.priority.lowPartitionWeight=0.20
   jms.consumerConfig.receiverQueueSize=50
   ```

5. **Create unit tests**
   ```java
   @Test
   public void testWeightedSelection() {
       PartitionWeightSelector selector = new PartitionWeightSelector();
       
       // Test 1000 selections
       int highCount = 0;
       int lowCount = 0;
       for (int i = 0; i < 1000; i++) {
           int partition = selector.selectPartition(1, 0.80, 0, 0.20);
           if (partition == 1) highCount++;
           else lowCount++;
       }
       
       // Should be approximately 80/20
       assertTrue(highCount > 750 && highCount < 850);
       assertTrue(lowCount > 150 && lowCount < 250);
   }
   ```

#### Acceptance Criteria
- [ ] 80/20 weight distribution works
- [ ] Global receiver queue maintained
- [ ] Backward compatible
- [ ] All unit tests pass

---

### Phase 3: Aging Mechanism (Week 5)

**Goal**: Prevent LOW partition starvation

#### Tasks

1. **Create AgingTracker class**
   ```bash
   touch pulsar-jms/src/main/java/com/datastax/oss/pulsar/jms/AgingTracker.java
   ```

2. **Integrate with PriorityMultiTopicsConsumerImpl**
   ```java
   // Already shown in Component 2 above
   ```

3. **Add configuration properties**
   ```properties
   jms.priority.agingTimeThreshold=300000  # 5 minutes
   jms.priority.agingCountThreshold=100    # 100 skips
   ```

4. **Create unit tests**
   ```java
   @Test
   public void testTimeBasedAging() throws InterruptedException {
       AgingTracker tracker = new AgingTracker(1000, 100); // 1 second threshold
       
       // Record selection of HIGH partition
       tracker.recordSelection(1);
       
       // Wait for aging
       Thread.sleep(1100);
       
       // LOW partition should be aged
       assertTrue(tracker.isPartitionAged(0));
   }
   ```

#### Acceptance Criteria
- [ ] Time-based aging triggers correctly
- [ ] Count-based aging triggers correctly
- [ ] LOW partition never starves
- [ ] All unit tests pass

---

### Phase 4: Integration Testing (Week 6)

**Goal**: End-to-end testing with realistic scenarios

#### Test Scenarios

**Test 1: Normal Mixed Load**
```java
@Test
public void testNormalMixedLoad() {
    // Send 60 low, 40 high
    for (int i = 0; i < 60; i++) {
        sendMessage(2); // Low priority
    }
    for (int i = 0; i < 40; i++) {
        sendMessage(9); // High priority
    }
    
    // Receive and verify
    int highFirst = 0;
    for (int i = 0; i < 100; i++) {
        Message msg = consumer.receive();
        if (i < 40 && msg.getJMSPriority() == 9) {
            highFirst++;
        }
    }
    
    // Should get most high-priority first
    assertTrue(highFirst > 30); // >75% accuracy
}
```

**Test 2: Huge Backlog**
```java
@Test
public void testHugeBacklog() {
    // Create backlog
    for (int i = 0; i < 10000; i++) {
        sendMessage(2); // Low priority
    }
    
    // Send high priority
    for (int i = 0; i < 100; i++) {
        sendMessage(9); // High priority
    }
    
    // Receive first 100 messages
    int highPriorityCount = 0;
    for (int i = 0; i < 100; i++) {
        Message msg = consumer.receive();
        if (msg.getJMSPriority() == 9) {
            highPriorityCount++;
        }
    }
    
    // Should get most high-priority first
    assertTrue(highPriorityCount > 70); // >70% of first 100
}
```

**Test 3: Message Groups**
```java
@Test
public void testMessageGroupsWithPriority() {
    // Send grouped messages
    for (int i = 0; i < 10; i++) {
        Message msg = session.createTextMessage("Order " + i);
        msg.setJMSPriority(9);
        msg.setStringProperty("JMSXGroupID", "ORDER-123");
        producer.send(msg);
    }
    
    // Verify all go to same partition
    // (Check via monitoring or partition stats)
}
```

**Test 4: Aging Mechanism**
```java
@Test
public void testAgingPreventsStarvation() {
    // Send only high priority for 6 minutes
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < 360000) {
        sendMessage(9); // High priority
        Thread.sleep(100);
    }
    
    // Send low priority
    sendMessage(2);
    
    // Verify low priority processed soon (aging should trigger)
    // (Check logs for aging events)
}
```

#### Acceptance Criteria
- [ ] Priority accuracy >95%
- [ ] No partition starvation
- [ ] Message group ordering maintained
- [ ] All integration tests pass

---

### Phase 5: Monitoring & Metrics (Week 7)

**Goal**: Add comprehensive monitoring

#### Metrics to Add

```java
// In PriorityMultiTopicsConsumerImpl
public class Metrics {
    // Partition selection distribution
    Counter partitionSelectionCount = Counter.build()
        .name("partition_selection_count")
        .labelNames("partition")
        .help("Number of times each partition was selected")
        .register();
    
    // Aging events
    Counter agingEvents = Counter.build()
        .name("partition_aging_events")
        .labelNames("partition", "reason")
        .help("Number of aging events")
        .register();
    
    // Priority accuracy
    Gauge priorityAccuracy = Gauge.build()
        .name("priority_ordering_accuracy_percent")
        .help("Percentage of high-priority messages received first")
        .register();
    
    // Receiver queue size
    Gauge receiverQueueSize = Gauge.build()
        .name("receiver_queue_size")
        .help("Current size of receiver queue")
        .register();
}
```

#### Grafana Dashboard

```json
{
  "dashboard": {
    "title": "Solution 6 - Priority Queue Monitoring",
    "panels": [
      {
        "title": "Priority Accuracy Over Time",
        "targets": [
          {
            "expr": "priority_ordering_accuracy_percent"
          }
        ]
      },
      {
        "title": "Partition Selection Distribution",
        "targets": [
          {
            "expr": "rate(partition_selection_count[5m])"
          }
        ]
      },
      {
        "title": "Aging Events",
        "targets": [
          {
            "expr": "rate(partition_aging_events[5m])"
          }
        ]
      }
    ]
  }
}
```

#### Acceptance Criteria
- [ ] All metrics exported
- [ ] Dashboard functional
- [ ] Alerts configured
- [ ] Logging useful

---

## ⚙️ Configuration

### Complete Configuration Reference

```properties
# ============================================
# SOLUTION 6 CONFIGURATION
# ============================================

# Enable JMS Priority
jms.enableJMSPriority=true

# Partition Configuration
jms.priorityPartitionCount=2
jms.priorityThreshold=5

# Message Groups
jms.enableMessageGroups=true

# Producer Configuration
jms.producerConfig.batchingEnabled=false
jms.producerConfig.messageRouter=com.datastax.oss.pulsar.jms.PriorityGroupPartitionRouter

# Consumer Configuration
jms.useExtendedConsumer=true
jms.consumerConfig.receiverQueueSize=50

# Weighted Selection
jms.priority.highPartitionWeight=0.80
jms.priority.lowPartitionWeight=0.20

# Aging Thresholds
jms.priority.agingTimeThreshold=300000  # 5 minutes in milliseconds
jms.priority.agingCountThreshold=100    # 100 skips

# Monitoring
jms.priority.enableMetrics=true
jms.priority.metricsPort=9090
```

### Topic Creation

```bash
# Create 2-partition topic
bin/pulsar-admin topics create-partitioned-topic \
  persistent://public/default/priority-queue \
  --partitions 2

# Verify
bin/pulsar-admin topics list-partitioned-topics public/default

# Check stats
bin/pulsar-admin topics stats persistent://public/default/priority-queue
```

---

## 🧪 Testing Strategy

### Unit Tests (100+ tests)

```bash
# Run all unit tests
mvn test -Dtest=Priority*Test

# Run specific test
mvn test -Dtest=PriorityGroupPartitionRouterTest
```

### Integration Tests (20+ scenarios)

```bash
# Run integration tests
mvn verify -Dtest=Priority*IntegrationTest

# Run with specific scenario
mvn verify -Dtest=PriorityBacklogIntegrationTest
```

### Performance Tests

```bash
# Benchmark throughput
mvn verify -Dtest=PriorityPerformanceTest#benchmarkThroughput

# Benchmark latency
mvn verify -Dtest=PriorityPerformanceTest#benchmarkLatency
```

### Load Tests

```bash
# 50K msgs/sec sustained
mvn verify -Dtest=PriorityLoadTest#testSustainedLoad

# 100K msgs/sec burst
mvn verify -Dtest=PriorityLoadTest#testBurstLoad
```

---

## 📊 Performance Analysis

### Expected Results

| Metric | Current | Solution 6 | Change |
|--------|---------|------------|--------|
| **Priority Accuracy** | 60-70% | 95-98% | +30% |
| **Throughput** | 50K msgs/sec | 45K msgs/sec | -10% |
| **P99 Latency (High)** | 200ms | 80ms | -60% |
| **P99 Latency (Low)** | 300ms | 400ms | +33% |
| **CPU Usage** | 60% | 65-70% | +10% |
| **Memory** | 2GB | 2.2GB | +10% |

### Throughput Analysis

**Why 10% reduction?**
- Weighted selection adds overhead
- Smaller receiver queue (50 vs 1000)
- Aging checks add latency

**Mitigation**:
- Tune receiverQueueSize (50-100)
- Optimize weighted selection
- Cache aging calculations

### Latency Analysis

**High-Priority Improvement** (-60%):
- Processed first (80% weight)
- No backlog delay
- Smaller queue = faster delivery

**Low-Priority Increase** (+33%):
- Processed after high-priority
- Aging prevents excessive delay
- Acceptable trade-off

---

## 🔧 Troubleshooting

### Issue 1: Priority Accuracy Below 90%

**Symptoms**:
- High-priority messages delayed
- Mixed ordering observed

**Diagnosis**:
```bash
# Check partition distribution
bin/pulsar-admin topics stats persistent://public/default/priority-queue

# Check consumer lag
bin/pulsar-admin topics stats-internal persistent://public/default/priority-queue
```

**Solutions**:
1. Reduce receiverQueueSize (try 30)
2. Increase HIGH weight (try 0.90)
3. Check if messages routed correctly

### Issue 2: LOW Partition Starving

**Symptoms**:
- Aging events frequent (>10/hour)
- LOW partition lag increasing

**Diagnosis**:
```bash
# Check aging metrics
curl http://localhost:9090/metrics | grep aging_events

# Check partition lag
bin/pulsar-admin topics stats persistent://public/default/priority-queue
```

**Solutions**:
1. Increase LOW weight (try 0.30)
2. Reduce aging thresholds
3. Check if too many high-priority messages

### Issue 3: Throughput Too Low

**Symptoms**:
- Throughput <40K msgs/sec
- Consumer lag increasing

**Diagnosis**:
```bash
# Check consumer stats
bin/pulsar-admin topics stats persistent://public/default/priority-queue

# Check system resources
top -p <consumer-pid>
```

**Solutions**:
1. Increase receiverQueueSize (try 100)
2. Add more consumers
3. Optimize message processing

---

## ✅ Summary

### What We Built

**A three-layer priority system**:
