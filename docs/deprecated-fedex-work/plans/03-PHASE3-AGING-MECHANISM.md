# Phase 3: Aging Mechanism Implementation Plan

**Duration**: 1 week (Week 5)  
**Owner**: Backend Team  
**Status**: Not Started  
**Dependencies**: Phase 2 Complete

---

## 🎯 Phase Objectives

### Primary Goals
1. Implement `AgingTracker` class for starvation prevention
2. Implement time-based aging (5 minute threshold)
3. Implement count-based aging (100 skip threshold)
4. Integrate with `PriorityMultiTopicsConsumerImpl`

### Success Criteria
- ✅ Time-based aging triggers correctly
- ✅ Count-based aging triggers correctly
- ✅ LOW partition never starves (verified in 24hr test)
- ✅ All unit tests pass (20+ tests)
- ✅ Aging events logged and monitored

---

## 📋 Week 5: Implementation & Testing

### Day 1-2: AgingTracker Core Implementation

#### Tasks
1. **Create AgingTracker class**
   ```bash
   File: pulsar-jms/src/main/java/com/datastax/oss/pulsar/jms/AgingTracker.java
   ```

2. **Implement PartitionStats inner class**
   - Track last read timestamp
   - Track skip count
   - Track total messages read
   - Track total aging events

3. **Implement aging detection logic**
   - Time-based: Check if partition unread for >threshold
   - Count-based: Check if partition skipped >threshold
   - Return true if either condition met

#### Code Structure
```java
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
    
    public boolean isPartitionAged(int partition) {
        PartitionStats stats = partitionStats.computeIfAbsent(
            partition, k -> new PartitionStats()
        );
        
        long timeSinceLastRead = System.currentTimeMillis() - stats.lastReadTimestamp;
        
        // Time-based aging
        if (timeSinceLastRead > timeThreshold) {
            log.warn("Partition {} aged by time: {}ms since last read (threshold: {}ms)",
                    partition, timeSinceLastRead, timeThreshold);
            stats.totalAgingEvents++;
            return true;
        }
        
        // Count-based aging
        if (stats.skipCount > countThreshold) {
            log.warn("Partition {} aged by count: {} skips (threshold: {})",
                    partition, stats.skipCount, countThreshold);
            stats.totalAgingEvents++;
            return true;
        }
        
        return false;
    }
    
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
    
    public Map<String, Object> getStats() {
        // Return statistics for monitoring
    }
}
```

#### Acceptance Criteria
- [ ] Class compiles without errors
- [ ] Time-based aging logic works
- [ ] Count-based aging logic works
- [ ] Statistics tracking accurate
- [ ] Thread-safe implementation

---

### Day 3: Integration with PriorityMultiTopicsConsumerImpl

#### Tasks
1. **Add AgingTracker to consumer**
   ```java
   public class PriorityMultiTopicsConsumerImpl<T> extends MultiTopicsConsumerImpl<T> {
       
       private final PartitionWeightSelector weightSelector;
       private final AgingTracker agingTracker;
       
       public PriorityMultiTopicsConsumerImpl(...) {
           super(...);
           
           this.weightSelector = new PartitionWeightSelector();
           
           // Initialize aging tracker
           long agingTimeThreshold = conf.getProperty("agingTimeThreshold", 300000L); // 5 min
           int agingCountThreshold = conf.getProperty("agingCountThreshold", 100);
           this.agingTracker = new AgingTracker(agingTimeThreshold, agingCountThreshold);
       }
   }
   ```

2. **Modify internalReceive() to check aging**
   ```java
   @Override
   protected Message<T> internalReceive(long timeout, TimeUnit unit) 
           throws PulsarClientException {
       
       // 1. Check if LOW partition has aged
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
   ```

3. **Add configuration properties**
   ```properties
   # Aging configuration
   jms.priority.agingTimeThreshold=300000  # 5 minutes in milliseconds
   jms.priority.agingCountThreshold=100    # 100 consecutive skips
   ```

#### Acceptance Criteria
- [ ] AgingTracker integrated with consumer
- [ ] Aging check happens before weighted selection
- [ ] Selection tracking works correctly
- [ ] Configuration properties loaded

---

### Day 4-5: Unit Testing

#### Test Cases to Implement

**Test 1: Time-Based Aging**
```java
@Test
public void testTimeBasedAging() throws InterruptedException {
    AgingTracker tracker = new AgingTracker(1000, 100); // 1 second threshold
    
    // Record selection of HIGH partition
    tracker.recordSelection(1);
    
    // LOW partition should not be aged yet
    assertFalse(tracker.isPartitionAged(0));
    
    // Wait for aging
    Thread.sleep(1100);
    
    // LOW partition should be aged now
    assertTrue(tracker.isPartitionAged(0));
}

@Test
public void testTimeBasedAgingReset() throws InterruptedException {
    AgingTracker tracker = new AgingTracker(1000, 100);
    
    // Select HIGH partition
    tracker.recordSelection(1);
    Thread.sleep(500);
    
    // Select LOW partition (resets timer)
    tracker.recordSelection(0);
    Thread.sleep(600);
    
    // LOW partition should not be aged (only 600ms since last read)
    assertFalse(tracker.isPartitionAged(0));
}
```

**Test 2: Count-Based Aging**
```java
@Test
public void testCountBasedAging() {
    AgingTracker tracker = new AgingTracker(300000, 10); // 10 skip threshold
    
    // Skip LOW partition 10 times
    for (int i = 0; i < 10; i++) {
        tracker.recordSelection(1); // Select HIGH
        assertFalse(tracker.isPartitionAged(0)); // Not aged yet
    }
    
    // 11th skip should trigger aging
    tracker.recordSelection(1);
    assertTrue(tracker.isPartitionAged(0));
}

@Test
public void testCountBasedAgingReset() {
    AgingTracker tracker = new AgingTracker(300000, 10);
    
    // Skip LOW partition 5 times
    for (int i = 0; i < 5; i++) {
        tracker.recordSelection(1);
    }
    
    // Select LOW partition (resets skip count)
    tracker.recordSelection(0);
    
    // Skip 5 more times
    for (int i = 0; i < 5; i++) {
        tracker.recordSelection(1);
    }
    
    // Should not be aged (only 5 skips since last read)
    assertFalse(tracker.isPartitionAged(0));
}
```

**Test 3: Statistics Tracking**
```java
@Test
public void testStatisticsTracking() {
    AgingTracker tracker = new AgingTracker(300000, 100);
    
    // Record some selections
    tracker.recordSelection(1); // HIGH
    tracker.recordSelection(1); // HIGH
    tracker.recordSelection(0); // LOW
    
    Map<String, Object> stats = tracker.getStats();
    
    // Verify statistics
    Map<String, Object> partition0Stats = (Map) stats.get("partition0");
    assertEquals(1L, partition0Stats.get("totalMessagesRead"));
    assertEquals(2, partition0Stats.get("skipCount"));
    
    Map<String, Object> partition1Stats = (Map) stats.get("partition1");
    assertEquals(2L, partition1Stats.get("totalMessagesRead"));
    assertEquals(1, partition1Stats.get("skipCount"));
}

@Test
public void testAgingEventsTracked() throws InterruptedException {
    AgingTracker tracker = new AgingTracker(100, 10);
    
    // Trigger time-based aging
    tracker.recordSelection(1);
    Thread.sleep(150);
    tracker.isPartitionAged(0);
    
    // Trigger count-based aging
    for (int i = 0; i < 11; i++) {
        tracker.recordSelection(1);
    }
    tracker.isPartitionAged(0);
    
    Map<String, Object> stats = tracker.getStats();
    Map<String, Object> partition0Stats = (Map) stats.get("partition0");
    
    // Should have 2 aging events
    assertEquals(2L, partition0Stats.get("totalAgingEvents"));
}
```

**Test 4: Integration with Consumer**
```java
@Test
public void testAgingInConsumer() throws Exception {
    // Create consumer with aging enabled
    PriorityMultiTopicsConsumerImpl consumer = createConsumerWithAging(1000, 10);
    
    // Fill HIGH partition with messages
    fillPartitionWithMessages(1, 100);
    
    // Select HIGH partition 10 times
    for (int i = 0; i < 10; i++) {
        consumer.receive(1000, TimeUnit.MILLISECONDS);
    }
    
    // Next receive should check aging and boost LOW partition
    // (even though LOW partition is empty, aging check should happen)
}

@Test
public void testAgingBoostsLowPartition() throws Exception {
    PriorityMultiTopicsConsumerImpl consumer = createConsumerWithAging(1000, 5);
    
    // Fill both partitions
    fillPartitionWithMessages(0, 10); // LOW
    fillPartitionWithMessages(1, 100); // HIGH
    
    // Skip LOW partition 5 times
    for (int i = 0; i < 5; i++) {
        Message msg = consumer.receive(1000, TimeUnit.MILLISECONDS);
        // Should be from HIGH partition
    }
    
    // Next receive should boost LOW partition
    Message msg = consumer.receive(1000, TimeUnit.MILLISECONDS);
    // Should be from LOW partition (aged)
}
```

**Test 5: Configuration**
```java
@Test
public void testDefaultConfiguration() {
    AgingTracker tracker = new AgingTracker(300000, 100);
    
    // Verify defaults
    assertEquals(300000, tracker.getTimeThreshold());
    assertEquals(100, tracker.getCountThreshold());
}

@Test
public void testCustomConfiguration() {
    AgingTracker tracker = new AgingTracker(60000, 50);
    
    // Verify custom values
    assertEquals(60000, tracker.getTimeThreshold());
    assertEquals(50, tracker.getCountThreshold());
}
```

**Test 6: Edge Cases**
```java
@Test
public void testMultiplePartitions() {
    // Test with more than 2 partitions
    // Verify aging works for all partitions
}

@Test
public void testConcurrentAccess() throws InterruptedException {
    // Test thread-safety with concurrent selections
}

@Test
public void testZeroThresholds() {
    // Test with threshold=0 (always aged)
}
```

#### Acceptance Criteria
- [ ] 20+ unit tests implemented
- [ ] All tests pass
- [ ] Code coverage >85%
- [ ] Edge cases covered
- [ ] Thread-safety verified

---

### Day 6: Starvation Testing

#### Long-Running Tests

**Test 1: 24-Hour Starvation Test**
```java
@Test
@Timeout(value = 25, unit = TimeUnit.HOURS)
public void test24HourNoStarvation() throws Exception {
    // Setup
    PriorityMultiTopicsConsumerImpl consumer = createConsumerWithAging(300000, 100);
    
    // Continuously send HIGH priority messages
    ScheduledExecutorService sender = Executors.newScheduledThreadPool(1);
    sender.scheduleAtFixedRate(() -> {
        sendHighPriorityMessage();
    }, 0, 100, TimeUnit.MILLISECONDS);
    
    // Send LOW priority messages occasionally
    sender.scheduleAtFixedRate(() -> {
        sendLowPriorityMessage();
    }, 0, 10, TimeUnit.SECONDS);
    
    // Track LOW partition reads
    AtomicInteger lowPartitionReads = new AtomicInteger(0);
    long startTime = System.currentTimeMillis();
    
    // Consume for 24 hours
    while (System.currentTimeMillis() - startTime < TimeUnit.HOURS.toMillis(24)) {
        Message msg = consumer.receive(1000, TimeUnit.MILLISECONDS);
        if (msg != null && isFromLowPartition(msg)) {
            lowPartitionReads.incrementAndGet();
        }
    }
    
    // Verify LOW partition was read (not starved)
    assertTrue(lowPartitionReads.get() > 0, 
        "LOW partition was starved - no messages read in 24 hours");
    
    // Verify aging events occurred
    Map<String, Object> stats = consumer.getAgingTracker().getStats();
    Map<String, Object> lowStats = (Map) stats.get("partition0");
    assertTrue((Long) lowStats.get("totalAgingEvents") > 0,
        "No aging events occurred");
}
```

**Test 2: Extreme Load Test**
```java
@Test
public void testExtremeLoadNoStarvation() throws Exception {
    // Send 1M HIGH priority messages
    // Send 1K LOW priority messages
    // Verify LOW messages eventually processed
}
```

**Test 3: Aging Frequency Test**
```java
@Test
public void testAgingFrequency() throws Exception {
    // Monitor how often aging triggers
    // Verify it's not too frequent (performance impact)
    // Verify it's not too rare (starvation risk)
}
```

#### Acceptance Criteria
- [ ] 24-hour test passes (no starvation)
- [ ] Extreme load test passes
- [ ] Aging frequency acceptable
- [ ] Performance impact minimal

---

### Day 7: Documentation & Review

#### Documentation Tasks

1. **JavaDoc Comments**
   ```java
   /**
    * Tracks partition aging to prevent starvation.
    * 
    * <p>Aging Triggers:
    * <ul>
    *   <li>Time-based: Partition unread for >5 minutes (configurable)</li>
    *   <li>Count-based: Partition skipped >100 times (configurable)</li>
    * </ul>
    * 
    * <p>When a partition ages, it receives temporary 100% priority boost
    * until at least one message is read from it.
    * 
    * <p>Thread-safe: Uses ConcurrentHashMap for partition statistics.
    * 
    * @see PriorityMultiTopicsConsumerImpl
    */
   public class AgingTracker {
       // ...
   }
   ```

2. **Configuration Guide**
   ```markdown
   ## Aging Mechanism Configuration
   
   Prevent LOW partition starvation with aging:
   
   ```properties
   # Time-based aging (milliseconds)
   jms.priority.agingTimeThreshold=300000  # 5 minutes
   
   # Count-based aging (number of skips)
   jms.priority.agingCountThreshold=100
   ```
   
   **How Aging Works**:
   
   1. **Time-Based**: If LOW partition hasn't been read for >5 minutes, 
      it gets 100% priority boost
   
   2. **Count-Based**: If LOW partition has been skipped >100 times, 
      it gets 100% priority boost
   
   3. **Priority Boost**: When aged, LOW partition is read next, 
      regardless of weights
   
   4. **Reset**: After reading from aged partition, normal weights resume
   
   **Tuning Guidelines**:
   - Lower thresholds = more frequent aging = better starvation prevention
   - Higher thresholds = less frequent aging = better performance
   - Recommended: Keep defaults unless monitoring shows starvation
   ```

3. **Architecture Documentation**
   - Explain aging algorithm
   - Document integration with consumer
   - Add sequence diagrams

4. **Monitoring Guide**
   ```markdown
   ## Monitoring Aging Events
   
   **Metrics to Watch**:
   - `partition0.totalAgingEvents`: Number of times LOW partition aged
   - `partition0.timeSinceLastRead`: Time since last LOW partition read
   - `partition0.skipCount`: Number of consecutive skips
   
   **Alerts**:
   - Alert if `totalAgingEvents` > 100/hour (too frequent)
   - Alert if `timeSinceLastRead` > 10 minutes (potential starvation)
   - Alert if `skipCount` > 200 (potential starvation)
   ```

#### Code Review Checklist
- [ ] Code follows Google Java Format
- [ ] All methods have JavaDoc
- [ ] No TODO/FIXME comments
- [ ] Error handling complete
- [ ] Logging appropriate
- [ ] Thread-safety verified
- [ ] Tests comprehensive
- [ ] Performance acceptable

#### Acceptance Criteria
- [ ] All documentation complete
- [ ] Code review approved by 2+ engineers
- [ ] No critical issues found
- [ ] Ready for Phase 4

---

## 📊 Deliverables

### Code Artifacts
- ✅ `AgingTracker.java` (200-250 lines)
- ✅ Integration with `PriorityMultiTopicsConsumerImpl`
- ✅ Unit tests (20+ tests, 600+ lines)
- ✅ Starvation tests (3+ tests, 400+ lines)
- ✅ Configuration updates

### Documentation
- ✅ JavaDoc comments (100% coverage)
- ✅ Configuration guide
- ✅ Architecture documentation
- ✅ Monitoring guide
- ✅ Tuning guidelines

### Test Results
- ✅ Unit test report (100%pass)
- ✅ Starvation test report (24hr test)
- ✅ Performance impact report
- ✅ Code coverage report (>85%)

---

## 🚨 Risks & Mitigation

### Risk 1: Aging Too Frequent (Performance Impact)
**Impact**: Medium  
**Probability**: Medium  
**Mitigation**:
- Monitor aging frequency in production
- Tune thresholds based on actual usage
- Add configuration to disable if needed

### Risk 2: Aging Too Rare (Starvation Still Occurs)
**Impact**: High  
**Probability**: Low  
**Mitigation**:
- Comprehensive starvation testing
- Conservative default thresholds
- Monitoring and alerting

### Risk 3: Thread-Safety Issues
**Impact**: High  
**Probability**: Low  
**Mitigation**:
- Use ConcurrentHashMap
- Extensive concurrent testing
- Code review focus on thread-safety

### Risk 4: Memory Leak (Partition Stats Growth)
**Impact**: Medium  
**Probability**: Low  
**Mitigation**:
- Monitor memory usage
- Consider cleanup for unused partitions
- Add max partitions limit

---

## ✅ Phase 3 Completion Checklist

### Code Complete
- [ ] AgingTracker implemented
- [ ] Integration with consumer complete
- [ ] All unit tests passing
- [ ] Starvation tests passing
- [ ] Performance tests passing
- [ ] Code review approved

### Documentation Complete
- [ ] JavaDoc 100% coverage
- [ ] Configuration guide written
- [ ] Architecture documentation written
- [ ] Monitoring guide written
- [ ] Tuning guidelines written

### Quality Gates
- [ ] Code coverage >85%
- [ ] No critical bugs
- [ ] 24-hour starvation test passes
- [ ] Performance impact minimal
- [ ] Thread-safety verified

### Ready for Phase 4
- [ ] All acceptance criteria met
- [ ] Stakeholder approval received
- [ ] Phase 4 dependencies identified
- [ ] Team ready for integration testing

---

## 📞 Communication

### Daily Updates
- Slack channel: #solution6-implementation
- Daily standup: 9:00 AM
- Blockers escalated immediately

### Weekly Review
- Friday 2:00 PM
- Demo aging mechanism
- Review starvation test results

### Phase Gate Review
- End of Week 5
- Go/No-Go decision for Phase 4
- Stakeholder sign-off required

---

**Previous Phase**: [Phase 2 - Consumer-Side Extension](./02-PHASE2-CONSUMER-EXTENSION.md)  
**Next Phase**: [Phase 4 - Integration Testing](./04-PHASE4-INTEGRATION-TESTING.md)