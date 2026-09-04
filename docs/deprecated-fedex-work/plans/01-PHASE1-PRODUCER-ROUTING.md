# Phase 1: Producer-Side Routing Implementation Plan

**Duration**: 2 weeks (Weeks 1-2)  
**Owner**: Backend Team  
**Status**: Not Started  
**Dependencies**: None

---

## 🎯 Phase Objectives

### Primary Goals
1. Implement `PriorityGroupPartitionRouter` class for partition routing
2. Route messages based on JMS priority (0-4 → LOW, 5-9 → HIGH)
3. Maintain JMSXGroupID affinity for message ordering
4. Integrate with existing `PulsarMessageProducer`

### Success Criteria
- ✅ Priority 0-4 routes to partition 0 (100% accuracy)
- ✅ Priority 5-9 routes to partition 1 (100% accuracy)
- ✅ JMSXGroupID affinity maintained across sessions
- ✅ All unit tests pass (20+ tests)
- ✅ No performance regression in producer throughput

---

## 📋 Week 1: Core Implementation

### Day 1-2: Class Structure & Basic Routing

#### Tasks
1. **Create PriorityGroupPartitionRouter class**
   ```bash
   File: pulsar-jms/src/main/java/com/datastax/oss/pulsar/jms/PriorityGroupPartitionRouter.java
   ```

2. **Implement MessageRouter interface**
   - Override `choosePartition(Message<?> message, TopicMetadata metadata)`
   - Add logging with SLF4J
   - Add configuration support

3. **Implement priority reading logic**
   - Read `JMSPriority` property from message
   - Handle missing/invalid priority (default to 4)
   - Add validation and error handling

#### Code Structure
```java
public class PriorityGroupPartitionRouter implements MessageRouter {
    private static final Logger log = LoggerFactory.getLogger(...);
    private static final int LOW_PARTITION = 0;
    private static final int HIGH_PARTITION = 1;
    
    private final int priorityThreshold;  // Default: 5
    private final Map<String, Integer> groupPartitionMap;
    
    @Override
    public int choosePartition(Message<?> message, TopicMetadata metadata) {
        // 1. Read priority
        // 2. Read JMSXGroupID
        // 3. Determine partition
        // 4. Apply group affinity
    }
}
```

#### Acceptance Criteria
- [ ] Class compiles without errors
- [ ] Basic routing logic works (priority → partition)
- [ ] Logging statements in place
- [ ] Code follows Google Java Format

---

### Day 3-4: JMSXGroupID Affinity

#### Tasks
1. **Implement group tracking**
   - Use `ConcurrentHashMap` for thread-safe group tracking
   - Track group → partition mapping
   - Handle first-time group assignment

2. **Implement affinity logic**
   - Check if group exists in map
   - If exists, return existing partition
   - If new, assign based on priority and store

3. **Add group statistics**
   - Track total groups
   - Track groups per partition
   - Expose via `getStats()` method

#### Code Implementation
```java
private final Map<String, Integer> groupPartitionMap = new ConcurrentHashMap<>();

private int getPartitionForGroup(String groupId, int preferredPartition) {
    Integer existingPartition = groupPartitionMap.get(groupId);
    
    if (existingPartition != null) {
        return existingPartition;
    }
    
    groupPartitionMap.put(groupId, preferredPartition);
    log.info("Assigned group {} to partition {}", groupId, preferredPartition);
    return preferredPartition;
}
```

#### Acceptance Criteria
- [ ] Group affinity works correctly
- [ ] Same group always routes to same partition
- [ ] Statistics method implemented
- [ ] Thread-safe implementation verified

---

### Day 5: Unit Testing

#### Test Cases to Implement

**Test 1: Basic Priority Routing**
```java
@Test
public void testLowPriorityRoutesToPartition0() {
    PriorityGroupPartitionRouter router = new PriorityGroupPartitionRouter();
    Message msg = createMessageWithPriority(2);
    
    int partition = router.choosePartition(msg, metadata);
    
    assertEquals(0, partition);
}

@Test
public void testHighPriorityRoutesToPartition1() {
    PriorityGroupPartitionRouter router = new PriorityGroupPartitionRouter();
    Message msg = createMessageWithPriority(9);
    
    int partition = router.choosePartition(msg, metadata);
    
    assertEquals(1, partition);
}
```

**Test 2: Boundary Cases**
```java
@Test
public void testPriority4RoutesToLowPartition() {
    // Priority 4 should go to LOW (0-4 range)
}

@Test
public void testPriority5RoutesToHighPartition() {
    // Priority 5 should go to HIGH (5-9 range)
}

@Test
public void testMissingPriorityUsesDefault() {
    // Missing priority should default to 4 → LOW partition
}

@Test
public void testInvalidPriorityUsesDefault() {
    // Invalid priority should default to 4 → LOW partition
}
```

**Test 3: JMSXGroupID Affinity**
```java
@Test
public void testGroupAffinityMaintained() {
    PriorityGroupPartitionRouter router = new PriorityGroupPartitionRouter();
    
    // First message with group
    Message msg1 = createMessageWithPriorityAndGroup(9, "ORDER-123");
    int partition1 = router.choosePartition(msg1, metadata);
    
    // Second message with same group
    Message msg2 = createMessageWithPriorityAndGroup(9, "ORDER-123");
    int partition2 = router.choosePartition(msg2, metadata);
    
    assertEquals(partition1, partition2);
}

@Test
public void testDifferentGroupsCanHaveDifferentPartitions() {
    // GROUP-A and GROUP-B can be on different partitions
}
```

**Test 4: Configuration**
```java
@Test
public void testCustomPriorityThreshold() {
    PriorityGroupPartitionRouter router = new PriorityGroupPartitionRouter(7);
    
    // Priority 6 should go to LOW (threshold is 7)
    Message msg = createMessageWithPriority(6);
    assertEquals(0, router.choosePartition(msg, metadata));
}
```

**Test 5: Statistics**
```java
@Test
public void testStatisticsTracking() {
    PriorityGroupPartitionRouter router = new PriorityGroupPartitionRouter();
    
    // Send messages with groups
    router.choosePartition(createMessageWithGroup("G1"), metadata);
    router.choosePartition(createMessageWithGroup("G2"), metadata);
    
    Map<String, Object> stats = router.getStats();
    assertEquals(2, stats.get("totalGroups"));
}
```

#### Acceptance Criteria
- [ ] 20+ unit tests implemented
- [ ] All tests pass
- [ ] Code coverage >85%
- [ ] Edge cases covered

---

## 📋 Week 2: Integration & Testing

### Day 6-7: Integration with PulsarMessageProducer

#### Tasks
1. **Modify PulsarConnectionFactory**
   ```java
   // In createProducer() method
   ProducerBuilder<byte[]> builder = pulsarClient.newProducer()
       .topic(topicName)
       .messageRouter(new PriorityGroupPartitionRouter(priorityThreshold))
       .enableBatching(false);  // Disable for priority
   ```

2. **Add configuration properties**
   ```properties
   # In PulsarConnectionFactory configuration
   jms.enableJMSPriority=true
   jms.priorityPartitionCount=2
   jms.priorityThreshold=5
   jms.enableMessageGroups=true
   ```

3. **Add feature flag support**
   - Check `jms.enableJMSPriority` before using router
   - Fall back to default routing if disabled
   - Log configuration on startup

#### Integration Points
```java
public MessageProducer createProducer(Destination destination) {
    boolean enablePriority = config.getBoolean("jms.enableJMSPriority", false);
    
    ProducerBuilder<byte[]> builder = pulsarClient.newProducer()
        .topic(topicName);
    
    if (enablePriority) {
        int threshold = config.getInt("jms.priorityThreshold", 5);
        builder.messageRouter(new PriorityGroupPartitionRouter(threshold));
        builder.enableBatching(false);
        log.info("Priority routing enabled with threshold: {}", threshold);
    }
    
    return new PulsarMessageProducer(session, builder.create(), destination);
}
```

#### Acceptance Criteria
- [ ] Router integrated with producer
- [ ] Configuration properties work
- [ ] Feature flag toggles routing
- [ ] Backward compatible (default: disabled)

---

### Day 8: Integration Testing

#### Test Scenarios

**Test 1: End-to-End Priority Routing**
```java
@Test
public void testEndToEndPriorityRouting() throws Exception {
    // Setup
    Map<String, Object> config = new HashMap<>();
    config.put("jms.enableJMSPriority", true);
    config.put("jms.priorityThreshold", 5);
    
    PulsarConnectionFactory factory = new PulsarConnectionFactory(config);
    Connection conn = factory.createConnection();
    Session session = conn.createSession();
    
    Queue queue = session.createQueue("persistent://public/default/priority-test");
    MessageProducer producer = session.createProducer(queue);
    
    // Send low priority
    TextMessage lowMsg = session.createTextMessage("Low priority");
    lowMsg.setJMSPriority(2);
    producer.send(lowMsg);
    
    // Send high priority
    TextMessage highMsg = session.createTextMessage("High priority");
    highMsg.setJMSPriority(9);
    producer.send(highMsg);
    
    // Verify routing via Pulsar admin API
    // Check partition 0 has low priority message
    // Check partition 1 has high priority message
}
```

**Test 2: Message Group Routing**
```java
@Test
public void testMessageGroupRouting() throws Exception {
    // Send multiple messages with same JMSXGroupID
    // Verify all go to same partition
}
```

**Test 3: Feature Flag Toggle**
```java
@Test
public void testFeatureFlagDisabled() {
    // With jms.enableJMSPriority=false
    // Verify default routing is used
}
```

#### Acceptance Criteria
- [ ] End-to-end tests pass
- [ ] Messages route to correct partitions
- [ ] Feature flag works correctly
- [ ] No errors in logs

---

### Day 9: Performance Testing

#### Benchmarks to Run

**Test 1: Producer Throughput**
```java
@Test
public void testProducerThroughput() {
    // Baseline: Without priority routing
    long baseline = measureThroughput(false);
    
    // With priority routing
    long withPriority = measureThroughput(true);
    
    // Should be within 10% of baseline
    assertTrue(withPriority > baseline * 0.9);
}
```

**Test 2: Latency Impact**
```java
@Test
public void testLatencyImpact() {
    // Measure p50, p95, p99 latencies
    // Compare with/without priority routing
}
```

**Test 3: Memory Usage**
```java
@Test
public void testMemoryUsage() {
    // Monitor groupPartitionMap size
    // Verify no memory leaks
}
```

#### Performance Targets
- **Throughput**: >90% of baseline (>45K msgs/sec)
- **Latency p50**: <5ms overhead
- **Latency p99**: <20ms overhead
- **Memory**: <10MB for 10K groups

#### Acceptance Criteria
- [ ] Throughput within 10% of baseline
- [ ] Latency overhead acceptable
- [ ] No memory leaks
- [ ] Performance report documented

---

### Day 10: Documentation & Review

#### Documentation Tasks

1. **JavaDoc Comments**
   - Add class-level documentation
   - Document all public methods
   - Add usage examples

2. **Configuration Guide**
   ```markdown
   ## Priority Routing Configuration
   
   Enable priority-based partition routing:
   
   ```properties
   jms.enableJMSPriority=true
   jms.priorityPartitionCount=2
   jms.priorityThreshold=5
   jms.enableMessageGroups=true
   ```
   
   - `priorityThreshold`: Messages with priority >= threshold go to HIGH partition
   - Default threshold: 5 (priority 0-4 → LOW, 5-9 → HIGH)
   ```

3. **Migration Guide**
   - How to enable for existing applications
   - Backward compatibility notes
   - Rollback procedure

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
- [ ] Ready for Phase 2

---

## 📊 Deliverables

### Code Artifacts
- ✅ `PriorityGroupPartitionRouter.java` (200-300 lines)
- ✅ Unit tests (20+ tests, 500+ lines)
- ✅ Integration tests (5+ tests, 300+ lines)
- ✅ Configuration updates

### Documentation
- ✅ JavaDoc comments (100% coverage)
- ✅ Configuration guide
- ✅ Migration guide
- ✅ Performance report

### Test Results
- ✅ Unit test report (100% pass)
- ✅ Integration test report (100% pass)
- ✅ Performance benchmark report
- ✅ Code coverage report (>85%)

---

## 🚨 Risks & Mitigation

### Risk 1: Performance Regression
**Impact**: High  
**Probability**: Medium  
**Mitigation**:
- Continuous benchmarking during development
- Optimize hot paths (priority reading, group lookup)
- Use efficient data structures (ConcurrentHashMap)

### Risk 2: Group Map Memory Growth
**Impact**: Medium  
**Probability**: Low  
**Mitigation**:
- Monitor map size in production
- Consider LRU eviction for old groups
- Add configuration for max groups

### Risk 3: Integration Complexity
**Impact**: Medium  
**Probability**: Low  
**Mitigation**:
- Detailed integration plan
- Feature flag for safe rollout
- Extensive testing

---

## ✅ Phase 1 Completion Checklist

### Code Complete
- [ ] PriorityGroupPartitionRouter implemented
- [ ] All unit tests passing
- [ ] Integration tests passing
- [ ] Performance tests passing
- [ ] Code review approved

### Documentation Complete
- [ ] JavaDoc 100% coverage
- [ ] Configuration guide written
- [ ] Migration guide written
- [ ] Performance report documented

### Quality Gates
- [ ] Code coverage >85%
- [ ] No critical bugs
- [ ] Performance within targets
- [ ] Backward compatible

### Ready for Phase 2
- [ ] All acceptance criteria met
- [ ] Stakeholder approval received
- [ ] Phase 2 dependencies identified
- [ ] Team ready to proceed

---

## 📞 Communication

### Daily Updates
- Slack channel: #solution6-implementation
- Daily standup: 9:00 AM
- Blockers escalated immediately

### Weekly Review
- Friday 2:00 PM
- Demo progress to stakeholders
- Review metrics and risks

### Phase Gate Review
- End of Week 2
- Go/No-Go decision for Phase 2
- Stakeholder sign-off required

---

**Next Phase**: [Phase 2 - Consumer-Side Extension](./02-PHASE2-CONSUMER-EXTENSION.md)