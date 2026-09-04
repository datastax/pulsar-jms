# Phase 2: Consumer-Side Extension Implementation Plan

**Duration**: 2 weeks (Weeks 3-4)  
**Owner**: Backend Team  
**Status**: Not Started  
**Dependencies**: Phase 1 Complete

---

## 🎯 Phase Objectives

### Primary Goals
1. Implement `PriorityMultiTopicsConsumerImpl` for weighted partition selection
2. Implement `PartitionWeightSelector` for 80/20 distribution
3. Integrate with existing consumer architecture
4. Maintain global receiver queue pattern

### Success Criteria
- ✅ 80/20 weight distribution achieved (±5%)
- ✅ Global receiver queue maintained
- ✅ Backward compatible with existing consumers
- ✅ All unit tests pass (30+ tests)
- ✅ Throughput impact <10%

---

## 📋 Week 3: Core Implementation

### Day 1-2: PartitionWeightSelector Implementation

#### Tasks
1. **Create PartitionWeightSelector class**
   ```bash
   File: pulsar-jms/src/main/java/com/datastax/oss/pulsar/jms/PartitionWeightSelector.java
   ```

2. **Implement weighted selection algorithm**
   - Use `Random.nextDouble()` for selection
   - Normalize weights to sum to 1.0
   - Track selection statistics

3. **Add statistics tracking**
   - Count selections per partition
   - Calculate percentages
   - Expose via `getStats()` method

#### Code Structure
```java
public class PartitionWeightSelector {
    private final Random random = new Random();
    private final AtomicLong highPartitionSelections = new AtomicLong(0);
    private final AtomicLong lowPartitionSelections = new AtomicLong(0);
    
    public int selectPartition(
            int highPartition, double highWeight,
            int lowPartition, double lowWeight) {
        double rand = random.nextDouble();
        double totalWeight = highWeight + lowWeight;
        double normalizedHighWeight = highWeight / totalWeight;
        
        if (rand < normalizedHighWeight) {
            highPartitionSelections.incrementAndGet();
            return highPartition;
        } else {
            lowPartitionSelections.incrementAndGet();
            return lowPartition;
        }
    }
    
    public Map<String, Object> getStats() {
        // Return selection statistics
    }
}
```

#### Unit Tests
```java
@Test
public void testWeightedSelection() {
    PartitionWeightSelector selector = new PartitionWeightSelector();
    
    int highCount = 0;
    int lowCount = 0;
    for (int i = 0; i < 1000; i++) {
        int partition = selector.selectPartition(1, 0.80, 0, 0.20);
        if (partition == 1) highCount++;
        else lowCount++;
    }
    
    // Should be approximately 80/20 (±5%)
    assertTrue(highCount >= 750 && highCount <= 850);
    assertTrue(lowCount >= 150 && lowCount <= 250);
}

@Test
public void testDifferentWeights() {
    // Test 90/10, 70/30, 50/50 distributions
}

@Test
public void testStatistics() {
    // Verify statistics tracking
}
```

#### Acceptance Criteria
- [ ] Class compiles without errors
- [ ] Weighted selection works correctly
- [ ] Statistics tracking accurate
- [ ] Unit tests pass (10+ tests)

---

### Day 3-5: PriorityMultiTopicsConsumerImpl Implementation

#### Tasks
1. **Create PriorityMultiTopicsConsumerImpl class**
   ```bash
   File: pulsar-jms/src/main/java/com/datastax/oss/pulsar/jms/PriorityMultiTopicsConsumerImpl.java
   ```

2. **Extend MultiTopicsConsumerImpl**
   - Override `internalReceive()` method
   - Integrate PartitionWeightSelector
   - Maintain global receiver queue

3. **Implement partition-specific receive**
   - Get consumer for specific partition
   - Receive from partition's queue
   - Handle errors gracefully

#### Code Structure
```java
public class PriorityMultiTopicsConsumerImpl<T> extends MultiTopicsConsumerImpl<T> {
    
    private static final Logger log = LoggerFactory.getLogger(...);
    
    private final PartitionWeightSelector weightSelector;
    private final double highPartitionWeight;
    private final double lowPartitionWeight;
    
    public PriorityMultiTopicsConsumerImpl(
            PulsarClientImpl client,
            ConsumerConfigurationData<T> conf,
            ExecutorService listenerExecutor,
            CompletableFuture<Consumer<T>> subscribeFuture,
            Schema<T> schema,
            ConsumerInterceptors<T> interceptors,
            boolean createTopicIfDoesNotExist) {
        
        super(client, conf, listenerExecutor, subscribeFuture, 
              schema, interceptors, createTopicIfDoesNotExist);
        
        this.highPartitionWeight = conf.getProperty("highPartitionWeight", 0.80);
        this.lowPartitionWeight = conf.getProperty("lowPartitionWeight", 0.20);
        this.weightSelector = new PartitionWeightSelector();
        
        log.info("PriorityMultiTopicsConsumerImpl initialized: " +
                "highWeight={}, lowWeight={}", 
                highPartitionWeight, lowPartitionWeight);
    }
    
    @Override
    protected Message<T> internalReceive(long timeout, TimeUnit unit) 
            throws PulsarClientException {
        
        // Select partition based on weights
        int selectedPartition = weightSelector.selectPartition(
            HIGH_PARTITION, highPartitionWeight,
            LOW_PARTITION, lowPartitionWeight
        );
        
        // Receive from selected partition
        return receiveFromPartition(selectedPartition, timeout, unit);
    }
    
    private Message<T> receiveFromPartition(int partition, long timeout, TimeUnit unit) {
        ConsumerImpl<T> consumer = consumers.get(partition);
        if (consumer == null) {
            log.warn("No consumer found for partition {}", partition);
            return null;
        }
        
        try {
            return consumer.receive((int) timeout, unit);
        } catch (PulsarClientException e) {
            log.error("Error receiving from partition {}", partition, e);
            return null;
        }
    }
}
```

#### Acceptance Criteria
- [ ] Class extends MultiTopicsConsumerImpl correctly
- [ ] Weighted selection integrated
- [ ] Partition-specific receive works
- [ ] Error handling complete
- [ ] Logging appropriate

---

### Day 6-7: Unit Testing

#### Test Cases to Implement

**Test 1: Weighted Selection Integration**
```java
@Test
public void testWeightedSelectionInConsumer() {
    // Create consumer with 80/20 weights
    // Verify partition selection follows weights
}

@Test
public void testCustomWeights() {
    // Test with different weight configurations
    // 90/10, 70/30, 50/50
}
```

**Test 2: Partition Receive**
```java
@Test
public void testReceiveFromHighPartition() {
    // Mock HIGH partition consumer
    // Verify message received from correct partition
}

@Test
public void testReceiveFromLowPartition() {
    // Mock LOW partition consumer
    // Verify message received from correct partition
}

@Test
public void testPartitionNotAvailable() {
    // Test when partition consumer is null
    // Should handle gracefully
}
```

**Test 3: Configuration**
```java
@Test
public void testDefaultWeights() {
    // Verify default 80/20 weights
}

@Test
public void testCustomWeightsFromConfig() {
    // Load weights from configuration
    // Verify applied correctly
}
```

**Test 4: Error Handling**
```java
@Test
public void testReceiveTimeout() {
    // Test timeout handling
}

@Test
public void testReceiveException() {
    // Test exception handling
}
```

**Test 5: Statistics**
```java
@Test
public void testStatisticsCollection() {
    // Verify statistics collected correctly
}
```

#### Acceptance Criteria
- [ ] 30+ unit tests implemented
- [ ] All tests pass
- [ ] Code coverage >85%
- [ ] Edge cases covered

---

## 📋 Week 4: Integration & Testing

### Day 8-9: Integration with PulsarConnectionFactory

#### Tasks
1. **Modify PulsarConnectionFactory**
   ```java
   public MessageConsumer createConsumer(Destination destination) {
       boolean useExtendedConsumer = config.getBoolean("jms.useExtendedConsumer", false);
       
       if (useExtendedConsumer) {
           ConsumerBuilder<byte[]> builder = pulsarClient.newConsumer()
               .topic(topicName)
               .subscriptionName(subscriptionName)
               .receiverQueueSize(50)  // Reduced from 1000
               .consumerImpl(PriorityMultiTopicsConsumerImpl.class);
           
           // Set weight configuration
           builder.property("highPartitionWeight", 
               config.getDouble("jms.priority.highPartitionWeight", 0.80));
           builder.property("lowPartitionWeight", 
               config.getDouble("jms.priority.lowPartitionWeight", 0.20));
           
           return new PulsarMessageConsumer(session, builder.subscribe(), destination);
       }
       
       // Standard consumer
       return createStandardConsumer(destination);
   }
   ```

2. **Add configuration properties**
   ```properties
   # Enable extended consumer
   jms.useExtendedConsumer=true
   
   # Weight configuration
   jms.priority.highPartitionWeight=0.80
   jms.priority.lowPartitionWeight=0.20
   
   # Receiver queue size (reduced for better priority control)
   jms.consumerConfig.receiverQueueSize=50
   ```

3. **Add feature flag support**
   - Check `jms.useExtendedConsumer` before using extended consumer
   - Fall back to standard consumer if disabled
   - Log configuration on startup

#### Integration Points
```java
private MessageConsumer createExtendedConsumer(Destination destination) {
    log.info("Creating extended consumer with priority support");
    
    ConsumerBuilder<byte[]> builder = pulsarClient.newConsumer()
        .topic(getTopicName(destination))
        .subscriptionName(getSubscriptionName(destination))
        .receiverQueueSize(config.getInt("jms.consumerConfig.receiverQueueSize", 50));
    
    // Configure weights
    double highWeight = config.getDouble("jms.priority.highPartitionWeight", 0.80);
    double lowWeight = config.getDouble("jms.priority.lowPartitionWeight", 0.20);
    
    builder.property("highPartitionWeight", highWeight);
    builder.property("lowPartitionWeight", lowWeight);
    
    // Use extended consumer implementation
    builder.consumerImpl(PriorityMultiTopicsConsumerImpl.class);
    
    return new PulsarMessageConsumer(session, builder.subscribe(), destination);
}
```

#### Acceptance Criteria
- [ ] Extended consumer integrated
- [ ] Configuration properties work
- [ ] Feature flag toggles consumer type
- [ ] Backward compatible

---

### Day 10: Integration Testing

#### Test Scenarios

**Test 1: End-to-End Weighted Selection**
```java
@Test
public void testEndToEndWeightedSelection() throws Exception {
    // Setup with extended consumer
    Map<String, Object> config = new HashMap<>();
    config.put("jms.useExtendedConsumer", true);
    config.put("jms.priority.highPartitionWeight", 0.80);
    config.put("jms.priority.lowPartitionWeight", 0.20);
    
    PulsarConnectionFactory factory = new PulsarConnectionFactory(config);
    Connection conn = factory.createConnection();
    Session session = conn.createSession();
    
    Queue queue = session.createQueue("persistent://public/default/priority-test");
    MessageConsumer consumer = session.createConsumer(queue);
    
    // Send 100 messages to each partition
    sendMessagesToPartition(0, 100);  // LOW
    sendMessagesToPartition(1, 100);  // HIGH
    
    // Receive 100 messages
    int highCount = 0;
    int lowCount = 0;
    for (int i = 0; i < 100; i++) {
        Message msg = consumer.receive(1000);
        if (isFromHighPartition(msg)) highCount++;
        else lowCount++;
    }
    
    // Should be approximately 80/20
    assertTrue(highCount >= 75 && highCount <= 85);
    assertTrue(lowCount >= 15 && lowCount <= 25);
}
```

**Test 2: Receiver Queue Size Impact**
```java
@Test
public void testReceiverQueueSizeReduction() {
    // Test with receiverQueueSize=50 vs 1000
    // Verify priority control improves with smaller queue
}
```

**Test 3: Feature Flag Toggle**
```java
@Test
public void testFeatureFlagDisabled() {
    // With jms.useExtendedConsumer=false
    // Verify standard consumer is used
}
```

**Test 4: Configuration Validation**
```java
@Test
public void testInvalidWeights() {
    // Test with invalid weight values
    // Should use defaults or throw error
}
```

#### Acceptance Criteria
- [ ] End-to-end tests pass
- [ ] Weight distribution verified
- [ ] Feature flag works correctly
- [ ] No errors in logs

---

### Day 11: Performance Testing

#### Benchmarks to Run

**Test 1: Consumer Throughput**
```java
@Test
public void testConsumerThroughput() {
    // Baseline: Standard consumer
    long baseline = measureConsumerThroughput(false);
    
    // With extended consumer
    long withExtended = measureConsumerThroughput(true);
    
    // Should be within 10% of baseline
    assertTrue(withExtended > baseline * 0.9);
}
```

**Test 2: Latency Impact**
```java
@Test
public void testLatencyImpact() {
    // Measure receive latency
    // Compare standard vs extended consumer
}
```

**Test 3: Memory Usage**
```java
@Test
public void testMemoryUsage() {
    // Monitor receiver queue memory
    // Verify reduced queue size helps
}
```

**Test 4: Priority Accuracy**
```java
@Test
public void testPriorityAccuracy() {
    // Send mixed priority messages
    // Measure how many high-priority received first
    // Target: >80% accuracy (vs current 60-70%)
}
```

#### Performance Targets
- **Throughput**: >90% of baseline (>45K msgs/sec)
- **Latency p50**: <10ms overhead
- **Latency p99**: <30ms overhead
- **Priority Accuracy**: >80% (improvement from 60-70%)
- **Memory**: Reduced by ~50% (queue size 50 vs 1000)

#### Acceptance Criteria
- [ ] Throughput within 10% of baseline
- [ ] Latency overhead acceptable
- [ ] Priority accuracy improved
- [ ] Memory usage reduced
- [ ] Performance report documented

---

### Day 12: Documentation & Review

#### Documentation Tasks

1. **JavaDoc Comments**
   - Add class-level documentation
   - Document all public methods
   - Add usage examples

2. **Configuration Guide**
   ```markdown
   ## Extended Consumer Configuration
   
   Enable weighted partition selection:
   
   ```properties
   # Enable extended consumer
   jms.useExtendedConsumer=true
   
   # Configure weights (must sum to 1.0)
   jms.priority.highPartitionWeight=0.80
   jms.priority.lowPartitionWeight=0.20
   
   # Reduce receiver queue for better priority control
   jms.consumerConfig.receiverQueueSize=50
   ```
   
   **Weight Configuration**:
   - `highPartitionWeight`: Percentage of selections from HIGH partition (0.0-1.0)
   - `lowPartitionWeight`: Percentage of selections from LOW partition (0.0-1.0)
   - Weights are normalized automatically
   
   **Receiver Queue Size**:
   - Smaller queue = better priority control
   - Recommended: 50 (vs default 1000)
   - Trade-off: Slightly lower throughput for better priority
   ```

3. **Architecture Documentation**
   - Explain weighted selection algorithm
   - Document integration points
   - Add sequence diagrams

4. **Migration Guide**
   - How to enable for existing applications
   - Performance considerations
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
- [ ] Ready for Phase 3

---

## 📊 Deliverables

### Code Artifacts
- ✅ `PartitionWeightSelector.java` (100-150 lines)
- ✅ `PriorityMultiTopicsConsumerImpl.java` (300-400 lines)
- ✅ Unit tests (30+ tests, 800+ lines)
- ✅ Integration tests (5+ tests, 400+ lines)
- ✅ Configuration updates

### Documentation
- ✅ JavaDoc comments (100% coverage)
- ✅ Configuration guide
- ✅ Architecture documentation
- ✅ Migration guide
- ✅ Performance report

### Test Results
- ✅ Unit test report (100% pass)
- ✅ Integration test report (100% pass)
- ✅ Performance benchmark report
- ✅ Code coverage report (>85%)

---

## 🚨 Risks & Mitigation

### Risk 1: Throughput Degradation >10%
**Impact**: High  
**Probability**: Medium  
**Mitigation**:
- Optimize partition selection (minimize overhead)
- Tune receiver queue size
- Consider async selection if needed

### Risk 2: Weight Distribution Inaccurate
**Impact**: High  
**Probability**: Low  
**Mitigation**:
- Extensive testing with different weights
- Statistical validation (chi-square test)
- Monitoring in production

### Risk 3: Integration Complexity
**Impact**: Medium  
**Probability**: Medium  
**Mitigation**:
- Detailed integration plan
- Feature flag for safe rollout
- Extensive testing

### Risk 4: Backward Compatibility Issues
**Impact**: High  
**Probability**: Low  
**Mitigation**:
- Feature flag (default: disabled)
- Comprehensive testing with existing apps
- Clear migration guide

---

## ✅ Phase 2 Completion Checklist

### Code Complete
- [ ] PartitionWeightSelector implemented
- [ ] PriorityMultiTopicsConsumerImpl implemented
- [ ] All unit tests passing
- [ ] Integration tests passing
- [ ] Performance tests passing
- [ ] Code review approved

### Documentation Complete
- [ ] JavaDoc 100% coverage
- [ ] Configuration guide written
- [ ] Architecture documentation written
- [ ] Migration guide written
- [ ] Performance report documented

### Quality Gates
- [ ] Code coverage >85%
- [ ] No critical bugs
- [ ] Performance within targets
- [ ] Backward compatible
- [ ] Priority accuracy improved

### Ready for Phase 3
- [ ] All acceptance criteria met
- [ ] Stakeholder approval received
- [ ] Phase 3 dependencies identified
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
- End of Week 4
- Go/No-Go decision for Phase 3
- Stakeholder sign-off required

---

**Previous Phase**: [Phase 1 - Producer-Side Routing](./01-PHASE1-PRODUCER-ROUTING.md)  
**Next Phase**: [Phase 3 - Aging Mechanism](./03-PHASE3-AGING-MECHANISM.md)