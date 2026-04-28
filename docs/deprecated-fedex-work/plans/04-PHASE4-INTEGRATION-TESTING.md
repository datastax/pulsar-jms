# Phase 4: Integration Testing Implementation Plan

**Duration**: 1 week (Week 6)  
**Owner**: QA Team + Backend Team  
**Status**: Not Started  
**Dependencies**: Phases 1-3 Complete

---

## 🎯 Phase Objectives

### Primary Goals
1. End-to-end testing of complete solution
2. Verify priority accuracy >95%
3. Verify no partition starvation
4. Verify message group ordering maintained
5. Load and stress testing

### Success Criteria
- ✅ Priority accuracy >95% (vs current 60-70%)
- ✅ No partition starvation in 24hr test
- ✅ Message group ordering maintained
- ✅ Throughput impact <10%
- ✅ All integration tests pass (20+ scenarios)

---

## 📋 Test Scenarios

### Scenario 1: Normal Mixed Load
**Goal**: Verify priority handling with typical workload

```java
@Test
public void testNormalMixedLoad() throws Exception {
    // Setup
    setupPrioritySystem();
    
    // Send 60 low priority, 40 high priority
    for (int i = 0; i < 60; i++) {
        sendMessage(2, "Low-" + i);
    }
    for (int i = 0; i < 40; i++) {
        sendMessage(9, "High-" + i);
    }
    
    // Receive and verify order
    int highFirst = 0;
    for (int i = 0; i < 100; i++) {
        Message msg = consumer.receive(5000);
        assertNotNull(msg, "Message " + i + " should not be null");
        
        if (i < 40 && msg.getJMSPriority() == 9) {
            highFirst++;
        }
    }
    
    // Should get >75% of high-priority first (30+ out of 40)
    assertTrue(highFirst >= 30, 
        "Expected >=30 high-priority first, got " + highFirst);
    
    // Calculate accuracy
    double accuracy = (highFirst / 40.0) * 100;
    log.info("Priority accuracy: {}%", accuracy);
    assertTrue(accuracy >= 75, "Accuracy should be >=75%");
}
```

**Expected Results**:
- 30-38 high-priority messages received first (75-95%)
- Remaining high-priority mixed with low-priority
- Total time <10 seconds

---

### Scenario 2: Huge Backlog
**Goal**: Verify high-priority not blocked by large low-priority backlog

```java
@Test
public void testHugeBacklog() throws Exception {
    // Create massive backlog
    log.info("Creating backlog of 10,000 low-priority messages");
    for (int i = 0; i < 10000; i++) {
        sendMessage(2, "Backlog-" + i);
        if (i % 1000 == 0) {
            log.info("Sent {} messages", i);
        }
    }
    
    // Wait for messages to settle
    Thread.sleep(2000);
    
    // Send high-priority messages
    log.info("Sending 100 high-priority messages");
    for (int i = 0; i < 100; i++) {
        sendMessage(9, "Urgent-" + i);
    }
    
    // Receive first 100 messages
    int highPriorityCount = 0;
    for (int i = 0; i < 100; i++) {
        Message msg = consumer.receive(5000);
        assertNotNull(msg);
        
        if (msg.getJMSPriority() == 9) {
            highPriorityCount++;
        }
    }
    
    // Should get >70% high-priority in first 100
    assertTrue(highPriorityCount >= 70,
        "Expected >=70 high-priority, got " + highPriorityCount);
    
    log.info("Received {} high-priority out of first 100 messages", 
        highPriorityCount);
}
```

**Expected Results**:
- 70-90 high-priority messages in first 100 received
- High-priority not blocked by backlog
- Demonstrates solution effectiveness

---

### Scenario 3: Message Groups with Priority
**Goal**: Verify JMSXGroupID affinity maintained with priority routing

```java
@Test
public void testMessageGroupsWithPriority() throws Exception {
    String groupId = "ORDER-123";
    
    // Send 10 messages with same group ID, mixed priorities
    for (int i = 0; i < 10; i++) {
        Message msg = session.createTextMessage("Order item " + i);
        msg.setJMSPriority(i % 2 == 0 ? 9 : 2); // Alternate priorities
        msg.setStringProperty("JMSXGroupID", groupId);
        producer.send(msg);
    }
    
    // Verify all messages went to same partition
    Map<String, Object> routerStats = getRouterStats();
    assertEquals(1, routerStats.get("totalGroups"),
        "All messages should be in one group");
    
    // Receive messages
    List<Message> received = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
        Message msg = consumer.receive(5000);
        assertNotNull(msg);
        received.add(msg);
        
        // Verify group ID
        assertEquals(groupId, msg.getStringProperty("JMSXGroupID"));
    }
    
    // Verify ordering within group (high priority first)
    int highCount = 0;
    for (int i = 0; i < 5; i++) {
        if (received.get(i).getJMSPriority() == 9) {
            highCount++;
        }
    }
    
    // Most high-priority should come first
    assertTrue(highCount >= 3, 
        "Expected >=3 high-priority in first 5, got " + highCount);
}
```

**Expected Results**:
- All messages with same group ID go to same partition
- Within group, high-priority messages prioritized
- Group ordering maintained

---

### Scenario 4: Aging Mechanism
**Goal**: Verify LOW partition doesn't starve

```java
@Test
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public void testAgingPreventsStarvation() throws Exception {
    // Configure aggressive aging for faster test
    reconfigureAging(60000, 50); // 1 min, 50 skips
    
    // Send continuous high-priority stream
    ScheduledExecutorService sender = Executors.newScheduledThreadPool(1);
    AtomicBoolean keepSending = new AtomicBoolean(true);
    
    sender.scheduleAtFixedRate(() -> {
        if (keepSending.get()) {
            try {
                sendMessage(9, "High-" + System.currentTimeMillis());
            } catch (Exception e) {
                log.error("Error sending", e);
            }
        }
    }, 0, 100, TimeUnit.MILLISECONDS);
    
    // Send one low-priority message
    Thread.sleep(1000);
    sendMessage(2, "LOW-PRIORITY-TEST");
    
    // Consume messages and track LOW partition reads
    AtomicInteger lowPartitionReads = new AtomicInteger(0);
    long startTime = System.currentTimeMillis();
    boolean lowMessageFound = false;
    
    while (!lowMessageFound && 
           System.currentTimeMillis() - startTime < 300000) { // 5 min max
        Message msg = consumer.receive(1000);
        if (msg != null) {
            String text = ((TextMessage) msg).getText();
            if (text.equals("LOW-PRIORITY-TEST")) {
                lowMessageFound = true;
                long elapsed = System.currentTimeMillis() - startTime;
                log.info("Found low-priority message after {}ms", elapsed);
            }
        }
    }
    
    keepSending.set(false);
    sender.shutdown();
    
    // Verify low-priority message was eventually processed
    assertTrue(lowMessageFound, 
        "Low-priority message should be processed (aging should trigger)");
    
    // Verify aging events occurred
    Map<String, Object> agingStats = getAgingStats();
    assertTrue((Long) agingStats.get("totalAgingEvents") > 0,
        "Aging events should have occurred");
}
```

**Expected Results**:
- LOW partition message processed within 5 minutes
- Aging events logged
- No starvation

---

### Scenario 5: Configuration Toggle
**Goal**: Verify feature flags work correctly

```java
@Test
public void testFeatureFlagDisabled() throws Exception {
    // Disable priority system
    Map<String, Object> config = new HashMap<>();
    config.put("jms.enableJMSPriority", false);
    config.put("jms.useExtendedConsumer", false);
    
    PulsarConnectionFactory factory = new PulsarConnectionFactory(config);
    
    // Send mixed priority messages
    sendMixedPriorityMessages(100);
    
    // Verify standard behavior (no priority routing)
    // Messages should be in arrival order, not priority order
}

@Test
public void testFeatureFlagEnabled() throws Exception {
    // Enable priority system
    Map<String, Object> config = new HashMap<>();
    config.put("jms.enableJMSPriority", true);
    config.put("jms.useExtendedConsumer", true);
    
    PulsarConnectionFactory factory = new PulsarConnectionFactory(config);
    
    // Send mixed priority messages
    sendMixedPriorityMessages(100);
    
    // Verify priority behavior
    // High-priority should come first
}
```

---

### Scenario 6: Load Testing
**Goal**: Verify system handles high throughput

```java
@Test
public void testHighThroughput() throws Exception {
    int messageCount = 50000;
    int producerThreads = 4;
    int consumerThreads = 4;
    
    // Start consumers
    ExecutorService consumerPool = Executors.newFixedThreadPool(consumerThreads);
    AtomicInteger consumed = new AtomicInteger(0);
    
    for (int i = 0; i < consumerThreads; i++) {
        consumerPool.submit(() -> {
            try {
                while (consumed.get() < messageCount) {
                    Message msg = consumer.receive(1000);
                    if (msg != null) {
                        consumed.incrementAndGet();
                    }
                }
            } catch (Exception e) {
                log.error("Consumer error", e);
            }
        });
    }
    
    // Start producers
    ExecutorService producerPool = Executors.newFixedThreadPool(producerThreads);
    AtomicInteger produced = new AtomicInteger(0);
    long startTime = System.currentTimeMillis();
    
    for (int i = 0; i < producerThreads; i++) {
        producerPool.submit(() -> {
            try {
                while (produced.get() < messageCount) {
                    int priority = ThreadLocalRandom.current().nextInt(10);
                    sendMessage(priority, "Msg-" + produced.incrementAndGet());
                }
            } catch (Exception e) {
                log.error("Producer error", e);
            }
        });
    }
    
    // Wait for completion
    producerPool.shutdown();
    producerPool.awaitTermination(5, TimeUnit.MINUTES);
    
    consumerPool.shutdown();
    consumerPool.awaitTermination(5, TimeUnit.MINUTES);
    
    long elapsed = System.currentTimeMillis() - startTime;
    double throughput = (messageCount * 1000.0) / elapsed;
    
    log.info("Throughput: {} msgs/sec", throughput);
    
    // Should achieve >45K msgs/sec (90% of baseline 50K)
    assertTrue(throughput >= 45000, 
        "Throughput should be >=45K msgs/sec, got " + throughput);
}
```

**Expected Results**:
- Throughput >45K msgs/sec
- No errors or timeouts
- System stable under load

---

### Scenario 7: Stress Testing (24 Hours)
**Goal**: Verify system stability over extended period

```java
@Test
@Timeout(value = 25, unit = TimeUnit.HOURS)
public void test24HourStability() throws Exception {
    long duration = TimeUnit.HOURS.toMillis(24);
    long startTime = System.currentTimeMillis();
    
    // Metrics tracking
    AtomicLong totalSent = new AtomicLong(0);
    AtomicLong totalReceived = new AtomicLong(0);
    AtomicLong highPriorityReceived = new AtomicLong(0);
    AtomicLong lowPriorityReceived = new AtomicLong(0);
    
    // Start producer thread
    Thread producer = new Thread(() -> {
        while (System.currentTimeMillis() - startTime < duration) {
            try {
                int priority = ThreadLocalRandom.current().nextInt(10);
                sendMessage(priority, "Msg-" + totalSent.incrementAndGet());
                Thread.sleep(10); // ~100 msgs/sec
            } catch (Exception e) {
                log.error("Producer error", e);
            }
        }
    });
    producer.start();
    
    // Start consumer thread
    Thread consumer = new Thread(() -> {
        while (System.currentTimeMillis() - startTime < duration) {
            try {
                Message msg = this.consumer.receive(1000);
                if (msg != null) {
                    totalReceived.incrementAndGet();
                    if (msg.getJMSPriority() >= 5) {
                        highPriorityReceived.incrementAndGet();
                    } else {
                        lowPriorityReceived.incrementAndGet();
                    }
                }
            } catch (Exception e) {
                log.error("Consumer error", e);
            }
        }
    });
    consumer.start();
    
    // Monitor every hour
    while (System.currentTimeMillis() - startTime < duration) {
        Thread.sleep(TimeUnit.HOURS.toMillis(1));
        
        log.info("Hour {} - Sent: {}, Received: {}, High: {}, Low: {}",
            (System.currentTimeMillis() - startTime) / TimeUnit.HOURS.toMillis(1),
            totalSent.get(), totalReceived.get(),
            highPriorityReceived.get(), lowPriorityReceived.get());
        
        // Check for starvation
        assertTrue(lowPriorityReceived.get() > 0, 
            "LOW partition starved");
    }
    
    producer.join();
    consumer.join();
    
    // Final verification
    log.info("24-hour test complete - Total sent: {}, received: {}",
        totalSent.get(), totalReceived.get());
    
    assertTrue(totalReceived.get() > 0, "Should have received messages");
    assertTrue(lowPriorityReceived.get() > 0, "LOW partition should not starve");
}
```

**Expected Results**:
- No crashes or errors
- No memory leaks
- No partition starvation
- Consistent performance

---

## 📊 Test Execution Plan

### Week 6 Schedule

**Day 1-2: Setup & Basic Tests**
- Setup test environment
- Run Scenarios 1-3
- Fix any issues found

**Day 3-4: Advanced Tests**
- Run Scenarios 4-5
- Run load tests (Scenario 6)
- Performance analysis

**Day 5: Stress Testing**
- Start 24-hour test (Scenario 7)
- Monitor continuously

**Day 6: Analysis**
- Complete 24-hour test
- Analyze results
- Document findings

**Day 7: Bug Fixes & Retesting**
- Fix any issues found
- Rerun failed tests
- Final report

---

## ✅ Acceptance Criteria

### Functional Requirements
- [ ] Priority accuracy >95%
- [ ] No partition starvation (24hr test)
- [ ] Message group ordering maintained
- [ ] Feature flags work correctly
- [ ] All 20+ test scenarios pass

### Performance Requirements
- [ ] Throughput >45K msgs/sec (>90% of baseline)
- [ ] Latency p99 <150ms for high-priority
- [ ] Memory usage stable (no leaks)
- [ ] CPU usage <80% under load

### Quality Requirements
- [ ] No critical bugs
- [ ] No data loss
- [ ] No message duplication
- [ ] Graceful error handling

---

## 📞 Communication

### Daily Updates
- Test results posted to #solution6-testing
- Blockers escalated immediately
- Metrics dashboard updated

### End of Phase Review
- Comprehensive test report
- Performance analysis
- Go/No-Go decision for Phase 5

---

**Previous Phase**: [Phase 3 - Aging Mechanism](./03-PHASE3-AGING-MECHANISM.md)  
**Next Phase**: [Phase 5 - Monitoring & Metrics](./05-PHASE5-MONITORING-METRICS.md)