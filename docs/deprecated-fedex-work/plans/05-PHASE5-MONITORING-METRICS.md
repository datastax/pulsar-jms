# Phase 5: Monitoring & Metrics Implementation Plan

**Duration**: 1 week (Week 7)  
**Owner**: DevOps Team + Backend Team  
**Status**: Not Started  
**Dependencies**: Phases 1-4 Complete

---

## 🎯 Phase Objectives

### Primary Goals
1. Add JMX metrics for all components
2. Create Grafana dashboard for visualization
3. Set up alerts for critical conditions
4. Create operations runbook
5. Prepare for production deployment

### Success Criteria
- ✅ All metrics visible in Grafana
- ✅ Alerts trigger correctly
- ✅ Runbook tested and validated
- ✅ Production deployment approved
- ✅ Team trained on monitoring

---

## 📊 Metrics to Implement

### Producer Metrics (PriorityGroupPartitionRouter)

```java
public class PriorityGroupPartitionRouterMetrics {
    
    // Message routing metrics
    private final Counter totalMessagesRouted;
    private final Counter lowPartitionMessages;
    private final Counter highPartitionMessages;
    
    // Group affinity metrics
    private final Gauge totalGroups;
    private final Gauge groupsInLowPartition;
    private final Gauge groupsInHighPartition;
    
    // Performance metrics
    private final Histogram routingLatency;
    
    public void recordRouting(int partition, long latencyNanos) {
        totalMessagesRouted.inc();
        if (partition == 0) {
            lowPartitionMessages.inc();
        } else {
            highPartitionMessages.inc();
        }
        routingLatency.observe(latencyNanos / 1_000_000.0); // Convert to ms
    }
}
```

**JMX MBean**:
```
pulsar.jms:type=PriorityRouter,name=routing
- TotalMessagesRouted
- LowPartitionMessages
- HighPartitionMessages
- TotalGroups
- GroupsInLowPartition
- GroupsInHighPartition
- RoutingLatencyP50
- RoutingLatencyP95
- RoutingLatencyP99
```

---

### Consumer Metrics (PriorityMultiTopicsConsumerImpl)

```java
public class PriorityConsumerMetrics {
    
    // Partition selection metrics
    private final Counter totalSelections;
    private final Counter highPartitionSelections;
    private final Counter lowPartitionSelections;
    
    // Weight distribution metrics
    private final Gauge currentHighWeight;
    private final Gauge currentLowWeight;
    private final Gauge actualHighPercentage;
    private final Gauge actualLowPercentage;
    
    // Performance metrics
    private final Histogram receiveLatency;
    private final Counter receiveTimeouts;
    
    public void recordSelection(int partition, long latencyNanos) {
        totalSelections.inc();
        if (partition == 1) {
            highPartitionSelections.inc();
        } else {
            lowPartitionSelections.inc();
        }
        receiveLatency.observe(latencyNanos / 1_000_000.0);
    }
}
```

**JMX MBean**:
```
pulsar.jms:type=PriorityConsumer,name=selection
- TotalSelections
- HighPartitionSelections
- LowPartitionSelections
- ActualHighPercentage
- ActualLowPercentage
- ReceiveLatencyP50
- ReceiveLatencyP95
- ReceiveLatencyP99
- ReceiveTimeouts
```

---

### Aging Metrics (AgingTracker)

```java
public class AgingTrackerMetrics {
    
    // Aging event metrics
    private final Counter totalAgingEvents;
    private final Counter timeBasedAgingEvents;
    private final Counter countBasedAgingEvents;
    
    // Partition health metrics
    private final Gauge partition0TimeSinceLastRead;
    private final Gauge partition0SkipCount;
    private final Gauge partition1TimeSinceLastRead;
    private final Gauge partition1SkipCount;
    
    // Statistics
    private final Counter partition0TotalReads;
    private final Counter partition1TotalReads;
    
    public void recordAgingEvent(int partition, String type) {
        totalAgingEvents.inc();
        if ("time".equals(type)) {
            timeBasedAgingEvents.inc();
        } else {
            countBasedAgingEvents.inc();
        }
    }
}
```

**JMX MBean**:
```
pulsar.jms:type=AgingTracker,name=aging
- TotalAgingEvents
- TimeBasedAgingEvents
- CountBasedAgingEvents
- Partition0TimeSinceLastRead
- Partition0SkipCount
- Partition0TotalReads
- Partition1TimeSinceLastRead
- Partition1SkipCount
- Partition1TotalReads
```

---

### Priority Accuracy Metrics

```java
public class PriorityAccuracyMetrics {
    
    // Accuracy tracking
    private final Histogram priorityAccuracy;
    private final Counter totalMessagesReceived;
    private final Counter highPriorityReceivedFirst;
    private final Counter lowPriorityReceivedFirst;
    
    // Window-based accuracy (last 1000 messages)
    private final CircularBuffer<Boolean> recentAccuracy;
    
    public void recordMessageReceived(int priority, int expectedPriority) {
        totalMessagesReceived.inc();
        boolean correct = (priority == expectedPriority);
        
        if (correct && priority >= 5) {
            highPriorityReceivedFirst.inc();
        } else if (correct && priority < 5) {
            lowPriorityReceivedFirst.inc();
        }
        
        recentAccuracy.add(correct);
        double accuracy = calculateAccuracy();
        priorityAccuracy.observe(accuracy);
    }
}
```

**JMX MBean**:
```
pulsar.jms:type=PriorityAccuracy,name=accuracy
- CurrentAccuracy (last 1000 messages)
- TotalMessagesReceived
- HighPriorityReceivedFirst
- LowPriorityReceivedFirst
- AccuracyP50
- AccuracyP95
- AccuracyP99
```

---

## 📈 Grafana Dashboard

### Dashboard Layout

```
┌─────────────────────────────────────────────────────────────┐
│  Solution 6: Priority Queue Monitoring                      │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Priority     │  │ Throughput   │  │ Latency      │      │
│  │ Accuracy     │  │              │  │              │      │
│  │   95.8%      │  │  47.2K/sec   │  │  P99: 120ms  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ Priority Accuracy Over Time                          │    │
│  │ [Line graph showing accuracy trending]               │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                               │
│  ┌──────────────────────┐  ┌──────────────────────────┐    │
│  │ Partition Selection  │  │ Aging Events             │    │
│  │ [Pie chart 80/20]    │  │ [Bar chart over time]    │    │
│  └──────────────────────┘  └──────────────────────────┘    │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ Message Flow                                         │    │
│  │ [Sankey diagram: Producer → Partitions → Consumer]  │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                               │
│  ┌──────────────────────┐  ┌──────────────────────────┐    │
│  │ Partition Health     │  │ System Resources         │    │
│  │ [Status indicators]  │  │ [CPU, Memory, Network]   │    │
│  └──────────────────────┘  └──────────────────────────┘    │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### Panel Definitions

**Panel 1: Priority Accuracy**
```json
{
  "title": "Priority Accuracy",
  "type": "stat",
  "targets": [{
    "expr": "pulsar_jms_priority_accuracy_current"
  }],
  "thresholds": [
    { "value": 0, "color": "red" },
    { "value": 90, "color": "yellow" },
    { "value": 95, "color": "green" }
  ]
}
```

**Panel 2: Throughput**
```json
{
  "title": "Message Throughput",
  "type": "graph",
  "targets": [{
    "expr": "rate(pulsar_jms_total_messages_routed[1m])"
  }],
  "yaxis": { "label": "Messages/sec" }
}
```

**Panel 3: Partition Selection Distribution**
```json
{
  "title": "Partition Selection (80/20 Target)",
  "type": "piechart",
  "targets": [
    {
      "expr": "pulsar_jms_high_partition_selections",
      "legendFormat": "HIGH (80%)"
    },
    {
      "expr": "pulsar_jms_low_partition_selections",
      "legendFormat": "LOW (20%)"
    }
  ]
}
```

**Panel 4: Aging Events**
```json
{
  "title": "Aging Events (Starvation Prevention)",
  "type": "graph",
  "targets": [
    {
      "expr": "rate(pulsar_jms_time_based_aging_events[5m])",
      "legendFormat": "Time-based"
    },
    {
      "expr": "rate(pulsar_jms_count_based_aging_events[5m])",
      "legendFormat": "Count-based"
    }
  ]
}
```

**Panel 5: Partition Health**
```json
{
  "title": "Partition Health",
  "type": "table",
  "targets": [
    {
      "expr": "pulsar_jms_partition0_time_since_last_read",
      "format": "table"
    },
    {
      "expr": "pulsar_jms_partition0_skip_count",
      "format": "table"
    }
  ]
}
```

---

## 🚨 Alerts Configuration

### Alert 1: Low Priority Accuracy
```yaml
alert: LowPriorityAccuracy
expr: pulsar_jms_priority_accuracy_current < 90
for: 5m
labels:
  severity: warning
annotations:
  summary: "Priority accuracy below 90%"
  description: "Current accuracy: {{ $value }}%. Target: >95%"
```

### Alert 2: Partition Starvation
```yaml
alert: PartitionStarvation
expr: pulsar_jms_partition0_time_since_last_read > 600000
for: 1m
labels:
  severity: critical
annotations:
  summary: "LOW partition not read for >10 minutes"
  description: "Potential starvation detected"
```

### Alert 3: Excessive Aging Events
```yaml
alert: ExcessiveAgingEvents
expr: rate(pulsar_jms_total_aging_events[1h]) > 100
for: 5m
labels:
  severity: warning
annotations:
  summary: "Too many aging events"
  description: "Aging events: {{ $value }}/hour. May indicate configuration issue"
```

### Alert 4: Throughput Degradation
```yaml
alert: ThroughputDegradation
expr: rate(pulsar_jms_total_messages_routed[5m]) < 40000
for: 10m
labels:
  severity: warning
annotations:
  summary: "Throughput below 40K msgs/sec"
  description: "Current: {{ $value }} msgs/sec. Target: >45K"
```

### Alert 5: High Latency
```yaml
alert: HighLatency
expr: pulsar_jms_receive_latency_p99 > 200
for: 5m
labels:
  severity: warning
annotations:
  summary: "P99 latency above 200ms"
  description: "Current P99: {{ $value }}ms. Target: <150ms"
```

---

## 📚 Operations Runbook

### Runbook: Low Priority Accuracy

**Symptom**: Priority accuracy drops below 90%

**Diagnosis**:
1. Check partition selection distribution
   - Should be ~80/20
   - If skewed, check weight configuration
2. Check aging events
   - If too frequent, may indicate backlog
3. Check receiver queue size
   - Should be 50 (not 1000)

**Resolution**:
1. Verify configuration:
   ```properties
   jms.priority.highPartitionWeight=0.80
   jms.priority.lowPartitionWeight=0.20
   jms.consumerConfig.receiverQueueSize=50
   ```
2. Check for backlogs in partitions
3. Restart consumers if needed
4. Monitor for 15 minutes

---

### Runbook: Partition Starvation

**Symptom**: LOW partition not read for >10 minutes

**Diagnosis**:
1. Check aging configuration:
   ```properties
   jms.priority.agingTimeThreshold=300000  # 5 min
   jms.priority.agingCountThreshold=100
   ```
2. Check if aging events are occurring
3. Check consumer health

**Resolution**:
1. If aging disabled, enable it
2. If aging threshold too high, lower it:
   ```properties
   jms.priority.agingTimeThreshold=180000  # 3 min
   jms.priority.agingCountThreshold=50
   ```
3. Restart consumers
4. Monitor LOW partition reads

---

### Runbook: Excessive Aging Events

**Symptom**: >100 aging events per hour

**Diagnosis**:
1. Check message load distribution
   - Too many HIGH priority messages?
2. Check aging thresholds
   - Too aggressive?
3. Check consumer throughput
   - Consumers keeping up?

**Resolution**:
1. If load imbalanced, adjust weights:
   ```properties
   jms.priority.highPartitionWeight=0.70
   jms.priority.lowPartitionWeight=0.30
   ```
2. If thresholds too aggressive, increase:
   ```properties
   jms.priority.agingTimeThreshold=600000  # 10 min
   jms.priority.agingCountThreshold=200
   ```
3. Monitor for improvement

---

### Runbook: Throughput Degradation

**Symptom**: Throughput <40K msgs/sec

**Diagnosis**:
1. Check system resources (CPU, memory, network)
2. Check partition selection overhead
3. Check receiver queue size
4. Check for errors in logs

**Resolution**:
1. If CPU high, scale horizontally
2. If receiver queue too small, increase:
   ```properties
   jms.consumerConfig.receiverQueueSize=100
   ```
3. Check for network issues
4. Review recent changes

---

## 📋 Implementation Tasks

### Day 1-2: Metrics Implementation
- [ ] Add JMX MBeans to all components
- [ ] Implement metrics collection
- [ ] Test metrics locally
- [ ] Document metric meanings

### Day 3-4: Grafana Dashboard
- [ ] Create dashboard JSON
- [ ] Configure data sources
- [ ] Test all panels
- [ ] Add documentation

### Day 5: Alerts Setup
- [ ] Configure Prometheus alerts
- [ ] Test alert triggers
- [ ] Set up notification channels
- [ ] Document alert responses

### Day 6: Runbook Creation
- [ ] Write runbooks for each alert
- [ ] Test runbook procedures
- [ ] Train operations team
- [ ] Create quick reference guide

### Day 7: Production Readiness
- [ ] Final review of all monitoring
- [ ] Load test with monitoring
- [ ] Sign-off from operations
- [ ] Deployment plan finalized

---

## ✅ Acceptance Criteria

### Metrics
- [ ] All JMX metrics exposed
- [ ] Metrics accurate and reliable
- [ ] No performance impact from metrics
- [ ] Documentation complete

### Dashboard
- [ ] All panels working
- [ ] Real-time updates
- [ ] Intuitive layout
- [ ] Mobile-friendly

### Alerts
- [ ] All alerts configured
- [ ] Alerts trigger correctly
- [ ] No false positives
- [ ] Notification channels working

### Runbooks
- [ ] Runbooks complete
- [ ] Procedures tested
- [ ] Team trained
- [ ] Quick reference available

### Production Readiness
- [ ] All monitoring in place
- [ ] Operations team trained
- [ ] Deployment plan approved
- [ ] Rollback plan documented

---

## 📞 Communication

### Daily Updates
- Monitoring setup progress
- Issues and blockers
- Training schedule

### End of Phase Review
- Demo dashboard and alerts
- Review runbooks
- Production deployment approval

---

**Previous Phase**: [Phase 4 - Integration Testing](./04-PHASE4-INTEGRATION-TESTING.md)  
**Related Documents**: 
- [Testing Strategy](./06-TESTING-STRATEGY.md)
- [Configuration Reference](./07-CONFIGURATION-REFERENCE.md)