# Deprecated FedEx Work Documentation

This directory contains documentation from the FedEx priority queue improvement project that has been deprecated but preserved for historical reference.

## Contents

### Solution Documentation
- **[SOLUTION_6_COMPLETE_DETAILED_GUIDE.md](SOLUTION_6_COMPLETE_DETAILED_GUIDE.md)** - The complete detailed guide for Solution 6 (Two-Partition with JMSXGroupID Affinity) that informed the Phase 1 implementation

### Implementation Plans
- **[plans/](plans/)** - Directory containing all phase implementation plans:
  - `00-MASTER-IMPLEMENTATION-TIMELINE.md` - Master timeline for all phases
  - `01-PHASE1-PRODUCER-ROUTING.md` - Phase 1: Producer-side routing implementation plan
  - `02-PHASE2-CONSUMER-EXTENSION.md` - Phase 2: Consumer extension plan
  - `03-PHASE3-AGING-MECHANISM.md` - Phase 3: Aging mechanism plan
  - `04-PHASE4-INTEGRATION-TESTING.md` - Phase 4: Integration testing plan
  - `05-PHASE5-MONITORING-METRICS.md` - Phase 5: Monitoring and metrics plan

### Progress Tracking
- **[PHASE1_BACKLOG_STATUS.md](PHASE1_BACKLOG_STATUS.md)** - Detailed backlog document tracking Phase 1 implementation progress (70% complete as of last update)

## Implementation Status

**Phase 1 (Producer-Side Routing):** 70% Complete
- ✅ Days 1-7: Core implementation, unit tests, and integration complete
- ⏳ Days 8-10: Integration testing, performance testing, and documentation pending

## Key Implementation Files

The actual implementation can be found in:
- `pulsar-jms/src/main/java/com/datastax/oss/pulsar/jms/PriorityGroupPartitionRouter.java`
- `pulsar-jms/src/test/java/com/datastax/oss/pulsar/jms/PriorityGroupPartitionRouterTest.java`
- `pulsar-jms/src/main/java/com/datastax/oss/pulsar/jms/PulsarConnectionFactory.java`

## Why Deprecated?

These documents were part of the initial FedEx project planning and implementation phase. They have been moved to this deprecated folder to:
1. Reduce confusion in the main repository
2. Preserve historical context and decision-making process
3. Keep essential reference material accessible but organized

## Reference Only

These documents are for reference purposes only and may not reflect the current state of the codebase. For current documentation, please refer to the main project README and inline code documentation.