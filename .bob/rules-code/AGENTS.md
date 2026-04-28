# Project Coding Rules (Non-Obvious Only)

## Priority Queue Implementation Details
- **Custom comparator:** `MessagePriorityGrowableArrayBlockingQueue` uses custom comparator that sorts by priority DESC, then messageId ASC
- **Priority extraction:** JMS priority extracted from message properties, not Pulsar message priority field
- **Partition mapping:** `Utils.mapPriorityToPartition()` has special hardcoded cases for 2-3 partitions (see lines 369-383, 426-437)
- **Non-deterministic:** Uses `ThreadLocalRandom` for partition selection within priority buckets (lines 394, 457)

## Test Dependencies
- **NAR files required:** Tests fail without NAR files in `target/classes/filters/` and `target/classes/interceptors/`
- **Build order:** Must run `mvn install` on parent before `mvn test -pl pulsar-jms` works
- **Container image:** Tests hardcoded to `datastax/lunastreaming:4.0.7_2` (not standard Apache Pulsar)

## JMS-Pulsar Mapping
- **Queue subscription names:** Use `PulsarQueue.extractSubscriptionName()` - don't parse manually
- **Selector filtering:** Client-side filtering in `PulsarMessageConsumer.requiresClientSideFiltering()` when server-side unavailable
- **Message properties:** JMS properties stored in Pulsar message properties map with `JMS` prefix

## Code Patterns
- **Error handling:** Use `Utils.handleException()` to convert checked exceptions to JMSException
- **Async operations:** Use `Utils.get()` to unwrap CompletableFuture with proper exception handling
- **Session context:** Use `Utils.executeMessageListenerInSessionContext()` for listener callbacks
- **Deep copy:** Use `Utils.deepCopyObject()` for message body cloning (handles serialization)

## Module-Specific Notes
- **pulsar-jms-filters:** Builds NAR file, not JAR - required for broker-side filtering
- **pulsar-client-original:** Main code uses this, not `pulsar-client-shaded` (despite module existing)