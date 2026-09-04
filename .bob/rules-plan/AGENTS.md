# Project Architecture Rules (Non-Obvious Only)

## Multi-Module Build Architecture
- **Build order critical:** `pulsar-jms-filters` → `pulsar-jms` (NAR dependency)
- **NAR deployment:** Filter NAR files copied to test classpath via maven-antrun-plugin
- **Module isolation:** Each module can be built independently after parent install

## Priority Queue Architecture
- **Two-tier design:** `MessagePriorityGrowableArrayBlockingQueue` wraps `PriorityBlockingQueue`
- **Partition mapping:** Non-linear distribution (1/4 low, 1/2 mid, 1/4 high) vs linear (even spread)
- **Special cases:** 2-3 partition configs use hardcoded logic, not formulas
- **Non-deterministic:** `ThreadLocalRandom` used for partition selection within priority buckets

## JMS-Pulsar Mapping Constraints
- **Queue subscriptions:** Subscription name embedded in topic name, extracted via `PulsarQueue.extractSubscriptionName()`
- **Temporary destinations:** UUID-based naming, session-scoped lifecycle
- **Selector filtering:** Two-tier (server-side NAR + client-side fallback)
- **Transaction coordinator:** Must be explicitly enabled in Pulsar config

## Test Infrastructure Architecture
- **Container per test class:** Fresh Pulsar container for each test class (isolation over speed)
- **DataStax Luna Streaming:** Tests use `datastax/lunastreaming:4.0.7_2`, not Apache Pulsar
- **Filter deployment:** NAR files must exist in `target/classes/filters/` before tests run
- **Build dependency:** Parent `mvn install` required before submodule tests work

## Code Organization Patterns
- **Utils class:** Central utility methods for exception handling, async operations, deep copy
- **Session context:** Message listeners must execute in session context via `Utils.executeMessageListenerInSessionContext()`
- **Error handling:** All checked exceptions converted to JMSException via `Utils.handleException()`
- **Async unwrapping:** CompletableFuture results unwrapped via `Utils.get()` with proper exception handling

## Performance Considerations
- **Priority queue overhead:** Custom comparator sorts by priority DESC, then messageId ASC
- **Partition randomization:** ThreadLocalRandom adds non-determinism within priority buckets
- **Client-side filtering:** Fallback when server-side NAR unavailable (performance impact)