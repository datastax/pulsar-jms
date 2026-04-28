# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Build & Test Commands

**Run all tests:**
```bash
mvn clean install
```

**Run single test class:**
```bash
mvn test -Dtest=PriorityTest -pl pulsar-jms
```

**Run single test method:**
```bash
mvn test -Dtest=PriorityTest#basicTest -pl pulsar-jms
```

**Run TCK tests (requires JDK 8, Docker, Ant):**
```bash
mvn clean install -Prun-tck
```

**Format code (auto-applied on build):**
```bash
mvn fmt:format
```

**Check SpotBugs:**
```bash
mvn spotbugs:check
```

## Critical Non-Obvious Patterns

### Priority Queue Implementation
- **Custom queue:** `MessagePriorityGrowableArrayBlockingQueue` wraps `PriorityBlockingQueue` to handle JMS priority (0-9)
- **Priority mapping:** Two modes exist - "linear" and "non-linear" (see `Utils.mapPriorityToPartition()`)
  - Linear: Distributes priorities evenly across partitions with randomization within buckets
  - Non-linear: Allocates 1/4 partitions to low (0-3), 1/2 to mid (4), 1/4 to high (5-9)
- **Partition selection:** Uses `ThreadLocalRandom` for distribution within priority buckets (not deterministic)
- **Special cases:** 2-3 partition configurations have hardcoded logic in both mapping functions

### Test Infrastructure
- **Container setup:** Tests use `PulsarContainerExtension` with DataStax Luna Streaming image (`datastax/lunastreaming:4.0.7_2`)
- **Filter deployment:** Tests require NAR files in `target/classes/filters/` and `target/classes/interceptors/`
  - Built by `pulsar-jms-filters` module and copied via maven-antrun-plugin
  - Must run `mvn install` on parent before running tests in `pulsar-jms` module
- **Test isolation:** Each test class gets fresh Pulsar container (slow but isolated)

### JMS-Pulsar Mapping Quirks
- **Queue subscriptions:** Extract subscription name from topic name using `PulsarQueue.extractSubscriptionName()`
- **Temporary destinations:** Must call `delete()` explicitly; tied to session lifecycle
- **Selector support:** Server-side filtering requires broker-side NAR deployment (not just client-side)
- **Transaction coordinator:** Must be explicitly enabled in Pulsar config for transactional tests

### Code Style (Enforced)
- **Formatter:** Google Java Format via `fmt-maven-plugin` (auto-formats on build)
- **License headers:** Apache 2.0 header required on all `.java`, `.xml`, `.properties` files (auto-added)
- **Lombok:** Used extensively - `@Slf4j`, `@AllArgsConstructor`, `@SneakyThrows` common
- **Spotbugs:** Runs on `verify` phase; omits `FindReturnRef` and `ConstructorThrow` visitors

### Module Dependencies
- **Build order matters:** `pulsar-jms-filters` must build before `pulsar-jms` (NAR file dependency)
- **Shaded client:** `pulsar-client-shaded` module exists but main code uses `pulsar-client-original`
- **Admin API:** Separate `pulsar-jms-admin-api` and `pulsar-jms-admin-ext` modules for CLI extensions

## Running Tests in Specific Modules

Always use `-pl` flag to target specific module:
```bash
mvn test -pl pulsar-jms
mvn test -pl resource-adapter
mvn test -pl pulsar-jms-filters
```

## JDK Requirements
- **Build:** JDK 11+ (maven.compiler.source=11)
- **Runtime:** JDK 8+ (java.release.version=8)
- **TCK tests:** Requires JDK 8 specifically
- **Test JVM args:** Extensive `--add-opens` flags required for JDK 11+ (see pom.xml test.additional.args)