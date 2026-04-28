# Project Documentation Rules (Non-Obvious Only)

## Module Structure
- **pulsar-jms:** Core JMS implementation over Pulsar client
- **pulsar-jms-filters:** Builds NAR (not JAR) for broker-side message filtering
- **pulsar-jms-all:** Fat JAR with all dependencies bundled
- **resource-adapter:** JakartaEE/JavaEE resource adapter implementation
- **tck-executor:** JMS TCK test runner (requires JDK 8, Docker, Ant)

## Test Organization
- **Test container:** Uses DataStax Luna Streaming (`datastax/lunastreaming:4.0.7_2`), not Apache Pulsar
- **Filter deployment:** Tests copy NAR files from `pulsar-jms-filters/target/` to `pulsar-jms/target/classes/filters/`
- **Build dependency:** Must build parent project before running tests in submodules

## Priority Queue Architecture
- **Two mapping modes:** "linear" (even distribution) vs "non-linear" (1/4 low, 1/2 mid, 1/4 high)
- **Special partition cases:** 2-3 partition configurations have hardcoded logic (not formula-based)
- **Non-deterministic:** Uses `ThreadLocalRandom` for partition selection within priority ranges

## JMS-Pulsar Mapping
- **Queue topics:** JMS queues map to Pulsar topics with subscription names extracted from topic name
- **Temporary destinations:** Created with UUID suffix, must be explicitly deleted
- **Selector filtering:** Can be server-side (NAR required) or client-side (fallback)
- **Transaction support:** Requires transaction coordinator enabled in Pulsar config

## Documentation Locations
- **Official docs:** https://docs.datastax.com/en/streaming/starlight-for-jms/
- **JMS spec:** Jakarta EE Messaging 2.0/3.0 specifications
- **Pulsar docs:** https://pulsar.apache.org
- **Internal docs:** INTERNALS.md, README.md in project root