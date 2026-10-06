# 0002: Integration test strategy (Testcontainers, MiniCluster and Compose scripts)

Status: accepted

## Context

The PLAN asks for integration tests with real Kafka, PostgreSQL and MinIO, failure and recovery tests, and a Docker Compose
environment that remains the main local environment. There are three ways to get real infrastructure into a test, and they suit
different things.

## Decision

Three layers, each used for what it is good at.

| Layer | Used for | Why |
|---|---|---|
| **Flink MiniCluster, in the JVM** (`flink-job` tests) | Streaming logic: validation, deduplication, event time, risk, with parallelism above 1 | Recommended by the Flink documentation; fast, deterministic, no containers. |
| **Testcontainers** (`integration-tests` module, `*IT` classes, run by failsafe in `mvn verify`) | Everything that needs a real PostgreSQL or Kafka: the repository, the pool, the whole job against real topics and tables | The common approach in the JVM ecosystem (Spring Boot service connections, Quarkus Dev Services, and the Flink Kafka connector's own tests use it). Each run gets isolated infrastructure, so `mvn verify` works on any machine with Docker, with no stack started beforehand, no data left by earlier runs and no test depending on another. |
| **Scripts over the Compose stack** (`scripts/e2e`) | Failures that kill processes: TaskManager, JobManager (HA), Kafka broker, MinIO, a long PostgreSQL outage | These need the real topology (ZooKeeper HA, S3 storage, several containers) and a restart that keeps addresses stable, which a per-test container does not give. Exact commands go in the README. |

Tests against an already running Compose stack are not used for integration tests: they tie `mvn verify` to external state, leave data behind and are the classic source of flaky builds. The Compose stack is exercised by the scripts and by the manual end-to-end run.

## Choices

- **Testcontainers 2.0.5** (the current line; 1.21 only gets maintenance). Modules were renamed in 2.x: `testcontainers-postgresql`, `testcontainers-kafka`, `testcontainers-junit-jupiter`.
- **Real migrations.** Each test gets its own database, migrated by Flyway with the scripts in `db/migration` and the same Flyway version as the `postgres-init-schema` service (11.20.3). The schema is therefore under test, and destructive tests (dropped table, killed connections) cannot disturb each other.
- **One PostgreSQL container per JVM**, started on first use; per-test isolation comes from separate databases, which is cheap.
- **`integration-tests` may depend on the real validation rules**; `flink-job` still never does, so the tests prove the plugins are found without being referenced.
- `mvn verify -DskipITs` skips them for a quick build. They need a running Docker daemon.

## What the first integration tests found

- The alert row was written even when the transaction and its score already existed. A replay that disagreed with the first write could leave a HIGH alert next to a LOW score. The alert is now copied from the stored score and only when that score has the event's level, so the first write wins everywhere.
- An assumption that the driver's `TimeZone` session option would apply was wrong; the driver ignores it (see ADR 0001).
