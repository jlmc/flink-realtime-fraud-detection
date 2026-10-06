# Architecture decisions

Short record of what was decided and why. Detailed decisions get their own file; findings from experiments live in `docs/spikes/`.

| Decision | Why | More |
|---|---|---|
| **Flink 2.2.1**, not 2.3.0 | 2.3.0 is the newest 2.x, but there is no Kafka connector for it yet (latest is `5.0.0-2.2`; the 2.3 docs say so). Using a connector built for another minor is unsupported. The version is one property in the root POM, so upgrading later is one line. | root `pom.xml` |
| **Java 17** | Default and recommended Java of Flink 2.x, official image available. Java 21 is documented as experimental. Java 25 is not listed and has two real blockers. JDK 25 is the build toolchain (`release 17`); running on it is the last milestone. | [S4](../spikes/S4-jdk25.md) |
| **Plain Java and native Flink APIs, no Spring** | Requirement of the PLAN; nothing here needs a container. | |
| **Ports and adapters inside `flink-job`**, enforced by tests | Flink, Kafka and JDBC are details. Domain and application are plain Java, testable without Flink. ArchUnit fails the build if domain or application touch a framework, if layers point outwards, or if inbound and outbound adapters depend on each other. | `ArchitectureTest` |
| **`validation-rules` aggregator module** | The three rule JARs share one parent that declares the only dependency they may have (`validation-api`) and bans Flink and Kafka. Adding a rule is a sibling module and nothing else. The name says what the children are (implementations); `validation-api` is the contract. | `validation-rules/pom.xml` |
| **Plugins live outside the job JAR**; `validation-api` is excluded from the shaded JAR | Rules must be independent JARs. One shared copy of the API avoids `ClassCastException` between job and plugins. Session Mode needs the JARs in `lib/`, Application Mode in `usrlib/` of every JobManager and TaskManager. | [S1](../spikes/S1-plugin-classloading.md) |
| **Plugin that throws rejects the transaction** (`RULE_ERROR`) | A faulty plugin must not restart the job in a loop, and must not silently accept the transaction. | `ValidateTransactionService` |
| **`transactionId`, `customerId`, `timestamp` always required**, independent of plugins | Deduplication key, partition key and event time. A deployment without the schema plugin must not crash the job with a null key. | `ValidateTransactionService` |
| **Poison messages travel as data** | The deserializer never throws, it returns a message carrying the error, which is routed to `transaction.invalid.events`. A throwing deserializer would restart the job forever on the same record. | `TransactionJsonParser` |
| **Risk is evaluated on watermark, in event-time order** | Events are buffered by timestamp and evaluated when the watermark passes, so a result does not depend on arrival order. The cost is latency equal to the out-of-orderness (30 s). Late events go to a side output and are stored with `status=LATE`. | `RiskEvaluationFunction` |
| **Risk rules produce contributions, not results** | `TransactionRiskRule` returns a score and a reason; the application sums them (capped at 100) into one `RiskResult`. Deviates from the PLAN's example signature. | `EvaluateRiskService` |
| **Risk results to Kafka exactly-once, invalid events at-least-once** | Results are transactional (consumers must read committed). A duplicated dead-letter record is harmless, so it does not pay for transactions. | `KafkaSinks` |
| **Custom PostgreSQL sink** | `flink-connector-jdbc` has no release for Flink 2.x (latest `3.4.0-1.20`). | [S1](../spikes/S1-plugin-classloading.md) |
| **PostgreSQL: at-least-once plus idempotent writes** | Checkpoint and database commit are two separate atomic actions, so this is not exactly-once. `INSERT ... ON CONFLICT DO NOTHING` makes replays after recovery no-ops. Records the database rejects permanently are skipped with an error log, because failing the job would restart it forever; a missing table is a system failure and is never skipped. | `JdbcTransactionRepository`, `SqlErrors` |
| **Fraud alerts for HIGH risk only** | Strong signal only; LOW and MEDIUM are recorded as scores. One constant to change. | `FraudAlertPolicy` |
| **Connection pool** | See the ADR. | [0001](0001-shared-postgres-connection-pool.md) |
| **ZooKeeper in the stack** | Flink JobManager HA needs ZooKeeper or Kubernetes; without it a JobManager restart loses the job. Required by the PLAN's "JobManager failure" scenario. | `docker-compose.yml` |
| **MinIO built from source** | MinIO stopped publishing community Docker images on 2025-10-23. The pinned release is the last community line with the full web console, so the bucket can be browsed. | `docker/minio/Dockerfile` |
| **Every image version pinned** | `latest` is not reproducible. | `docker-compose.yml` |
| **Data in `./.data` (bind mounts, git-ignored)** | Named volumes live inside the Docker VM and cannot be inspected. Kafka sets `KAFKA_LOG_DIRS` because the image otherwise ignores the volume. Postgres uses a `PGDATA` subdirectory to avoid an ownership error. | `docker-compose.yml` |
| **`groupId` `io.github.jlmc`**, base package `io.github.jlmc.fraud` | Owner's GitHub namespace; the package follows it. | root `pom.xml` |
