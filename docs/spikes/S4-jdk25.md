# Spike S4: running on JDK 25 (deferred: last milestone)

Decision: the project stays on **JDK 17** (default and recommended by Flink 2.x, official image `flink:2.2.1-java17`)
until the blockers below are gone. The migration to JDK 25 is the **last** milestone.
JDK 21 is the intermediate option: official image `flink:2.2.1-java21` exists, support is documented as experimental.

## What works today on JDK 25

- **Build:** compiled with a JDK 25 toolchain and `maven.compiler.release=17`, so the bytecode runs on 17.
- **Tests:** the whole suite, including the in-JVM Flink MiniCluster tests and the operator harness tests, runs on JDK 25
  locally without changes.

## Facts (observed 2026-10-06, Flink 2.2.1, Temurin 25.0.4)

| # | Finding | Evidence |
|---|---|---|
| 1 | No official JDK 25 image. | `docker manifest inspect flink:2.2.1-java25` and `flink:2.3.0-java25` do not exist; tags are `java11`, `java17`, `java21`. |
| 2 | Flink documentation lists Java 11, 17 and 21 (experimental). Java 25 is not mentioned. | `docs/deployment/java_compatibility` (stable and 2.3). |
| 3 | **Blocker A: the cluster scripts do not start on JDK 24+.** `bin/config.sh` appends `-Djava.security.manager=allow` for every Java above 17; JDK 24+ aborts with `Enabling a Security Manager is not supported`. | JobManager container exits at JVM init. |
| 4 | **Blocker B: `flink-s3-fs-hadoop` fails on JDK 24+.** Hadoop's `UserGroupInformation` calls `Subject.getSubject`, which throws `UnsupportedOperationException: getSubject is not supported` once the Security Manager is gone. The S3 filesystem cannot be created, so HA storage on S3 fails and the entrypoint dies (`Could not create FileSystem for highly available storage path`). | JobManager log, after working around A. |
| 5 | Warnings that do not block today: `sun.misc.Unsafe` terminally deprecated (Pekko, Hadoop) and `System.loadLibrary` restricted-method warning (Hadoop native loader). | Container logs. JDK will block them in a future release; `--enable-native-access=ALL-UNNAMED` silences the second one. |

## Workarounds tried

| For | Workaround | Result |
|---|---|---|
| A | Image `FROM flink:2.2.1-java17` with `/opt/java/openjdk` replaced by `eclipse-temurin:25-jre-noble` (same Ubuntu 24.04 base, so the copy is safe) and the `-Djava.security.manager=allow` line removed from `config.sh` (`sed`). | Cluster proceeds past JVM init. Unsupported by the Flink project. |
| B | Use `flink-s3-fs-presto` (`ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-presto-2.2.1.jar`) instead of the Hadoop one. | JobManager and TaskManager start, ZooKeeper HA with S3 storage works, TaskManager registers. |

## Not verified yet (do this when the milestone comes)

- Job execution, RocksDB checkpoints to S3 and savepoints on JDK 25 with the Presto filesystem (the test was interrupted).
- Whether Presto is acceptable for savepoints and recovery in this project: the Flink docs recommend it for checkpoints and the Hadoop
  implementation for savepoints and the recoverable writer, so this needs a real stop-with-savepoint and restore test.
- Upstream status: a Flink release or image with Java 25 support, and the Hadoop release that fixes `Subject.getSubject` on JDK 24+.
  (Not researched yet. Check Flink release notes and the Hadoop JIRA before picking a workaround.)
- The native S3 filesystem (experimental in Flink 2.3, not available in 2.2.1) as a third way out of blocker B.

## Migration checklist (last milestone)

1. Re-check upstream: Flink release notes, `docker manifest inspect flink:<version>-java25`, Hadoop fix for blocker B.
2. If upstream supports 25: bump the Flink image and `flink.version`, delete the workarounds, rerun everything below.
3. If not: either stay on 17/21, or build a project image as in the table above, with the S3 plugin chosen after the savepoint test.
4. Change the toolchain: `maven.compiler.release` to the target, enforcer to `[25,)`, update the README prerequisites.
5. Rerun the full validation: `mvn clean verify`, session and application mode deploy, plugin discovery, checkpoint, savepoint
   stop and restore, TaskManager kill, JobManager kill (HA), PostgreSQL outage, Kafka broker restart.
6. Record the result, including known limitations, in the README.
