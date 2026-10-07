# Spike S1: plugin classloading (ServiceLoader) on Flink 2.2.1

Question: can operators running in TaskManagers discover validation-rule plugins that are **not** inside the job JAR?
Method: throwaway job (`ServiceLoader.load(TransactionValidationRule.class, Thread.currentThread().getContextClassLoader())`
inside `RichMapFunction#open`), run on the real Docker Compose cluster (1 JM + 1 TM, `flink:2.2.1-java17`).

## Facts (observed)

| Deployment | Where the plugin JARs go | Result |
|---|---|---|
| Session Mode | `/opt/flink/usrlib` | **Fails**: `NoClassDefFoundError` when the job is submitted. `usrlib` is not on the classpath in Session Mode. |
| Session Mode | `/opt/flink/lib` (JM **and** TM) | **Works**: all 3 rules found. Rule API loaded by `AppClassLoader`. |
| Application Mode (`standalone-job`) | `/opt/flink/usrlib/**` on JM **and** TM (subfolders are scanned) | **Works**: all 3 rules found. Rule API loaded by the user `ChildFirstClassLoader`. |
| Application Mode | `usrlib` on the JM only | **Fails**: TM throws `ClassNotFoundException` for the job classes. |

- The job JAR does **not** contain `validation-api` (excluded in the shade config): job and plugins share one copy supplied next to the plugins.
  Otherwise `ServiceLoader` would return rule classes whose `TransactionValidationRule` differs from the job's copy (`ClassCastException`).
- The shaded job JAR is ~25 MB (Kafka client + PostgreSQL driver) and contains no concrete rule classes.
- Plugin discovery must use the **context** classloader, not `ServiceLoader.load(Class)` with the caller's loader by accident.

## Decisions that follow

- Session Mode: Compose starts JM/TM with the plugin JARs copied into `lib/` (plugins live with the runtime, so changing a rule means restarting the cluster).
- Application Mode: plugin JARs and the job JAR are mounted under `usrlib/` of **every** JM and TM container.
- Either way: changing a plugin requires redeploying (intended, per PLAN section 5; no hot loading).
- Not needed: shading rule JARs. Potential conflicts are limited to what rules bring transitively; rules depend only on `validation-api`, enforced by Maven Enforcer.

## Spike S3 (same session): JDBC connector

`flink-connector-jdbc` has no release for Flink 2.x on Maven Central (latest `3.4.0-1.20`). The PostgreSQL sink is therefore a custom Sink V2 with plain JDBC.

## Operational notes from the first real deployment (Session Mode, Compose cluster)

- Submitting through `POST /jars/:id/run` did **not** apply the cluster's `parallelism.default: 3`: all operators came up with
  parallelism 1. Passing `"parallelism": 3` in the request body fixes it, so the deploy script must always send it.
- With `execution.checkpointing.*` from the cluster `config.yaml`, the job took a checkpoint every 10 s into
  `s3://flink-state/checkpoints/<job-id>/chk-N` (RocksDB, exactly-once mode, retained on cancellation), as configured.
- Smoke test results over real Kafka (16 messages): duplicate collapsed to one result, an out-of-order event still produced the
  PT -> US -> PT alert, 6th transaction within a minute raised the velocity alert, and the real plugin rules, a poison message and a
  message without customer all went to `transaction.invalid.events`.

## Addendum: fraud (risk) rules use the same mechanism

The fraud rules (`fraud-rules/fraud-rule-*`, `TransactionRiskRule`) are discovered exactly like the validation rules: the context
classloader inside `RiskEvaluationFunction.open()` on the TaskManager, JARs in `lib/` (Session Mode) or `usrlib/` (Application Mode),
`validation-api` shared next to them. Nothing in the conclusions above changes. See [ADR 0005](../decisions/0005-risk-rules-as-plugins.md).
