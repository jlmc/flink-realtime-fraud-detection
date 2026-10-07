# ADR 0005: fraud rules as plugins, one JAR each

Status: accepted

## Context

Validation rules were already plugins, but the four risk rules were compiled into the job. Two things changed: the risk rules had
to grow (three new ones, one replaced) and they are the part of the system a fraud team changes most often. Adding or tuning a
rule should not mean touching or redeploying the pipeline code.

## Decision

- **One JAR per fraud rule**, in `fraud-rules/fraud-rule-*`, each depending only on `validation-api` (Maven Enforcer bans Flink and
  Kafka) and registered in `META-INF/services/io.github.jlmc.fraud.validation.TransactionRiskRule`. The job finds them with
  `ServiceLoader` on the TaskManager (`ServiceLoaderRiskRuleProvider`), the same mechanism, classloader and deployment as the
  validation rules ([S1](../spikes/S1-plugin-classloading.md)). ArchUnit and Enforcer keep the job from referencing a concrete rule.
- **Flink owns the context, rules own the decision.** The job keeps the per-customer history in state and builds a `RiskContext` (plain
  data: windows, previous country and time, known countries, declined attempts). A rule receives the transaction and that context and
  returns a score and a reason. It never sees state, timers or the watermark.
- **Settings reach plugins through `configure(Map)`**, a default method on `TransactionRiskRule`. `ServiceLoader` needs a no-argument
  constructor, so the thresholds cannot be constructor parameters. The job passes every `risk.*` key (program argument, or environment
  variable `RISK_*`) to every rule, and a rule reads the keys it knows and keeps its own defaults otherwise. The existing keys keep
  working.
- **Rules are sorted by name** before use. `reasons` follow the order of the rules, and an alert must not change because the JARs
  were listed in another order on the classpath. Consequence: the order of `reasons` is alphabetical by rule name.
- **A deployment with no fraud-rule JAR fails at start-up.** Scoring everything as harmless is worse than not starting.
- **Schema change: `paymentStatus`** (`APPROVED` or `DECLINED`, optional, absent means approved). Failed-attempt detection needs it.
  An unknown value is a malformed payload (dead-letter topic). It is stored in `transactions.payment_status` (migration V4).
- **Declined attempts count as attempts but spend nothing:** they are in the transaction velocity and in the previous-country and
  previous-time context, and outside the spending sum and the average amount.
- **Geographic impossibility replaces the A, B, A country-change rule.** Two rules for the same behaviour would score it twice.

## Consequences

- Adding a rule: a new module and a JAR, rebuild, `scripts/stage-dist.sh`, restart the Flink containers. Changing a threshold needs
  no code, only a `risk.*` key.
- `HistoryEntry` (Flink state of the risk operator) gained a field, so **state from older savepoints cannot be restored**. Acceptable
  for the POC (no production savepoints); a real deployment would plan a state migration.
- The state per customer grows by one flag per history entry. See the open question on state size in the Flink pipeline docs.
- The job's own tests use throwaway stub rules; the real rules are tested in their modules and together in
  `integration-tests` (`FraudRulesPipelineTest`, no containers).

## Known limits

- "Geographic impossibility" only compares country codes and the time between two events: no coordinates, so neighbouring countries
  inside the window trigger as well.
- "New country" is relative to the retained history (one hour), not to the customer's whole life.
- Amounts of different currencies are added as plain numbers (unchanged).
- Settings are global `risk.*` keys, with no isolation per rule.
- A plugin that blocks has no time limit (see [ADR 0004](0004-sequential-rule-evaluation.md)).
