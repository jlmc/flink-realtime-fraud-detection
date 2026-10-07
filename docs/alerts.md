# High-risk fraud alerts

Two different topics, two different meanings:

| Topic | Meaning | Volume |
|---|---|---|
| `transaction.risk.events` | "Flink evaluated this transaction and produced a risk result." | one per evaluated transaction (LOW, MEDIUM and HIGH) |
| `fraud.high-risk.alerts` | "This transaction requires downstream fraud handling." | only transactions the alert policy flags (HIGH) |

Not every risk event becomes an alert. The alert is **additional**: the risk result is still published.

## When an alert is raised

`FraudAlertPolicy.shouldAlert` is the single decision. It alerts when the risk level is `HIGH`, that is **score >= 70**
(`RiskLevel.fromScore`: below 40 LOW, below 70 MEDIUM, otherwise HIGH). The same policy decides the `fraud_alerts` PostgreSQL row,
so the table and the topic cannot disagree about what "high risk" means. There is no second engine: the alert is emitted by
`RiskEvaluationFunction` from the same evaluation as the risk result, through a side output (`PipelineTags.ALERTS`).

Invalid, poison and late transactions never produce alerts (they are never risk-evaluated).

## Payload

```json
{
  "alertId": "high-risk-v1-tx-123",
  "transactionId": "tx-123",
  "customerId": "customer-42",
  "riskScore": 80,
  "riskLevel": "HIGH",
  "reasons": ["HIGH_TRANSACTION_VELOCITY", "HIGH_SPENDING_VELOCITY"],
  "transactionTimestamp": "2026-10-06T13:10:01Z"
}
```

- `alertId` is deterministic (`high-risk-v1-<transactionId>`): the alert is a pure function of the risk result, so processing the
  same transaction twice yields an identical record. The `v1` is the alert's meaning; change it if that meaning changes.
- There is no `alertTimestamp`: a processing-time value would differ on every replay and defeat the deterministic id. The Kafka record
  timestamp is the creation time.
- There is no `severity`: with one alerting level it would be a constant. `riskLevel` is kept so the record stays self-describing.
- The record key is `customerId`, so one customer's alerts stay ordered within a partition.

## Delivery guarantees

- **Flink to Kafka: exactly-once** (transactional sink, same strategy as `transaction.risk.events`, own transactional id prefix
  `fraud-alerts`). Consumers **must** use `isolation.level=read_committed`. Alerts become visible when a checkpoint completes.
- **Duplicates by input**: the transaction is deduplicated by `transactionId` before risk evaluation (state TTL 24 h), so a Kafka
  redelivery or producer retry does not produce a second alert.
- **Restarts**: state and source offsets come from the last checkpoint and uncommitted Kafka transactions are aborted
  (covered by `AlertRecoveryIT`).
- **Not guaranteed**: a duplicate is still possible if the deduplication state is lost (the job restarts without state or
  savepoint) or the same transaction id comes back after the 24 h TTL. Consumers that cannot tolerate that should deduplicate on `alertId`.
- **No cross-topic atomicity**: results, alerts and the PostgreSQL rows are written by independent sinks. During a failure a consumer can briefly see a
  risk result before its alert, or the reverse, until recovery commits. This is not end-to-end exactly-once.

## What downstream systems can do with it

Fraud investigation queues, case creation, transaction blocking, customer notification and security monitoring can consume
`fraud.high-risk.alerts`. None of these is implemented here.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 \
  --topic fraud.high-risk.alerts --from-beginning --isolation-level read_committed --timeout-ms 5000
```
