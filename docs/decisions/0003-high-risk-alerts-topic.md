# 0003 - A dedicated topic for high-risk alerts

Status: accepted (2026-10-07)

## Context
`transaction.risk.events` carries every risk result. Downstream fraud systems only want the transactions that need action, and should
not have to filter and re-derive "high risk" themselves.

## Decision
Add `fraud.high-risk.alerts`, written by the same job, from the same evaluation.

- **Side output, not a filter**: the official Flink guidance for producing several outputs from one function is side outputs
  (`OutputTag`), not replicating the stream and filtering. The project already does this for invalid and late events, so the
  alerts follow the same pattern (`PipelineTags.ALERTS`). The evaluation happens once.
- **Reuse `FraudAlertPolicy`** instead of a new threshold: one definition of high risk for the topic and the `fraud_alerts` table.
- **Deterministic `alertId`, no processing-time field**: replays produce identical records, so deduplication downstream is trivial.
- **Exactly-once Kafka sink with its own transactional id prefix**: same guarantee as the risk topic; prefixes must be distinct between sinks.
- **Existing topics, schemas and PostgreSQL untouched**: purely additive. New operators carry explicit uids, so savepoints taken before this
  change still restore (the new sink simply starts without state).

## Consequences
- Three independent sinks; no atomicity across topics and tables (documented in `docs/alerts.md`).
- Alerts are visible after the checkpoint interval (read_committed).
- Possible duplicate only if the dedup state is lost or the TTL expires; consumers can use `alertId`.
