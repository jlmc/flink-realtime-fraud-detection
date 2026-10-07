# 0004 - Rules are evaluated sequentially, not in parallel or asynchronously

Status: accepted (2026-10-07)

## Context
`EvaluateRiskService` (risk rules) and `ValidateTransactionService` (validation plugins) loop over the rules for each transaction.
Running them in parallel (`parallelStream`, an executor) or asynchronously looks attractive.

## Decision
Keep the sequential loop.

- **Cost.** The rules are pure in-memory comparisons on the `RiskContext`. A thread hand-off costs more than they do. This is
  reasoning about the work, not a measurement; there is no profile showing the loop as a bottleneck.
- **Parallelism exists where it belongs.** Flink spreads customers over the subtasks of the operator (the job runs with several).
- **Determinism.** `reasons` follows the order of the rules and the alert is built from it. A replayed transaction must produce an
  identical alert (see [0003](0003-high-risk-alerts-topic.md)). Concurrency would need an extra ordering step.
- **Flink's threading model.** The operator runs in the task thread, which also handles timers and checkpoint barriers. Work moved to the
  common pool competes with the rest of the JVM, and failures arrive on another thread. Rules would also have to be thread-safe,
  which the plugin contract does not ask of them.

## When it would be worth changing
| Situation | What to do |
|---|---|
| A rule calls a remote service or a database | Flink **Async I/O** (`AsyncDataStream`) as a separate operator: it keeps order, timeouts and checkpoint semantics. Not threads inside the loop. |
| A rule is heavy on CPU (a model, large computation) | Measure first. If it is the bottleneck, raise the parallelism of the operator (and the partitions of the input topic). |
| A plugin may block or hang | Add a time limit per rule; today a blocking plugin stalls its task, and there is no limit. |

## Consequences
Simple, ordered and reproducible. The known weakness is a blocking plugin, which is a contract and time-limit problem, not a reason for parallel execution.
