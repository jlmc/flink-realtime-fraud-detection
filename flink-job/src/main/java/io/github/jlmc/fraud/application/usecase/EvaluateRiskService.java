package io.github.jlmc.fraud.application.usecase;

import io.github.jlmc.fraud.application.port.in.EvaluateRiskUseCase;
import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.RiskLevel;
import io.github.jlmc.fraud.validation.RiskResult;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;

import java.util.ArrayList;
import java.util.List;

/**
 * Sums the contributions of all risk rules (capped at 100) and derives the level.
 *
 * <p>The rules run one after the other, on purpose, and not in parallel or asynchronously:
 * <ul>
 *   <li>They are pure in-memory computations on the {@link RiskContext}; handing them to other threads costs more than they do.</li>
 *   <li>Parallelism already comes from Flink: customers are spread over the subtasks of the operator.</li>
 *   <li>{@code reasons} follows the order of the rules, and the alert built from it must be identical when a transaction is
 *       replayed. Concurrent execution would need an extra re-ordering step to keep that.</li>
 *   <li>It runs in the task thread (events, timers and checkpoint barriers share it); the rules are not required to be thread-safe.</li>
 * </ul>
 * It would be worth changing if a rule did I/O (a remote call or database lookup): then use Flink's Async I/O
 * ({@code AsyncDataStream}), which keeps ordering, timeouts and checkpoint semantics, not threads inside this method. For heavy
 * CPU work, first measure, then raise the parallelism of the operator. See docs/decisions/0004-sequential-rule-evaluation.md.
 */
public final class EvaluateRiskService implements EvaluateRiskUseCase {

    private final List<TransactionRiskRule> rules;

    public EvaluateRiskService(List<TransactionRiskRule> rules) {
        this.rules = List.copyOf(rules);
    }

    @Override
    public RiskResult evaluate(Transaction transaction, RiskContext context) {
        int score = 0;
        List<String> reasons = new ArrayList<>();
        for (TransactionRiskRule rule : rules) {
            RiskContribution c = rule.evaluate(transaction, context);
            if (c.isTriggered()) {
                score += c.score();
                reasons.add(c.reason());
            }
        }
        score = Math.min(score, 100);
        return new RiskResult(transaction.transactionId(), transaction.customerId(), score,
                RiskLevel.fromScore(score), reasons, transaction.timestamp());
    }
}
