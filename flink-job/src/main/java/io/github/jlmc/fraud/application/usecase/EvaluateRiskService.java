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

/** Sums the contributions of all risk rules (capped at 100) and derives the level. */
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
