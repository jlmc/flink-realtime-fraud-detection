package io.github.jlmc.fraud.domain.risk;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;

/** More than N transactions within 1 minute (the current one included). */
public final class TransactionVelocityRule implements TransactionRiskRule {

    public static final String REASON = "HIGH_TRANSACTION_VELOCITY";
    public static final int SCORE = 40;

    private final int maxPerMinute;

    public TransactionVelocityRule(int maxPerMinute) {
        this.maxPerMinute = maxPerMinute;
    }

    @Override
    public String name() {
        return "transaction-velocity";
    }

    @Override
    public RiskContribution evaluate(Transaction transaction, RiskContext context) {
        return context.transactionsLastMinute() > maxPerMinute
                ? RiskContribution.triggered(SCORE, REASON)
                : RiskContribution.none();
    }
}
