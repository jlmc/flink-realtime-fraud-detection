package io.github.jlmc.fraud.risk.rules.transactionvelocity;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;
import java.util.Map;

/**
 * More than N attempts within 1 minute (the current one included). Declined attempts count: they are attempts.
 * Setting: {@code risk.max-transactions-per-minute} (default 5).
 */
public final class TransactionVelocityRule implements TransactionRiskRule {

    public static final String REASON = "HIGH_TRANSACTION_VELOCITY";
    public static final int SCORE = 40;
    public static final String SETTING = "risk.max-transactions-per-minute";

    private int maxPerMinute;

    public TransactionVelocityRule() {
        this(5);
    }

    public TransactionVelocityRule(int maxPerMinute) {
        this.maxPerMinute = maxPerMinute;
    }

    @Override
    public String name() {
        return "transaction-velocity";
    }

    @Override
    public void configure(Map<String, String> settings) {
        maxPerMinute = Integer.parseInt(settings.getOrDefault(SETTING, String.valueOf(maxPerMinute)));
    }

    @Override
    public RiskContribution evaluate(Transaction transaction, RiskContext context) {
        return context.transactionsLastMinute() > maxPerMinute
                ? RiskContribution.triggered(SCORE, REASON)
                : RiskContribution.none();
    }
}
