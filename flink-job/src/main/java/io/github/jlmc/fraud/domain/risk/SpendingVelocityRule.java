package io.github.jlmc.fraud.domain.risk;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;

import java.math.BigDecimal;

/**
 * More than X spent within 10 minutes (the current transaction included).
 *
 * <p>Known limitation: amounts are summed as plain numbers, whatever their currency. Converting currencies is out of
 * scope for this POC.
 */
public final class SpendingVelocityRule implements TransactionRiskRule {

    public static final String REASON = "HIGH_SPENDING_VELOCITY";
    public static final int SCORE = 40;

    private final BigDecimal maxAmount;

    public SpendingVelocityRule(BigDecimal maxAmount) {
        this.maxAmount = maxAmount;
    }

    @Override
    public String name() {
        return "spending-velocity";
    }

    @Override
    public RiskContribution evaluate(Transaction transaction, RiskContext context) {
        return context.amountLastTenMinutes().compareTo(maxAmount) > 0
                ? RiskContribution.triggered(SCORE, REASON)
                : RiskContribution.none();
    }
}
