package io.github.jlmc.fraud.risk.rules.spendingvelocity;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;
import java.math.BigDecimal;
import java.util.Map;

/**
 * More than X spent within 10 minutes (the current transaction included; declined attempts spent nothing).
 * Setting: {@code risk.max-amount-per-ten-minutes} (default 5000).
 *
 * <p>Known limitation: amounts are summed as plain numbers, whatever their currency. Converting currencies is out of
 * scope for this POC.
 */
public final class SpendingVelocityRule implements TransactionRiskRule {

    public static final String REASON = "HIGH_SPENDING_VELOCITY";
    public static final int SCORE = 40;
    public static final String SETTING = "risk.max-amount-per-ten-minutes";

    private BigDecimal maxAmount;

    public SpendingVelocityRule() {
        this(new BigDecimal("5000"));
    }

    public SpendingVelocityRule(BigDecimal maxAmount) {
        this.maxAmount = maxAmount;
    }

    @Override
    public String name() {
        return "spending-velocity";
    }

    @Override
    public void configure(Map<String, String> settings) {
        maxAmount = new BigDecimal(settings.getOrDefault(SETTING, maxAmount.toPlainString()));
    }

    @Override
    public RiskContribution evaluate(Transaction transaction, RiskContext context) {
        return context.amountLastTenMinutes().compareTo(maxAmount) > 0
                ? RiskContribution.triggered(SCORE, REASON)
                : RiskContribution.none();
    }
}
