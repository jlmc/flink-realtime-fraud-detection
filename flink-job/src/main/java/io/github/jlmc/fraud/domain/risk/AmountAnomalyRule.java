package io.github.jlmc.fraud.domain.risk;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;

import java.math.BigDecimal;

/**
 * Amount well above the customer's recent behaviour: {@code amount > multiplier x average}.
 *
 * <p>Known limitations: a plain mean is sensitive to earlier outliers and ignores currency, merchant and time of day.
 * A customer with fewer than {@code minHistory} past transactions is never flagged (not enough evidence).
 */
public final class AmountAnomalyRule implements TransactionRiskRule {

    public static final String REASON = "UNUSUAL_AMOUNT";
    public static final int SCORE = 30;

    private final BigDecimal multiplier;
    private final int minHistory;

    public AmountAnomalyRule(BigDecimal multiplier, int minHistory) {
        this.multiplier = multiplier;
        this.minHistory = minHistory;
    }

    @Override
    public String name() {
        return "amount-anomaly";
    }

    @Override
    public RiskContribution evaluate(Transaction transaction, RiskContext context) {
        BigDecimal average = context.averageRecentAmount();
        if (transaction.amount() == null
                || average == null
                || average.signum() <= 0
                || context.recentTransactionCount() < minHistory) {
            return RiskContribution.none();
        }
        return transaction.amount().compareTo(average.multiply(multiplier)) > 0
                ? RiskContribution.triggered(SCORE, REASON)
                : RiskContribution.none();
    }
}
