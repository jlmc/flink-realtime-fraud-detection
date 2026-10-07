package io.github.jlmc.fraud.risk.rules.unusualamount;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;
import java.math.BigDecimal;
import java.util.Map;

/**
 * Amount well above the customer's recent behaviour: {@code amount > multiplier x average}.
 * Settings: {@code risk.anomaly-multiplier} (default 5), {@code risk.anomaly-min-history} (default 3).
 *
 * <p>Known limitations: a plain mean is sensitive to earlier outliers and ignores currency, merchant and time of day.
 * A customer with fewer than {@code minHistory} past transactions is never flagged (not enough evidence).
 */
public final class AmountAnomalyRule implements TransactionRiskRule {

    public static final String REASON = "UNUSUAL_AMOUNT";
    public static final int SCORE = 30;
    public static final String MULTIPLIER_SETTING = "risk.anomaly-multiplier";
    public static final String MIN_HISTORY_SETTING = "risk.anomaly-min-history";

    private BigDecimal multiplier;
    private int minHistory;

    public AmountAnomalyRule() {
        this(new BigDecimal("5"), 3);
    }

    public AmountAnomalyRule(BigDecimal multiplier, int minHistory) {
        this.multiplier = multiplier;
        this.minHistory = minHistory;
    }

    @Override
    public String name() {
        return "amount-anomaly";
    }

    @Override
    public void configure(Map<String, String> settings) {
        multiplier = new BigDecimal(settings.getOrDefault(MULTIPLIER_SETTING, multiplier.toPlainString()));
        minHistory = Integer.parseInt(settings.getOrDefault(MIN_HISTORY_SETTING, String.valueOf(minHistory)));
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
