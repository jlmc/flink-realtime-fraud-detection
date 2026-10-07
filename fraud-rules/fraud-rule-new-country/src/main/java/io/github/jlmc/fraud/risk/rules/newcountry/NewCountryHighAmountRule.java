package io.github.jlmc.fraud.risk.rules.newcountry;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;
import java.math.BigDecimal;
import java.util.Map;

/**
 * First transaction from a country, and a high amount for this customer: {@code amount > multiplier x average}.
 * Settings: {@code risk.new-country-amount-multiplier} (default 2), {@code risk.new-country-min-history} (default 3).
 *
 * <p>"New" is relative to the retained history (one hour by default), not to the customer's whole life. A customer
 * with fewer than {@code minHistory} approved transactions is never flagged: with little evidence every country looks
 * new.
 */
public final class NewCountryHighAmountRule implements TransactionRiskRule {

    public static final String REASON = "NEW_COUNTRY_HIGH_AMOUNT";
    public static final int SCORE = 40;
    public static final String MULTIPLIER_SETTING = "risk.new-country-amount-multiplier";
    public static final String MIN_HISTORY_SETTING = "risk.new-country-min-history";

    private BigDecimal multiplier;
    private int minHistory;

    public NewCountryHighAmountRule() {
        this(new BigDecimal("2"), 3);
    }

    public NewCountryHighAmountRule(BigDecimal multiplier, int minHistory) {
        this.multiplier = multiplier;
        this.minHistory = minHistory;
    }

    @Override
    public String name() {
        return "new-country-high-amount";
    }

    @Override
    public void configure(Map<String, String> settings) {
        multiplier = new BigDecimal(settings.getOrDefault(MULTIPLIER_SETTING, multiplier.toPlainString()));
        minHistory = Integer.parseInt(settings.getOrDefault(MIN_HISTORY_SETTING, String.valueOf(minHistory)));
    }

    @Override
    public RiskContribution evaluate(Transaction transaction, RiskContext context) {
        BigDecimal average = context.averageRecentAmount();
        if (transaction.country() == null
                || transaction.amount() == null
                || transaction.isDeclined()
                || average == null
                || average.signum() <= 0
                || context.recentTransactionCount() < minHistory
                || context.knownCountries().contains(transaction.country())) {
            return RiskContribution.none();
        }
        return transaction.amount().compareTo(average.multiply(multiplier)) > 0
                ? RiskContribution.triggered(SCORE, REASON)
                : RiskContribution.none();
    }
}
