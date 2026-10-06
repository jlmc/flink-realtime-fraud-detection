package io.github.jlmc.fraud.domain.risk;

import io.github.jlmc.fraud.validation.TransactionRiskRule;

import java.util.List;

/** The built-in risk rules, configured from {@link RiskThresholds}. */
public final class RiskRules {

    private RiskRules() {
    }

    public static List<TransactionRiskRule> from(RiskThresholds t) {
        return List.of(
                new TransactionVelocityRule(t.maxTransactionsPerMinute()),
                new SpendingVelocityRule(t.maxAmountPerTenMinutes()),
                new CountryChangeRule(),
                new AmountAnomalyRule(t.anomalyMultiplier(), t.anomalyMinHistory()));
    }
}
