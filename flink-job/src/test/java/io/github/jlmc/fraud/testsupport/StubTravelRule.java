package io.github.jlmc.fraud.testsupport;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;

import java.time.Duration;
import java.util.Map;

/** Test-only risk plugin: another country than the previous transaction, within ten minutes. */
public class StubTravelRule implements TransactionRiskRule {

    @Override
    public String name() {
        return "stub-travel";
    }

    @Override
    public RiskContribution evaluate(Transaction transaction, RiskContext context) {
        Duration since = context.timeSincePreviousTransaction();
        return transaction.country() != null && context.previousCountry() != null && since != null
                && !transaction.country().equals(context.previousCountry()) && since.compareTo(Duration.ofMinutes(10)) < 0
                ? RiskContribution.triggered(50, "STUB_TRAVEL") : RiskContribution.none();
    }
}
