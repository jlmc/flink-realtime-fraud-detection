package io.github.jlmc.fraud.testsupport;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;

import java.time.Duration;
import java.util.Map;

/** Test-only risk plugin: more than 5000 in ten minutes. */
public class StubSpendingRule implements TransactionRiskRule {

    @Override
    public String name() {
        return "stub-spending";
    }

    @Override
    public RiskContribution evaluate(Transaction transaction, RiskContext context) {
        return context.amountLastTenMinutes().signum() > 0 && context.amountLastTenMinutes().intValue() > 5000
                ? RiskContribution.triggered(40, "STUB_SPENDING") : RiskContribution.none();
    }
}
