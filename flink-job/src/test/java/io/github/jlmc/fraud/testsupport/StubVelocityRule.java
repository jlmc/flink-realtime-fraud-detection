package io.github.jlmc.fraud.testsupport;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;

import java.time.Duration;
import java.util.Map;

/** Test-only risk plugin (registered in src/test/resources/META-INF/services). NOT one of the real fraud-rule modules. */
public class StubVelocityRule implements TransactionRiskRule {

    private int max = 5;

    @Override
    public String name() {
        return "stub-velocity";
    }

    @Override
    public void configure(Map<String, String> settings) {
        max = Integer.parseInt(settings.getOrDefault("risk.max-transactions-per-minute", String.valueOf(max)));
    }

    @Override
    public RiskContribution evaluate(Transaction transaction, RiskContext context) {
        return context.transactionsLastMinute() > max ? RiskContribution.triggered(40, "STUB_VELOCITY") : RiskContribution.none();
    }
}
