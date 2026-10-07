package io.github.jlmc.fraud.risk.rules.failedattempts;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;
import java.util.Map;

/**
 * An approved transaction right after several declined ones: the classic card-testing / guessing pattern that ends
 * with a payment that finally goes through. Looks at the 5 minutes before the current transaction.
 * Setting: {@code risk.max-declined-per-five-minutes} (default 3; triggers at that many or more).
 */
public final class FailedAttemptsThenSuccessRule implements TransactionRiskRule {

    public static final String REASON = "FAILED_ATTEMPTS_THEN_SUCCESS";
    public static final int SCORE = 40;
    public static final String SETTING = "risk.max-declined-per-five-minutes";

    private int minDeclined;

    public FailedAttemptsThenSuccessRule() {
        this(3);
    }

    public FailedAttemptsThenSuccessRule(int minDeclined) {
        this.minDeclined = minDeclined;
    }

    @Override
    public String name() {
        return "failed-attempts-then-success";
    }

    @Override
    public void configure(Map<String, String> settings) {
        minDeclined = Integer.parseInt(settings.getOrDefault(SETTING, String.valueOf(minDeclined)));
    }

    @Override
    public RiskContribution evaluate(Transaction transaction, RiskContext context) {
        return !transaction.isDeclined() && context.declinedLastFiveMinutes() >= minDeclined
                ? RiskContribution.triggered(SCORE, REASON)
                : RiskContribution.none();
    }
}
