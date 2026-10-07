package io.github.jlmc.fraud.risk.rules.geographicimpossibility;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;
import java.time.Duration;
import java.util.Map;

/**
 * A different country from the previous transaction's, too soon to have travelled (e.g. PT at 12:00, US at 12:03).
 * Setting: {@code risk.impossible-travel-minutes} (default 10).
 *
 * <p>Heuristic: no coordinates or distances are known, only country codes and the time between the two events, so a
 * trip between neighbouring countries inside the window triggers as well. It looks at the previous transaction only,
 * whatever its status: a declined attempt also tells where the card was used.
 */
public final class GeographicImpossibilityRule implements TransactionRiskRule {

    public static final String REASON = "GEOGRAPHIC_IMPOSSIBILITY";
    public static final int SCORE = 50;
    public static final String SETTING = "risk.impossible-travel-minutes";

    private Duration window;

    public GeographicImpossibilityRule() {
        this(Duration.ofMinutes(10));
    }

    public GeographicImpossibilityRule(Duration window) {
        this.window = window;
    }

    @Override
    public String name() {
        return "geographic-impossibility";
    }

    @Override
    public void configure(Map<String, String> settings) {
        window = Duration.ofMinutes(Long.parseLong(settings.getOrDefault(SETTING, String.valueOf(window.toMinutes()))));
    }

    @Override
    public RiskContribution evaluate(Transaction transaction, RiskContext context) {
        String current = transaction.country();
        String previous = context.previousCountry();
        Duration since = context.timeSincePreviousTransaction();
        if (current == null || previous == null || since == null) {
            return RiskContribution.none();
        }
        return !current.equals(previous) && since.compareTo(window) < 0
                ? RiskContribution.triggered(SCORE, REASON)
                : RiskContribution.none();
    }
}
