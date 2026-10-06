package io.github.jlmc.fraud.domain.risk;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;

import java.util.List;

/**
 * Detects an A -&gt; B -&gt; A country pattern (e.g. PT -&gt; US -&gt; PT) inside the 10 minute window.
 *
 * <p>No geographic distance is computed: the rule only looks at country codes, so a PT -&gt; ES -&gt; PT trip triggers
 * as well. That is a known limitation of the POC.
 */
public final class CountryChangeRule implements TransactionRiskRule {

    public static final String REASON = "SUSPICIOUS_COUNTRY_CHANGE";
    public static final int SCORE = 50;

    @Override
    public String name() {
        return "country-change";
    }

    @Override
    public RiskContribution evaluate(Transaction transaction, RiskContext context) {
        String current = transaction.country();
        List<String> recent = context.recentCountries();
        int n = recent.size();
        if (current == null || n < 2) {
            return RiskContribution.none();
        }
        String previous = recent.get(n - 1);
        String beforePrevious = recent.get(n - 2);
        return !previous.equals(current) && beforePrevious.equals(current)
                ? RiskContribution.triggered(SCORE, REASON)
                : RiskContribution.none();
    }
}
