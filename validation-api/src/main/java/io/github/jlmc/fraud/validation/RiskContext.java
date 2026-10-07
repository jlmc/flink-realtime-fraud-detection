package io.github.jlmc.fraud.validation;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * Domain-level view of a customer's recent behaviour, computed by the stream processor from its
 * managed state. Risk rules never see Flink state.
 *
 * @param transactionsLastMinute   attempts (approved or declined) in the 1 minute window ending at the current event (inclusive)
 * @param amountLastTenMinutes     total spent in the 10 minute window ending at the current event (inclusive); declined attempts spent nothing and are not counted
 * @param previousCountry          country of the previous transaction, or {@code null} if none
 * @param recentCountries          countries of recent transactions in chronological order, oldest first,
 *                                 excluding the current one
 * @param averageRecentAmount      mean amount of recent approved transactions excluding the current one,
 *                                 or {@code null} if there is no history
 * @param recentTransactionCount   number of approved transactions that make up {@code averageRecentAmount}
 * @param timeSincePreviousTransaction time between the previous transaction (any status) and the current one,
 *                                 or {@code null} if there is none
 * @param knownCountries           every country seen in the retained history, excluding the current transaction
 * @param declinedLastFiveMinutes  declined attempts in the 5 minutes before the current one (current excluded)
 */
public record RiskContext(
        int transactionsLastMinute,
        BigDecimal amountLastTenMinutes,
        String previousCountry,
        List<String> recentCountries,
        BigDecimal averageRecentAmount,
        int recentTransactionCount,
        Duration timeSincePreviousTransaction,
        Set<String> knownCountries,
        int declinedLastFiveMinutes) {

    public RiskContext {
        recentCountries = List.copyOf(recentCountries);
        knownCountries = Set.copyOf(knownCountries);
    }

    /** A context without the later additions: no previous transaction timing, no known countries, no declines. */
    public RiskContext(int transactionsLastMinute, BigDecimal amountLastTenMinutes, String previousCountry,
                       List<String> recentCountries, BigDecimal averageRecentAmount, int recentTransactionCount) {
        this(transactionsLastMinute, amountLastTenMinutes, previousCountry, recentCountries, averageRecentAmount,
                recentTransactionCount, null, Set.of(), 0);
    }
}
