package io.github.jlmc.fraud.domain.risk;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;

/**
 * Tunable thresholds of the risk rules and the size of the per-customer history.
 * Defaults are POC examples, see the README for the reasoning.
 *
 * @param maxTransactionsPerMinute  velocity triggers when MORE than this many transactions fall in 1 minute
 * @param maxAmountPerTenMinutes    spending velocity triggers when the 10 minute total EXCEEDS this amount
 * @param anomalyMultiplier         amount anomaly triggers when amount &gt; multiplier x average of recent amounts
 * @param anomalyMinHistory         minimum number of past transactions before the anomaly rule is allowed to trigger
 * @param historyRetention          how long a transaction stays in the customer history
 * @param maxHistoryEntries         hard bound on history size (bounds state size per customer)
 */
public record RiskThresholds(
        int maxTransactionsPerMinute,
        BigDecimal maxAmountPerTenMinutes,
        BigDecimal anomalyMultiplier,
        int anomalyMinHistory,
        Duration historyRetention,
        int maxHistoryEntries) implements Serializable {

    public static RiskThresholds defaults() {
        return new RiskThresholds(
                5,
                new BigDecimal("5000"),
                new BigDecimal("5"),
                3,
                Duration.ofHours(1),
                100);
    }
}
