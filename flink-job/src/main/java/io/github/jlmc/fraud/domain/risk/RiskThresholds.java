package io.github.jlmc.fraud.domain.risk;

import java.io.Serializable;
import java.time.Duration;

/**
 * Size of the per-customer history the job keeps in state. The thresholds of the risk rules themselves are not here:
 * each rule is a plugin that reads its own {@code risk.*} settings (see {@code TransactionRiskRule#configure}).
 *
 * @param historyRetention  how long a transaction stays in the customer history; also the horizon of "known countries"
 * @param maxHistoryEntries hard bound on history size (bounds state size per customer)
 */
public record RiskThresholds(Duration historyRetention, int maxHistoryEntries) implements Serializable {

    public static RiskThresholds defaults() {
        return new RiskThresholds(Duration.ofHours(1), 100);
    }
}
