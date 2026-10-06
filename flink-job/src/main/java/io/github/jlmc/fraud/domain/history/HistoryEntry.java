package io.github.jlmc.fraud.domain.history;

import java.math.BigDecimal;

/**
 * What the risk rules need to remember about one past transaction. Kept deliberately small because it is
 * what the stream processor stores in managed state for every customer.
 *
 * @param timestampMillis event time (epoch millis)
 * @param amount          never null (a missing amount is stored as zero)
 * @param country         may be null
 */
public record HistoryEntry(long timestampMillis, BigDecimal amount, String country) {

    public HistoryEntry {
        amount = amount == null ? BigDecimal.ZERO : amount;
    }
}
