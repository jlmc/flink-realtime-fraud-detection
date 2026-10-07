package io.github.jlmc.fraud.domain.history;

import java.math.BigDecimal;

/**
 * What the risk rules need to remember about one past transaction. Kept deliberately small because it is
 * what the stream processor stores in managed state for every customer.
 *
 * @param timestampMillis event time (epoch millis)
 * @param amount          never null (a missing amount is stored as zero)
 * @param country         may be null
 * @param declined        true for a declined payment attempt (it spent nothing)
 */
public record HistoryEntry(long timestampMillis, BigDecimal amount, String country, boolean declined) {

    public HistoryEntry {
        amount = amount == null ? BigDecimal.ZERO : amount;
    }

    /** An approved attempt. */
    public HistoryEntry(long timestampMillis, BigDecimal amount, String country) {
        this(timestampMillis, amount, country, false);
    }
}
