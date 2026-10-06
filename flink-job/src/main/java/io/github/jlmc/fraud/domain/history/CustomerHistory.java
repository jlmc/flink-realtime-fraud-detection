package io.github.jlmc.fraud.domain.history;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.Transaction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Immutable, bounded view of a customer's recent transactions, oldest first.
 *
 * <p>Pure domain logic: it knows nothing about Flink. The stream processor persists
 * {@link #entries()} in managed state and rebuilds this object when evaluating a transaction.
 *
 * <p>Transactions must be {@linkplain #append appended} in event-time order; the processor guarantees that by
 * evaluating on watermark. The entry for the transaction being evaluated is NOT part of the history yet:
 * {@link #contextFor} adds the current transaction to the windowed figures itself.
 */
public record CustomerHistory(List<HistoryEntry> entries) {

    public static final Duration ONE_MINUTE = Duration.ofMinutes(1);
    public static final Duration TEN_MINUTES = Duration.ofMinutes(10);

    public CustomerHistory {
        entries = List.copyOf(entries);
    }

    public static CustomerHistory empty() {
        return new CustomerHistory(List.of());
    }

    /**
     * Returns a new history with {@code entry} appended, dropping entries older than {@code retention}
     * (relative to {@code entry}) and keeping at most {@code maxEntries} of the newest ones.
     */
    public CustomerHistory append(HistoryEntry entry, Duration retention, int maxEntries) {
        long oldestKept = entry.timestampMillis() - retention.toMillis();
        List<HistoryEntry> next = new ArrayList<>(entries.size() + 1);
        for (HistoryEntry e : entries) {
            if (e.timestampMillis() >= oldestKept) {
                next.add(e);
            }
        }
        next.add(entry);
        int excess = next.size() - maxEntries;
        return new CustomerHistory(excess > 0 ? next.subList(excess, next.size()) : next);
    }

    /**
     * Builds the {@link RiskContext} for {@code transaction}. Windows are {@code (t - window, t]} where {@code t}
     * is the transaction's event time, and include the transaction itself.
     */
    public RiskContext contextFor(Transaction transaction) {
        long t = transaction.timestamp().toEpochMilli();
        BigDecimal current = transaction.amount() == null ? BigDecimal.ZERO : transaction.amount();

        int lastMinute = 1;
        BigDecimal lastTenMinutes = current;
        List<String> recentCountries = new ArrayList<>();
        BigDecimal sum = BigDecimal.ZERO;

        for (HistoryEntry e : entries) {
            long age = t - e.timestampMillis();
            if (age < ONE_MINUTE.toMillis()) {
                lastMinute++;
            }
            if (age < TEN_MINUTES.toMillis()) {
                lastTenMinutes = lastTenMinutes.add(e.amount());
                if (e.country() != null) {
                    recentCountries.add(e.country());
                }
            }
            sum = sum.add(e.amount());
        }

        String previousCountry = entries.isEmpty() ? null : entries.get(entries.size() - 1).country();
        BigDecimal average = entries.isEmpty()
                ? null
                : sum.divide(BigDecimal.valueOf(entries.size()), 4, RoundingMode.HALF_UP);

        return new RiskContext(lastMinute, lastTenMinutes, previousCountry, recentCountries, average, entries.size());
    }
}
