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
     * Returns a new history with {@code entry} inserted in event-time order (after entries with the same timestamp),
     * dropping entries older than {@code retention} (relative to the newest entry) and keeping at most
     * {@code maxEntries} of the newest ones.
     *
     * <p>Normally entries arrive in order; out-of-order insertion only happens for late events that the pipeline
     * chooses to tolerate.
     */
    public CustomerHistory append(HistoryEntry entry, Duration retention, int maxEntries) {
        List<HistoryEntry> next = new ArrayList<>(entries.size() + 1);
        next.addAll(entries);
        int index = next.size();
        while (index > 0 && next.get(index - 1).timestampMillis() > entry.timestampMillis()) {
            index--;
        }
        next.add(index, entry);

        long oldestKept = next.get(next.size() - 1).timestampMillis() - retention.toMillis();
        int from = 0;
        while (from < next.size() && next.get(from).timestampMillis() < oldestKept) {
            from++;
        }
        from = Math.max(from, next.size() - maxEntries);
        return new CustomerHistory(next.subList(from, next.size()));
    }

    /** Returns a history without the entries older than {@code retention} relative to {@code nowMillis}. */
    public CustomerHistory evictOlderThan(long nowMillis, Duration retention) {
        long oldestKept = nowMillis - retention.toMillis();
        return new CustomerHistory(entries.stream().filter(e -> e.timestampMillis() >= oldestKept).toList());
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /**
     * Builds the {@link RiskContext} for {@code transaction}. Windows are {@code (t - window, t]} where {@code t}
     * is the transaction's event time, and include the transaction itself. Entries after {@code t} (possible only
     * when a late event is tolerated) are ignored.
     */
    public RiskContext contextFor(Transaction transaction) {
        long t = transaction.timestamp().toEpochMilli();
        BigDecimal current = transaction.amount() == null ? BigDecimal.ZERO : transaction.amount();

        int lastMinute = 1;
        BigDecimal lastTenMinutes = current;
        List<String> recentCountries = new ArrayList<>();
        BigDecimal sum = BigDecimal.ZERO;

        List<HistoryEntry> past = entries.stream().filter(e -> e.timestampMillis() <= t).toList();

        for (HistoryEntry e : past) {
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

        String previousCountry = past.isEmpty() ? null : past.get(past.size() - 1).country();
        BigDecimal average = past.isEmpty()
                ? null
                : sum.divide(BigDecimal.valueOf(past.size()), 4, RoundingMode.HALF_UP);

        return new RiskContext(lastMinute, lastTenMinutes, previousCountry, recentCountries, average, past.size());
    }
}
