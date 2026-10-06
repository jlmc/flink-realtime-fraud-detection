package io.github.jlmc.fraud.domain.history;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.Transaction;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class CustomerHistoryTest {

    private static final Instant T0 = Instant.parse("2026-10-06T13:00:00Z");

    private static Transaction tx(long secondsAfterT0, String amount, String country) {
        return new Transaction("t", "c", "m", new BigDecimal(amount), "EUR", country, T0.plusSeconds(secondsAfterT0));
    }

    private static HistoryEntry entry(long secondsAfterT0, String amount, String country) {
        return new HistoryEntry(T0.plusSeconds(secondsAfterT0).toEpochMilli(), new BigDecimal(amount), country);
    }

    @Test
    void emptyHistoryYieldsContextWithOnlyTheCurrentTransaction() {
        RiskContext ctx = CustomerHistory.empty().contextFor(tx(0, "10", "PT"));

        assertThat(ctx.transactionsLastMinute()).isEqualTo(1);
        assertThat(ctx.amountLastTenMinutes()).isEqualByComparingTo("10");
        assertThat(ctx.previousCountry()).isNull();
        assertThat(ctx.recentCountries()).isEmpty();
        assertThat(ctx.averageRecentAmount()).isNull();
        assertThat(ctx.recentTransactionCount()).isZero();
    }

    @Test
    void oneMinuteWindowIsHalfOpenAndIncludesTheCurrentTransaction() {
        CustomerHistory h = new CustomerHistory(java.util.List.of(
                entry(0, "1", "PT"),    // exactly 60s before the current one -> outside
                entry(1, "1", "PT"),    // 59s before -> inside
                entry(30, "1", "PT"))); // inside

        assertThat(h.contextFor(tx(60, "1", "PT")).transactionsLastMinute()).isEqualTo(3);
    }

    @Test
    void tenMinuteWindowSumsAmountsAndCollectsCountriesInOrder() {
        CustomerHistory h = new CustomerHistory(java.util.List.of(
                entry(-3600, "1000", "ES"),  // far outside the 10 minute window
                entry(0, "100", "PT"),
                entry(60, "200", "US")));

        RiskContext ctx = h.contextFor(tx(120, "50", "PT"));

        assertThat(ctx.amountLastTenMinutes()).isEqualByComparingTo("350");
        assertThat(ctx.recentCountries()).containsExactly("PT", "US");
        assertThat(ctx.previousCountry()).isEqualTo("US");
    }

    @Test
    void averageCoversTheWholeHistoryExcludingTheCurrentTransaction() {
        CustomerHistory h = new CustomerHistory(java.util.List.of(entry(0, "10", "PT"), entry(1, "30", "PT")));

        RiskContext ctx = h.contextFor(tx(2, "999", "PT"));

        assertThat(ctx.averageRecentAmount()).isEqualByComparingTo("20");
        assertThat(ctx.recentTransactionCount()).isEqualTo(2);
    }

    @Test
    void appendDropsExpiredEntriesAndKeepsTheNewestWithinTheBound() {
        CustomerHistory h = CustomerHistory.empty()
                .append(entry(0, "1", "PT"), Duration.ofMinutes(10), 3)
                .append(entry(60, "2", "PT"), Duration.ofMinutes(10), 3)
                .append(entry(120, "3", "PT"), Duration.ofMinutes(10), 3)
                .append(entry(180, "4", "PT"), Duration.ofMinutes(10), 3);

        assertThat(h.entries()).extracting(e -> e.amount().intValue()).containsExactly(2, 3, 4);

        CustomerHistory expired = h.append(entry(180 + 601, "5", "PT"), Duration.ofMinutes(10), 3);
        assertThat(expired.entries()).extracting(e -> e.amount().intValue()).containsExactly(5);
    }

    @Test
    void missingAmountCountsAsZero() {
        assertThat(new HistoryEntry(1L, null, null).amount()).isEqualByComparingTo("0");
    }
}
