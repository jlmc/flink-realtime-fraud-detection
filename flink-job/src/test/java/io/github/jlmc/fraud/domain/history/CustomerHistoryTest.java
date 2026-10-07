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

    @Test
    void lateEntriesAreInsertedInEventTimeOrder() {
        CustomerHistory h = CustomerHistory.empty()
                .append(entry(0, "1", "PT"), Duration.ofHours(1), 10)
                .append(entry(120, "3", "PT"), Duration.ofHours(1), 10)
                .append(entry(60, "2", "PT"), Duration.ofHours(1), 10);

        assertThat(h.entries()).extracting(e -> e.amount().intValue()).containsExactly(1, 2, 3);
    }

    @Test
    void entriesAfterTheEvaluatedTransactionAreIgnored() {
        CustomerHistory h = new CustomerHistory(java.util.List.of(entry(0, "10", "PT"), entry(100, "999", "US")));

        RiskContext ctx = h.contextFor(tx(50, "5", "PT"));

        assertThat(ctx.transactionsLastMinute()).isEqualTo(2);
        assertThat(ctx.amountLastTenMinutes()).isEqualByComparingTo("15");
        assertThat(ctx.recentCountries()).containsExactly("PT");
        assertThat(ctx.recentTransactionCount()).isEqualTo(1);
    }

    @Test
    void evictOlderThanDropsExpiredEntries() {
        CustomerHistory h = new CustomerHistory(java.util.List.of(entry(0, "1", "PT"), entry(600, "2", "PT")));

        assertThat(h.evictOlderThan(T0.plusSeconds(700).toEpochMilli(), Duration.ofMinutes(5)).entries())
                .extracting(e -> e.amount().intValue()).containsExactly(2);
        assertThat(h.evictOlderThan(T0.plusSeconds(2000).toEpochMilli(), Duration.ofMinutes(5)).isEmpty()).isTrue();
    }

    // ---- declined attempts, previous transaction, known countries

    private static HistoryEntry declined(long secondsAfterT0, String amount, String country) {
        return new HistoryEntry(T0.plusSeconds(secondsAfterT0).toEpochMilli(), new BigDecimal(amount), country, true);
    }

    @Test
    void declinedAttemptsCountInTheVelocityButSpendNothing() {
        CustomerHistory h = new CustomerHistory(java.util.List.of(
                entry(0, "100", "PT"),
                declined(10, "900", "PT"),
                declined(20, "900", "PT")));

        RiskContext ctx = h.contextFor(tx(30, "50", "PT"));

        assertThat(ctx.transactionsLastMinute()).isEqualTo(4);                 // 3 past attempts + the current one
        assertThat(ctx.amountLastTenMinutes()).isEqualByComparingTo("150");     // declined 900s are not spending
        assertThat(ctx.averageRecentAmount()).isEqualByComparingTo("100");      // mean of the approved ones only
        assertThat(ctx.recentTransactionCount()).isEqualTo(1);
    }

    @Test
    void aDeclinedCurrentTransactionAddsNoSpending() {
        Transaction current = new Transaction("t", "c", "m", new BigDecimal("700"), "EUR", "PT", T0, io.github.jlmc.fraud.validation.PaymentStatus.DECLINED);

        assertThat(CustomerHistory.empty().contextFor(current).amountLastTenMinutes()).isEqualByComparingTo("0");
    }

    @Test
    void countsOnlyTheDeclinedAttemptsOfTheLastFiveMinutes() {
        CustomerHistory h = new CustomerHistory(java.util.List.of(
                declined(0, "1", "PT"),      // exactly 5 minutes before -> outside
                declined(1, "1", "PT"),      // inside
                declined(200, "1", "PT"),    // inside
                entry(250, "1", "PT")));     // approved, not counted

        assertThat(h.contextFor(tx(300, "1", "PT")).declinedLastFiveMinutes()).isEqualTo(2);
    }

    @Test
    void reportsTheTimeSincePreviousAndEveryKnownCountry() {
        CustomerHistory h = new CustomerHistory(java.util.List.of(
                entry(-3000, "1", "ES"),     // outside the 10 minute window, still a known country
                entry(0, "1", "PT"),
                entry(100, "1", "US")));

        RiskContext ctx = h.contextFor(tx(160, "1", "PT"));

        assertThat(ctx.timeSincePreviousTransaction()).isEqualTo(Duration.ofSeconds(60));
        assertThat(ctx.previousCountry()).isEqualTo("US");
        assertThat(ctx.knownCountries()).containsExactlyInAnyOrder("ES", "PT", "US");
    }

    @Test
    void withoutHistoryThereIsNoPreviousTimeAndNoKnownCountry() {
        RiskContext ctx = CustomerHistory.empty().contextFor(tx(0, "1", "PT"));

        assertThat(ctx.timeSincePreviousTransaction()).isNull();
        assertThat(ctx.knownCountries()).isEmpty();
        assertThat(ctx.declinedLastFiveMinutes()).isZero();
    }
}
