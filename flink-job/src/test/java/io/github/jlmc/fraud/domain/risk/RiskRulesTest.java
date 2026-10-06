package io.github.jlmc.fraud.domain.risk;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.Transaction;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RiskRulesTest {

    private static Transaction tx(String amount, String country) {
        return new Transaction("t", "c", "m", amount == null ? null : new BigDecimal(amount), "EUR", country,
                Instant.parse("2026-10-06T13:00:00Z"));
    }

    private static RiskContext ctx(int perMinute, String tenMinutes, List<String> countries, String avg, int count) {
        return new RiskContext(perMinute, new BigDecimal(tenMinutes), countries.isEmpty() ? null : countries.get(countries.size() - 1),
                countries, avg == null ? null : new BigDecimal(avg), count);
    }

    // ---- velocity
    @Test
    void velocityTriggersOnlyAboveTheLimit() {
        var rule = new TransactionVelocityRule(5);
        assertThat(rule.evaluate(tx("1", "PT"), ctx(5, "1", List.of(), null, 0)).isTriggered()).isFalse();
        var hit = rule.evaluate(tx("1", "PT"), ctx(6, "1", List.of(), null, 0));
        assertThat(hit.isTriggered()).isTrue();
        assertThat(hit.reason()).isEqualTo("HIGH_TRANSACTION_VELOCITY");
    }

    // ---- spending
    @Test
    void spendingTriggersOnlyAboveTheLimit() {
        var rule = new SpendingVelocityRule(new BigDecimal("5000"));
        assertThat(rule.evaluate(tx("1", "PT"), ctx(1, "5000", List.of(), null, 0)).isTriggered()).isFalse();
        assertThat(rule.evaluate(tx("1", "PT"), ctx(1, "5000.01", List.of(), null, 0)).reason())
                .isEqualTo("HIGH_SPENDING_VELOCITY");
    }

    // ---- country change
    @Test
    void countryChangeDetectsAba() {
        var rule = new CountryChangeRule();
        assertThat(rule.evaluate(tx("1", "PT"), ctx(1, "1", List.of("PT", "US"), null, 0)).reason())
                .isEqualTo("SUSPICIOUS_COUNTRY_CHANGE");
    }

    @Test
    void countryChangeIgnoresOtherPatterns() {
        var rule = new CountryChangeRule();
        assertThat(rule.evaluate(tx("1", "PT"), ctx(1, "1", List.of("PT", "PT"), null, 0)).isTriggered()).isFalse(); // A A A
        assertThat(rule.evaluate(tx("1", "ES"), ctx(1, "1", List.of("PT", "US"), null, 0)).isTriggered()).isFalse(); // A B C
        assertThat(rule.evaluate(tx("1", "PT"), ctx(1, "1", List.of("US"), null, 0)).isTriggered()).isFalse();       // too short
        assertThat(rule.evaluate(tx("1", null), ctx(1, "1", List.of("PT", "US"), null, 0)).isTriggered()).isFalse(); // no country
    }

    // ---- amount anomaly
    @Test
    void anomalyTriggersAboveMultipleOfTheAverage() {
        var rule = new AmountAnomalyRule(new BigDecimal("5"), 3);
        assertThat(rule.evaluate(tx("500", "PT"), ctx(1, "1", List.of(), "100", 3)).isTriggered()).isFalse(); // == 5x
        assertThat(rule.evaluate(tx("500.01", "PT"), ctx(1, "1", List.of(), "100", 3)).reason()).isEqualTo("UNUSUAL_AMOUNT");
    }

    @Test
    void anomalyNeedsEnoughHistory() {
        var rule = new AmountAnomalyRule(new BigDecimal("5"), 3);
        assertThat(rule.evaluate(tx("9999", "PT"), ctx(1, "1", List.of(), "100", 2)).isTriggered()).isFalse();
        assertThat(rule.evaluate(tx("9999", "PT"), ctx(1, "1", List.of(), null, 0)).isTriggered()).isFalse();
        assertThat(rule.evaluate(tx("9999", "PT"), ctx(1, "1", List.of(), "0", 5)).isTriggered()).isFalse();
    }

    @Test
    void factoryBuildsTheFourBuiltInRules() {
        assertThat(RiskRules.from(RiskThresholds.defaults())).extracting("class")
                .extracting(c -> ((Class<?>) c).getSimpleName())
                .containsExactly("TransactionVelocityRule", "SpendingVelocityRule", "CountryChangeRule", "AmountAnomalyRule");
    }
}
