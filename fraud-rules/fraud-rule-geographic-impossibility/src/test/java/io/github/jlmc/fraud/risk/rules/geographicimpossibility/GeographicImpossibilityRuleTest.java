package io.github.jlmc.fraud.risk.rules.geographicimpossibility;

import io.github.jlmc.fraud.validation.PaymentStatus;
import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

class GeographicImpossibilityRuleTest {

    private static final Instant AT = Instant.parse("2026-10-06T13:00:00Z");

    private static Transaction tx(String amount, String country) {
        return new Transaction("t", "c", "m", amount == null ? null : new BigDecimal(amount), "EUR", country, AT);
    }

    private static Transaction declined(String amount, String country) {
        return new Transaction("t", "c", "m", new BigDecimal(amount), "EUR", country, AT, PaymentStatus.DECLINED);
    }

    private static RiskContext ctx(int perMinute, String tenMinutes, String previousCountry, Duration since, String avg, int count,
                                   Set<String> known, int declined) {
        return new RiskContext(perMinute, new BigDecimal(tenMinutes), previousCountry,
                previousCountry == null ? List.of() : List.of(previousCountry), avg == null ? null : new BigDecimal(avg),
                count, since, known, declined);
    }

    private static RiskContext plain() {
        return ctx(1, "1", null, null, null, 0, Set.of(), 0);
    }

    @Test
    void isRegisteredThroughServiceLoader() {
        assertThat(StreamSupport.stream(ServiceLoader.load(TransactionRiskRule.class).spliterator(), false))
                .anyMatch(r -> r instanceof GeographicImpossibilityRule);
    }

    private static RiskContext travel(String previousCountry, Duration since) {
        return ctx(1, "1", previousCountry, since, null, 0, previousCountry == null ? Set.of() : Set.of(previousCountry), 0);
    }

    @Test
    void triggersForAnotherCountryInsideTheWindow() {
        var rule = new GeographicImpossibilityRule(Duration.ofMinutes(10));
        var hit = rule.evaluate(tx("1", "US"), travel("PT", Duration.ofMinutes(3)));
        assertThat(hit.isTriggered()).isTrue();
        assertThat(hit.reason()).isEqualTo("GEOGRAPHIC_IMPOSSIBILITY");
        assertThat(hit.score()).isEqualTo(50);
    }

    @Test
    void theWindowBoundaryIsExclusive() {
        var rule = new GeographicImpossibilityRule(Duration.ofMinutes(10));
        assertThat(rule.evaluate(tx("1", "US"), travel("PT", Duration.ofMinutes(10).minusMillis(1))).isTriggered()).isTrue();
        assertThat(rule.evaluate(tx("1", "US"), travel("PT", Duration.ofMinutes(10))).isTriggered()).isFalse();
    }

    @Test
    void ignoresTheSameCountryAndMissingData() {
        var rule = new GeographicImpossibilityRule(Duration.ofMinutes(10));
        assertThat(rule.evaluate(tx("1", "PT"), travel("PT", Duration.ofMinutes(1))).isTriggered()).isFalse();
        assertThat(rule.evaluate(tx("1", "US"), travel(null, null)).isTriggered()).isFalse();
        assertThat(rule.evaluate(tx("1", null), travel("PT", Duration.ofMinutes(1))).isTriggered()).isFalse();
    }

    @Test
    void theWindowComesFromTheSettings() {
        var rule = new GeographicImpossibilityRule();
        assertThat(rule.name()).isEqualTo("geographic-impossibility");
        assertThat(rule.evaluate(tx("1", "US"), travel("PT", Duration.ofMinutes(20))).isTriggered()).isFalse();
        rule.configure(Map.of("risk.impossible-travel-minutes", "30"));
        assertThat(rule.evaluate(tx("1", "US"), travel("PT", Duration.ofMinutes(20))).isTriggered()).isTrue();
    }
}
