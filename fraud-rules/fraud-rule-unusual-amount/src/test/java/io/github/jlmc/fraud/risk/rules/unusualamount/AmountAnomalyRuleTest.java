package io.github.jlmc.fraud.risk.rules.unusualamount;

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

class AmountAnomalyRuleTest {

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
                .anyMatch(r -> r instanceof AmountAnomalyRule);
    }

    @Test
    void triggersAboveTheMultipleOfTheAverage() {
        var rule = new AmountAnomalyRule(new BigDecimal("5"), 3);
        assertThat(rule.evaluate(tx("500", "PT"), ctx(1, "1", null, null, "100", 3, Set.of(), 0)).isTriggered()).isFalse(); // == 5x
        var hit = rule.evaluate(tx("500.01", "PT"), ctx(1, "1", null, null, "100", 3, Set.of(), 0));
        assertThat(hit.reason()).isEqualTo("UNUSUAL_AMOUNT");
        assertThat(hit.score()).isEqualTo(30);
    }

    @Test
    void needsEnoughHistory() {
        var rule = new AmountAnomalyRule(new BigDecimal("5"), 3);
        assertThat(rule.evaluate(tx("9999", "PT"), ctx(1, "1", null, null, "100", 2, Set.of(), 0)).isTriggered()).isFalse();
        assertThat(rule.evaluate(tx("9999", "PT"), plain()).isTriggered()).isFalse();
        assertThat(rule.evaluate(tx("9999", "PT"), ctx(1, "1", null, null, "0", 5, Set.of(), 0)).isTriggered()).isFalse();
        assertThat(rule.evaluate(tx(null, "PT"), ctx(1, "1", null, null, "100", 5, Set.of(), 0)).isTriggered()).isFalse();
    }

    @Test
    void thresholdsComeFromTheSettings() {
        var rule = new AmountAnomalyRule();
        assertThat(rule.name()).isEqualTo("amount-anomaly");
        rule.configure(Map.of("risk.anomaly-multiplier", "2", "risk.anomaly-min-history", "1"));
        assertThat(rule.evaluate(tx("201", "PT"), ctx(1, "1", null, null, "100", 1, Set.of(), 0)).isTriggered()).isTrue();
        assertThat(rule.evaluate(tx("200", "PT"), ctx(1, "1", null, null, "100", 1, Set.of(), 0)).isTriggered()).isFalse();
    }
}
