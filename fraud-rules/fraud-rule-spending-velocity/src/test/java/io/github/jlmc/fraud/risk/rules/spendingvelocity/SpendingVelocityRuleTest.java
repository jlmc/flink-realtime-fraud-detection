package io.github.jlmc.fraud.risk.rules.spendingvelocity;

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

class SpendingVelocityRuleTest {

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
                .anyMatch(r -> r instanceof SpendingVelocityRule);
    }

    @Test
    void triggersOnlyAboveTheLimit() {
        var rule = new SpendingVelocityRule(new BigDecimal("5000"));
        assertThat(rule.evaluate(tx("1", "PT"), ctx(1, "5000", null, null, null, 0, Set.of(), 0)).isTriggered()).isFalse();
        var hit = rule.evaluate(tx("1", "PT"), ctx(1, "5000.01", null, null, null, 0, Set.of(), 0));
        assertThat(hit.reason()).isEqualTo("HIGH_SPENDING_VELOCITY");
        assertThat(hit.score()).isEqualTo(40);
    }

    @Test
    void theLimitComesFromTheSettings() {
        var rule = new SpendingVelocityRule();
        assertThat(rule.name()).isEqualTo("spending-velocity");
        assertThat(rule.evaluate(tx("1", "PT"), ctx(1, "5000.01", null, null, null, 0, Set.of(), 0)).isTriggered()).isTrue();
        rule.configure(Map.of("risk.max-amount-per-ten-minutes", "9000"));
        assertThat(rule.evaluate(tx("1", "PT"), ctx(1, "5000.01", null, null, null, 0, Set.of(), 0)).isTriggered()).isFalse();
    }
}
