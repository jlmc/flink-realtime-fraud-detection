package io.github.jlmc.fraud.risk.rules.failedattempts;

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

class FailedAttemptsThenSuccessRuleTest {

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
                .anyMatch(r -> r instanceof FailedAttemptsThenSuccessRule);
    }

    private static RiskContext declinedBefore(int n) {
        return ctx(1, "1", null, null, null, 0, Set.of(), n);
    }

    @Test
    void anApprovalAfterEnoughDeclinesTriggers() {
        var rule = new FailedAttemptsThenSuccessRule(3);
        assertThat(rule.evaluate(tx("1", "PT"), declinedBefore(2)).isTriggered()).isFalse();
        var hit = rule.evaluate(tx("1", "PT"), declinedBefore(3));
        assertThat(hit.reason()).isEqualTo("FAILED_ATTEMPTS_THEN_SUCCESS");
        assertThat(hit.score()).isEqualTo(40);
    }

    @Test
    void aDeclinedAttemptIsNeverTheSuccess() {
        var rule = new FailedAttemptsThenSuccessRule(3);
        assertThat(rule.evaluate(declined("1", "PT"), declinedBefore(10)).isTriggered()).isFalse();
    }

    @Test
    void theLimitComesFromTheSettings() {
        var rule = new FailedAttemptsThenSuccessRule();
        assertThat(rule.name()).isEqualTo("failed-attempts-then-success");
        rule.configure(Map.of("risk.max-declined-per-five-minutes", "2"));
        assertThat(rule.evaluate(tx("1", "PT"), declinedBefore(2)).isTriggered()).isTrue();
    }
}
