package io.github.jlmc.fraud.risk.rules.newcountry;

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

class NewCountryHighAmountRuleTest {

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
                .anyMatch(r -> r instanceof NewCountryHighAmountRule);
    }

    private static RiskContext history(String avg, int count, Set<String> known) {
        return ctx(1, "1", null, null, avg, count, known, 0);
    }

    @Test
    void aNewCountryNeedsAHighAmount() {
        var rule = new NewCountryHighAmountRule(new BigDecimal("2"), 3);
        var context = history("100", 3, Set.of("PT"));
        assertThat(rule.evaluate(tx("200", "US"), context).isTriggered()).isFalse();   // == 2x
        var hit = rule.evaluate(tx("200.01", "US"), context);
        assertThat(hit.reason()).isEqualTo("NEW_COUNTRY_HIGH_AMOUNT");
        assertThat(hit.score()).isEqualTo(40);
    }

    @Test
    void aKnownCountryOrTooLittleHistoryNeverTriggers() {
        var rule = new NewCountryHighAmountRule(new BigDecimal("2"), 3);
        assertThat(rule.evaluate(tx("9999", "PT"), history("100", 3, Set.of("PT"))).isTriggered()).isFalse();
        assertThat(rule.evaluate(tx("9999", "US"), history("100", 2, Set.of("PT"))).isTriggered()).isFalse();
        assertThat(rule.evaluate(tx("9999", "US"), history(null, 0, Set.of())).isTriggered()).isFalse();
        assertThat(rule.evaluate(tx("9999", null), history("100", 3, Set.of("PT"))).isTriggered()).isFalse();
    }

    @Test
    void aDeclinedAttemptIsNotJudged() {
        var rule = new NewCountryHighAmountRule(new BigDecimal("2"), 3);
        assertThat(rule.evaluate(declined("9999", "US"), history("100", 3, Set.of("PT"))).isTriggered()).isFalse();
    }

    @Test
    void thresholdsComeFromTheSettings() {
        var rule = new NewCountryHighAmountRule();
        assertThat(rule.name()).isEqualTo("new-country-high-amount");
        rule.configure(Map.of("risk.new-country-amount-multiplier", "4", "risk.new-country-min-history", "1"));
        assertThat(rule.evaluate(tx("401", "US"), history("100", 1, Set.of("PT"))).isTriggered()).isTrue();
        assertThat(rule.evaluate(tx("400", "US"), history("100", 1, Set.of("PT"))).isTriggered()).isFalse();
    }
}
