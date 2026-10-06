package io.github.jlmc.fraud.validation.rules.currency;

import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionValidationRule;
import io.github.jlmc.fraud.validation.ValidationContext;
import io.github.jlmc.fraud.validation.ValidationResult;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

class SupportedCurrencyRuleTest {

    private static final ValidationContext CTX = new ValidationContext(Instant.parse("2026-10-06T13:10:01Z"));
    private final SupportedCurrencyRule rule = new SupportedCurrencyRule();

    private static Transaction tx(String id, String amount, String currency) {
        return new Transaction(id, "customer-42", "merchant-10",
                amount == null ? null : new BigDecimal(amount), currency, "PT", Instant.parse("2026-10-06T13:10:01Z"));
    }

    @Test
    void acceptsSupportedCurrencies() {
        for (String c : new String[] {"EUR", "USD", "GBP"}) {
            assertThat(rule.validate(tx("t1", "10", c), CTX).isValid()).isTrue();
        }
    }

    @Test
    void rejectsUnsupportedCurrency() {
        ValidationResult r = rule.validate(tx("t1", "10", "JPY"), CTX);
        assertThat(r.violations()).extracting("code").containsExactly(SupportedCurrencyRule.CODE);
    }

    @Test
    void ignoresMissingCurrency() {
        assertThat(rule.validate(tx("t1", "10", null), CTX).isValid()).isTrue();
    }

    @Test
    void isDiscoverableThroughServiceLoader() {
        boolean found = StreamSupport.stream(ServiceLoader.load(TransactionValidationRule.class).spliterator(), false)
                .anyMatch(r -> r instanceof SupportedCurrencyRule);
        assertThat(found).isTrue();
    }
}
