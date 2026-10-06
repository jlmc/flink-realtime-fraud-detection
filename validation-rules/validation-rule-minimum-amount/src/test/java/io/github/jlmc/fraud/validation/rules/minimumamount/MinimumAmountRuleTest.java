package io.github.jlmc.fraud.validation.rules.minimumamount;

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

class MinimumAmountRuleTest {

    private static final ValidationContext CTX = new ValidationContext(Instant.parse("2026-10-06T13:10:01Z"));
    private final MinimumAmountRule rule = new MinimumAmountRule();

    private static Transaction tx(String id, String amount, String currency) {
        return new Transaction(id, "customer-42", "merchant-10",
                amount == null ? null : new BigDecimal(amount), currency, "PT", Instant.parse("2026-10-06T13:10:01Z"));
    }

    @Test
    void acceptsPositiveAmount() {
        assertThat(rule.validate(tx("t1", "0.01", "EUR"), CTX).isValid()).isTrue();
    }

    @Test
    void rejectsZeroAndNegative() {
        for (String amount : new String[] {"0", "0.00", "-5"}) {
            ValidationResult r = rule.validate(tx("t1", amount, "EUR"), CTX);
            assertThat(r.violations()).extracting("code").containsExactly(MinimumAmountRule.CODE);
        }
    }

    @Test
    void ignoresMissingAmount() {
        assertThat(rule.validate(tx("t1", null, "EUR"), CTX).isValid()).isTrue();
    }

    @Test
    void isDiscoverableThroughServiceLoader() {
        boolean found = StreamSupport.stream(ServiceLoader.load(TransactionValidationRule.class).spliterator(), false)
                .anyMatch(r -> r instanceof MinimumAmountRule);
        assertThat(found).isTrue();
    }
}
