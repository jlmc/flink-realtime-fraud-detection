package io.github.jlmc.fraud.validation.rules.schema;

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

class RequiredFieldsRuleTest {

    private static final ValidationContext CTX = new ValidationContext(Instant.parse("2026-10-06T13:10:01Z"));
    private final RequiredFieldsRule rule = new RequiredFieldsRule();

    private static Transaction tx(String id, String amount, String currency) {
        return new Transaction(id, "customer-42", "merchant-10",
                amount == null ? null : new BigDecimal(amount), currency, "PT", Instant.parse("2026-10-06T13:10:01Z"));
    }

    @Test
    void acceptsCompleteTransaction() {
        assertThat(rule.validate(tx("t1", "10", "EUR"), CTX).isValid()).isTrue();
    }

    @Test
    void reportsEveryMissingField() {
        Transaction empty = new Transaction(null, " ", null, null, null, null, null);
        ValidationResult r = rule.validate(empty, CTX);
        assertThat(r.violations()).hasSize(6);
        assertThat(r.violations()).extracting("message")
                .anyMatch(m -> m.toString().contains("transactionId"))
                .anyMatch(m -> m.toString().contains("customerId"))
                .anyMatch(m -> m.toString().contains("timestamp"));
    }

    @Test
    void isDiscoverableThroughServiceLoader() {
        boolean found = StreamSupport.stream(ServiceLoader.load(TransactionValidationRule.class).spliterator(), false)
                .anyMatch(r -> r instanceof RequiredFieldsRule);
        assertThat(found).isTrue();
    }
}
