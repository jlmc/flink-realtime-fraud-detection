package io.github.jlmc.fraud.application.usecase;

import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionValidationRule;
import io.github.jlmc.fraud.validation.ValidationContext;
import io.github.jlmc.fraud.validation.ValidationResult;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Uses stub rules on purpose: flink-job must never see the concrete rule modules. */
class ValidateTransactionServiceTest {

    private static final Transaction TX = new Transaction("t", "c", "m", BigDecimal.TEN, "EUR", "PT", Instant.EPOCH);
    private static final ValidationContext CTX = new ValidationContext(Instant.EPOCH);

    private static TransactionValidationRule rule(String name, ValidationResult result) {
        return new TransactionValidationRule() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public ValidationResult validate(Transaction t, ValidationContext c) {
                return result;
            }
        };
    }

    private static ValidateTransactionService service(TransactionValidationRule... rules) {
        return new ValidateTransactionService(() -> List.of(rules));
    }

    @Test
    void validWhenEveryRulePasses() {
        assertThat(service(rule("a", ValidationResult.valid()), rule("b", ValidationResult.valid())).validate(TX, CTX).isValid())
                .isTrue();
    }

    @Test
    void mergesViolationsFromAllFailingRules() {
        var result = service(rule("a", ValidationResult.invalid("A", "a failed")),
                rule("b", ValidationResult.valid()),
                rule("c", ValidationResult.invalid("C", "c failed"))).validate(TX, CTX);

        assertThat(result.violations()).extracting("code").containsExactly("A", "C");
    }

    @Test
    void aPluginThatThrowsRejectsTheTransactionInsteadOfCrashing() {
        TransactionValidationRule boom = new TransactionValidationRule() {
            @Override
            public String name() {
                return "boom";
            }

            @Override
            public ValidationResult validate(Transaction t, ValidationContext c) {
                throw new IllegalStateException("bug in plugin");
            }
        };

        var result = service(boom, rule("ok", ValidationResult.valid())).validate(TX, CTX);

        assertThat(result.isValid()).isFalse();
        assertThat(result.violations()).singleElement().satisfies(v -> {
            assertThat(v.code()).isEqualTo(ValidateTransactionService.RULE_ERROR);
            assertThat(v.message()).contains("boom").contains("bug in plugin");
        });
    }

    @Test
    void pipelineInvariantsAreEnforcedEvenWithoutAnyPlugin() {
        var empty = new Transaction(" ", null, "m", BigDecimal.TEN, "EUR", "PT", null);

        var result = service().validate(empty, CTX);

        assertThat(result.violations()).extracting("code").containsExactlyInAnyOrder(
                ValidateTransactionService.TRANSACTION_ID_MISSING,
                ValidateTransactionService.CUSTOMER_ID_MISSING,
                ValidateTransactionService.TIMESTAMP_MISSING);
    }
}
