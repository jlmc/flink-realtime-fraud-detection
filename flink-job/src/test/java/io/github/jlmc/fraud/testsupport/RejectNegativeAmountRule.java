package io.github.jlmc.fraud.testsupport;

import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionValidationRule;
import io.github.jlmc.fraud.validation.ValidationContext;
import io.github.jlmc.fraud.validation.ValidationResult;

/** Test-only plugin, registered through src/test/resources/META-INF/services. NOT one of the real rule modules. */
public class RejectNegativeAmountRule implements TransactionValidationRule {

    @Override
    public ValidationResult validate(Transaction transaction, ValidationContext context) {
        return transaction.amount() != null && transaction.amount().signum() < 0
                ? ValidationResult.invalid("NEGATIVE_AMOUNT", "amount is negative")
                : ValidationResult.valid();
    }
}
