package com.example.fraud.validation.rules.minimumamount;

import com.example.fraud.validation.Transaction;
import com.example.fraud.validation.TransactionValidationRule;
import com.example.fraud.validation.ValidationContext;
import com.example.fraud.validation.ValidationResult;

import java.math.BigDecimal;

/**
 * Rejects transactions with {@code amount <= 0}.
 *
 * <p>A missing amount is not judged here; that is the job of the schema rule.
 */
public class MinimumAmountRule implements TransactionValidationRule {

    public static final String CODE = "AMOUNT_NOT_POSITIVE";

    @Override
    public ValidationResult validate(Transaction transaction, ValidationContext context) {
        BigDecimal amount = transaction.amount();
        if (amount != null && amount.signum() <= 0) {
            return ValidationResult.invalid(CODE, "amount must be greater than 0 but was " + amount);
        }
        return ValidationResult.valid();
    }
}
