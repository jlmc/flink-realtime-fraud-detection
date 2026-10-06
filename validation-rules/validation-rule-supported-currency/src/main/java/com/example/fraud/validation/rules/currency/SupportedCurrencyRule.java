package com.example.fraud.validation.rules.currency;

import com.example.fraud.validation.Transaction;
import com.example.fraud.validation.TransactionValidationRule;
import com.example.fraud.validation.ValidationContext;
import com.example.fraud.validation.ValidationResult;

import java.util.Set;

/**
 * Accepts only EUR, USD and GBP (case-sensitive ISO 4217 codes).
 *
 * <p>A missing currency is not judged here; that is the job of the schema rule.
 */
public class SupportedCurrencyRule implements TransactionValidationRule {

    public static final String CODE = "CURRENCY_NOT_SUPPORTED";

    private static final Set<String> SUPPORTED = Set.of("EUR", "USD", "GBP");

    @Override
    public ValidationResult validate(Transaction transaction, ValidationContext context) {
        String currency = transaction.currency();
        if (currency != null && !SUPPORTED.contains(currency)) {
            return ValidationResult.invalid(CODE, "currency not supported: " + currency);
        }
        return ValidationResult.valid();
    }
}
