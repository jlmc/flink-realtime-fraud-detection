package com.example.fraud.validation.rules.schema;

import com.example.fraud.validation.Transaction;
import com.example.fraud.validation.TransactionValidationRule;
import com.example.fraud.validation.ValidationContext;
import com.example.fraud.validation.ValidationResult;
import com.example.fraud.validation.Violation;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates that the required fields are present: transactionId, customerId, merchantId, amount,
 * currency and timestamp. Reports every missing field, not only the first.
 */
public class RequiredFieldsRule implements TransactionValidationRule {

    public static final String CODE = "REQUIRED_FIELD_MISSING";

    @Override
    public ValidationResult validate(Transaction transaction, ValidationContext context) {
        List<Violation> violations = new ArrayList<>();
        requireText(violations, "transactionId", transaction.transactionId());
        requireText(violations, "customerId", transaction.customerId());
        requireText(violations, "merchantId", transaction.merchantId());
        requireText(violations, "currency", transaction.currency());
        if (transaction.amount() == null) {
            violations.add(missing("amount"));
        }
        if (transaction.timestamp() == null) {
            violations.add(missing("timestamp"));
        }
        return ValidationResult.invalid(violations);
    }

    private static void requireText(List<Violation> violations, String field, String value) {
        if (value == null || value.isBlank()) {
            violations.add(missing(field));
        }
    }

    private static Violation missing(String field) {
        return new Violation(CODE, "required field is missing or blank: " + field);
    }
}
