package io.github.jlmc.fraud.application.usecase;

import io.github.jlmc.fraud.application.port.in.ValidateTransactionUseCase;
import io.github.jlmc.fraud.application.port.out.ValidationRuleProvider;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionValidationRule;
import io.github.jlmc.fraud.validation.ValidationContext;
import io.github.jlmc.fraud.validation.ValidationResult;
import io.github.jlmc.fraud.validation.Violation;

import java.util.ArrayList;
import java.util.List;

/**
 * Applies all rules and merges their violations.
 *
 * <p>A plugin that throws is treated as a violation ({@value #RULE_ERROR}) instead of propagating: a faulty plugin
 * must never crash the job into a restart loop, and the transaction is rejected rather than silently accepted.
 */
public final class ValidateTransactionService implements ValidateTransactionUseCase {

    public static final String RULE_ERROR = "RULE_ERROR";

    private final List<TransactionValidationRule> rules;

    public ValidateTransactionService(ValidationRuleProvider provider) {
        this.rules = List.copyOf(provider.rules());
    }

    @Override
    public ValidationResult validate(Transaction transaction, ValidationContext context) {
        List<Violation> violations = new ArrayList<>();
        for (TransactionValidationRule rule : rules) {
            try {
                violations.addAll(rule.validate(transaction, context).violations());
            } catch (RuntimeException e) {
                violations.add(new Violation(RULE_ERROR, rule.name() + " failed: " + e));
            }
        }
        return ValidationResult.invalid(violations);
    }
}
