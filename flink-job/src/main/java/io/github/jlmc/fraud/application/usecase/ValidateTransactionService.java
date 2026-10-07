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
 * <p>Independently of the plugins, the pipeline needs three fields to work at all: {@code transactionId} (deduplication
 * key), {@code customerId} (partition key) and {@code timestamp} (event time). Their absence is always a violation, so a
 * deployment without the schema plugin cannot crash the job with a null key.
 *
 * <p>A plugin that throws is treated as a violation ({@value #RULE_ERROR}) instead of propagating: a faulty plugin
 * must never crash the job into a restart loop, and the transaction is rejected rather than silently accepted.
 *
 * <p>Rules run sequentially, like the risk rules (see {@link EvaluateRiskService} and
 * docs/decisions/0004-sequential-rule-evaluation.md). A plugin that blocks (I/O, a lock) stalls the task, and there is no
 * per-rule time limit yet; that, and Async I/O if a rule needs a remote call, is the improvement to make, not parallel execution.
 */
public final class ValidateTransactionService implements ValidateTransactionUseCase {

    public static final String RULE_ERROR = "RULE_ERROR";
    public static final String TRANSACTION_ID_MISSING = "PIPELINE_TRANSACTION_ID_MISSING";
    public static final String CUSTOMER_ID_MISSING = "PIPELINE_CUSTOMER_ID_MISSING";
    public static final String TIMESTAMP_MISSING = "PIPELINE_TIMESTAMP_MISSING";

    private final List<TransactionValidationRule> rules;

    public ValidateTransactionService(ValidationRuleProvider provider) {
        this.rules = List.copyOf(provider.rules());
    }

    @Override
    public ValidationResult validate(Transaction transaction, ValidationContext context) {
        List<Violation> violations = new ArrayList<>();
        if (isBlank(transaction.transactionId())) {
            violations.add(new Violation(TRANSACTION_ID_MISSING, "transactionId is required (deduplication key)"));
        }
        if (isBlank(transaction.customerId())) {
            violations.add(new Violation(CUSTOMER_ID_MISSING, "customerId is required (partition key)"));
        }
        if (transaction.timestamp() == null) {
            violations.add(new Violation(TIMESTAMP_MISSING, "timestamp is required (event time)"));
        }
        for (TransactionValidationRule rule : rules) {
            try {
                violations.addAll(rule.validate(transaction, context).violations());
            } catch (RuntimeException e) {
                violations.add(new Violation(RULE_ERROR, rule.name() + " failed: " + e));
            }
        }
        return ValidationResult.invalid(violations);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
