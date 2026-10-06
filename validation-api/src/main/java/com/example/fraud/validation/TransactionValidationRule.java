package com.example.fraud.validation;

/**
 * Stateless validation rule. Implementations are discovered with {@link java.util.ServiceLoader}
 * and MUST have a public no-arg constructor and be free of Flink types.
 *
 * <p>Rules must be thread-safe and side-effect free.
 */
public interface TransactionValidationRule {

    /** Stable rule identifier used in logs and metrics. Defaults to the class simple name. */
    default String name() {
        return getClass().getSimpleName();
    }

    ValidationResult validate(Transaction transaction, ValidationContext context);
}
