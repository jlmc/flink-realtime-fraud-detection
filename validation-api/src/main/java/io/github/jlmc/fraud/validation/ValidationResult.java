package io.github.jlmc.fraud.validation;

import java.util.List;

/**
 * Outcome of a validation rule. Immutable.
 */
public record ValidationResult(List<Violation> violations) {

    private static final ValidationResult VALID = new ValidationResult(List.of());

    public ValidationResult {
        violations = List.copyOf(violations);
    }

    public static ValidationResult valid() {
        return VALID;
    }

    public static ValidationResult invalid(String code, String message) {
        return new ValidationResult(List.of(new Violation(code, message)));
    }

    public static ValidationResult invalid(List<Violation> violations) {
        return violations.isEmpty() ? VALID : new ValidationResult(violations);
    }

    public boolean isValid() {
        return violations.isEmpty();
    }
}
