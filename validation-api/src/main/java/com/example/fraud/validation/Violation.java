package com.example.fraud.validation;

import java.util.Objects;

/**
 * A single reason why a transaction failed validation.
 *
 * @param code    stable, machine-readable code (e.g. {@code AMOUNT_NOT_POSITIVE})
 * @param message human-readable description
 */
public record Violation(String code, String message) {

    public Violation {
        Objects.requireNonNull(code, "code");
        Objects.requireNonNull(message, "message");
    }
}
