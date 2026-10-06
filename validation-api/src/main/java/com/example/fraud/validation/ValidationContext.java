package com.example.fraud.validation;

import java.time.Instant;

/**
 * Domain-level context handed to stateless validation rules.
 *
 * @param evaluatedAt instant at which the validation is performed (injected for testability)
 */
public record ValidationContext(Instant evaluatedAt) {
}
