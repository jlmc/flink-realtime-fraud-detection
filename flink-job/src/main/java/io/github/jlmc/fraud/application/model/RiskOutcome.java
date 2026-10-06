package io.github.jlmc.fraud.application.model;

import io.github.jlmc.fraud.validation.RiskResult;
import io.github.jlmc.fraud.validation.Transaction;

/** A transaction together with its risk evaluation. */
public record RiskOutcome(Transaction transaction, RiskResult result) {
}
