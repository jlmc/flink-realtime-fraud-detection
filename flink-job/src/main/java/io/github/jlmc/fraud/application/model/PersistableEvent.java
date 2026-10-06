package io.github.jlmc.fraud.application.model;

import io.github.jlmc.fraud.validation.RiskResult;
import io.github.jlmc.fraud.validation.Transaction;

/**
 * Everything the persistence side needs to record about one transaction, decided by the application layer so the
 * repository adapter contains no business rules.
 *
 * @param risk       risk evaluation, or {@code null} for a late transaction
 * @param raiseAlert whether a fraud alert row must be written
 */
public record PersistableEvent(Transaction transaction, TransactionStatus status, RiskResult risk, boolean raiseAlert) {
}
