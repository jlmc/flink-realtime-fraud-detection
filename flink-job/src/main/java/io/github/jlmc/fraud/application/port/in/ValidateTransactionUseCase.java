package io.github.jlmc.fraud.application.port.in;

import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.ValidationContext;
import io.github.jlmc.fraud.validation.ValidationResult;

/** Inbound port: runs every validation plugin against a transaction. */
public interface ValidateTransactionUseCase {

    ValidationResult validate(Transaction transaction, ValidationContext context);
}
