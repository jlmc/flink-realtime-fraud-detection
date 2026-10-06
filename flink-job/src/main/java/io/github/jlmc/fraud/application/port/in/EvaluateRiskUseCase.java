package io.github.jlmc.fraud.application.port.in;

import io.github.jlmc.fraud.validation.RiskContext;
import io.github.jlmc.fraud.validation.RiskResult;
import io.github.jlmc.fraud.validation.Transaction;

/** Inbound port: scores a transaction given the customer's recent behaviour. */
public interface EvaluateRiskUseCase {

    RiskResult evaluate(Transaction transaction, RiskContext context);
}
