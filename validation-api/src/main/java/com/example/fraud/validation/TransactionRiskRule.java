package com.example.fraud.validation;

/**
 * Stateful-risk rule contract. The stream processor owns state, timers and event time and hands
 * rules a {@link RiskContext}; rules return a partial {@link RiskContribution}.
 */
public interface TransactionRiskRule {

    String name();

    RiskContribution evaluate(Transaction transaction, RiskContext context);
}
