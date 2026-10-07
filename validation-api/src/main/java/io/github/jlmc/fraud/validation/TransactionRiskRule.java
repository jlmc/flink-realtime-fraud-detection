package io.github.jlmc.fraud.validation;

import java.util.Map;

/**
 * Stateful-risk rule contract. The stream processor owns state, timers and event time and hands
 * rules a {@link RiskContext}; rules return a partial {@link RiskContribution}.
 *
 * <p>Implementations are plugins: a public no-argument constructor, registered in
 * {@code META-INF/services/io.github.jlmc.fraud.validation.TransactionRiskRule}. They must not hold per-customer state;
 * everything they need about the past is in the context.
 */
public interface TransactionRiskRule {

    String name();

    RiskContribution evaluate(Transaction transaction, RiskContext context);

    /**
     * Receives the job's {@code risk.*} settings once, right after the rule is instantiated and before any
     * {@link #evaluate}. A rule reads the keys it knows and keeps its own default for the rest.
     */
    default void configure(Map<String, String> settings) {
    }
}
