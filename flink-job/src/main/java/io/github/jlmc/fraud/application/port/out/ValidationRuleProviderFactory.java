package io.github.jlmc.fraud.application.port.out;

import java.io.Serializable;

/**
 * Serializable recipe for a {@link ValidationRuleProvider}. Flink serialises operators and ships them to the
 * TaskManagers, where the provider must be created (in {@code open()}) so that plugins are looked up with the
 * classloader that is live there. Never serialise the provider itself.
 */
@FunctionalInterface
public interface ValidationRuleProviderFactory extends Serializable {

    ValidationRuleProvider create();
}
