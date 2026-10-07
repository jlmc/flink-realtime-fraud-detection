package io.github.jlmc.fraud.application.port.out;

import java.io.Serializable;
import java.util.Map;

/**
 * Serializable recipe for a {@link RiskRuleProvider}, created on the TaskManager in {@code open()} for the same reason as
 * {@link ValidationRuleProviderFactory}: plugins must be looked up with the classloader that is live there. The settings
 * are the job's {@code risk.*} keys, passed to every rule.
 */
@FunctionalInterface
public interface RiskRuleProviderFactory extends Serializable {

    RiskRuleProvider create(Map<String, String> settings);
}
