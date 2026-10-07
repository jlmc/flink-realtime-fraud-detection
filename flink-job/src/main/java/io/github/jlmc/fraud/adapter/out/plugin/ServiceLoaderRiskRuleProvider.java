package io.github.jlmc.fraud.adapter.out.plugin;

import io.github.jlmc.fraud.application.port.out.RiskRuleProvider;
import io.github.jlmc.fraud.validation.TransactionRiskRule;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Discovers the risk rules with {@link ServiceLoader} and configures each with the job's {@code risk.*} settings. Pass
 * the classloader that can see the plugin JARs (inside Flink: the thread context classloader, see
 * docs/spikes/S1-plugin-classloading.md).
 *
 * <p>Rules are sorted by name: the order of {@code reasons} in a result must not depend on the order of the JARs on the
 * classpath, or a replay could produce a different alert for the same transaction. Failing when nothing is found is
 * deliberate: a deployment that forgot the JARs must not score every transaction as harmless.
 */
public final class ServiceLoaderRiskRuleProvider implements RiskRuleProvider {

    private final List<TransactionRiskRule> rules;

    public ServiceLoaderRiskRuleProvider(ClassLoader classLoader, Map<String, String> settings, boolean requireAtLeastOne) {
        List<TransactionRiskRule> found = new ArrayList<>();
        ServiceLoader.load(TransactionRiskRule.class, classLoader).forEach(found::add);
        found.sort(Comparator.comparing(TransactionRiskRule::name));
        if (requireAtLeastOne && found.isEmpty()) {
            throw new IllegalStateException("No TransactionRiskRule found through ServiceLoader. "
                    + "Are the fraud-rule JARs deployed next to the job (usrlib/ or lib/)?");
        }
        found.forEach(rule -> rule.configure(settings));
        this.rules = List.copyOf(found);
    }

    @Override
    public List<TransactionRiskRule> rules() {
        return rules;
    }
}
