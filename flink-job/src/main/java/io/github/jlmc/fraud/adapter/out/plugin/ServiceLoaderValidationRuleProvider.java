package io.github.jlmc.fraud.adapter.out.plugin;

import io.github.jlmc.fraud.application.port.out.ValidationRuleProvider;
import io.github.jlmc.fraud.validation.TransactionValidationRule;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Discovers validation rules with {@link ServiceLoader}. Pass the classloader that can see the plugin JARs (inside
 * Flink: the thread context classloader, see docs/spikes/S1-plugin-classloading.md).
 *
 * <p>Failing when nothing is found is deliberate: a deployment that forgot the plugin JARs must not silently accept
 * every transaction.
 */
public final class ServiceLoaderValidationRuleProvider implements ValidationRuleProvider {

    private final List<TransactionValidationRule> rules;

    public ServiceLoaderValidationRuleProvider(ClassLoader classLoader, boolean requireAtLeastOne) {
        List<TransactionValidationRule> found = new ArrayList<>();
        ServiceLoader.load(TransactionValidationRule.class, classLoader).forEach(found::add);
        found.sort(Comparator.comparing(TransactionValidationRule::name));
        if (requireAtLeastOne && found.isEmpty()) {
            throw new IllegalStateException("No TransactionValidationRule found through ServiceLoader. "
                    + "Are the validation-rule JARs deployed next to the job (usrlib/ or lib/)?");
        }
        this.rules = List.copyOf(found);
    }

    @Override
    public List<TransactionValidationRule> rules() {
        return rules;
    }
}
