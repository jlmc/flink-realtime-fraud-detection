package io.github.jlmc.fraud.adapter.out.plugin;

import io.github.jlmc.fraud.validation.TransactionRiskRule;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ServiceLoaderRiskRuleProviderTest {

    @Test
    void discoversTheRegisteredRulesSortedByNameWhateverTheClasspathOrder() {
        var provider = new ServiceLoaderRiskRuleProvider(getClass().getClassLoader(), Map.of(), true);

        assertThat(provider.rules()).extracting(TransactionRiskRule::name)
                .containsExactly("stub-spending", "stub-travel", "stub-velocity");
    }

    @Test
    void everyRuleReceivesTheSettingsBeforeItIsUsed() {
        var provider = new ServiceLoaderRiskRuleProvider(getClass().getClassLoader(),
                Map.of("risk.max-transactions-per-minute", "1"), true);
        var velocity = provider.rules().stream().filter(r -> r.name().equals("stub-velocity")).findFirst().orElseThrow();

        var twoInAMinute = new io.github.jlmc.fraud.validation.RiskContext(2, java.math.BigDecimal.ONE, null, java.util.List.of(), null, 0);
        assertThat(velocity.evaluate(null, twoInAMinute).isTriggered()).isTrue();   // would not be with the default limit of 5
    }

    @Test
    void failsFastWhenNoPluginIsVisibleAndOneIsRequired() {
        ClassLoader empty = new URLClassLoader(new URL[0], null);

        assertThatThrownBy(() -> new ServiceLoaderRiskRuleProvider(empty, Map.of(), true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("fraud-rule");
    }

    @Test
    void toleratesNoPluginWhenNoneIsRequired() {
        ClassLoader empty = new URLClassLoader(new URL[0], null);

        assertThat(new ServiceLoaderRiskRuleProvider(empty, Map.of(), false).rules()).isEmpty();
    }
}
