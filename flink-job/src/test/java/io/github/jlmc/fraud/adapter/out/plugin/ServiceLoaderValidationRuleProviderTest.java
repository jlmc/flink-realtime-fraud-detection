package io.github.jlmc.fraud.adapter.out.plugin;

import io.github.jlmc.fraud.testsupport.RejectNegativeAmountRule;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ServiceLoaderValidationRuleProviderTest {

    @Test
    void discoversRulesRegisteredInMetaInfServices() {
        var provider = new ServiceLoaderValidationRuleProvider(getClass().getClassLoader(), true);

        assertThat(provider.rules()).singleElement().isInstanceOf(RejectNegativeAmountRule.class);
    }

    @Test
    void failsFastWhenNoPluginIsVisibleAndOneIsRequired() {
        ClassLoader empty = new URLClassLoader(new URL[0], null); // sees neither the plugins nor the API

        assertThatThrownBy(() -> new ServiceLoaderValidationRuleProvider(empty, true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("usrlib");
    }

    @Test
    void toleratesNoPluginWhenNoneIsRequired() {
        ClassLoader empty = new URLClassLoader(new URL[0], null);

        assertThat(new ServiceLoaderValidationRuleProvider(empty, false).rules()).isEmpty();
    }
}
