package io.github.jlmc.fraud.adapter.out.persistence;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyTest {

    private final RetryPolicy policy = new RetryPolicy(Duration.ofSeconds(60), Duration.ofMillis(200), Duration.ofSeconds(5), 2.0);

    @Test
    void backoffDoublesAndIsCapped() {
        assertThat(policy.backoff(1)).isEqualTo(Duration.ofMillis(200));
        assertThat(policy.backoff(2)).isEqualTo(Duration.ofMillis(400));
        assertThat(policy.backoff(3)).isEqualTo(Duration.ofMillis(800));
        assertThat(policy.backoff(6)).isEqualTo(Duration.ofMillis(5000));
        assertThat(policy.backoff(50)).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void defaultBudgetStaysBelowTheCheckpointTimeoutOfTwoMinutes() {
        assertThat(RetryPolicy.defaults().maxElapsed()).isLessThan(Duration.ofMinutes(2));
    }
}
