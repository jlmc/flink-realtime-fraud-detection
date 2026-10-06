package io.github.jlmc.fraud.bootstrap;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobConfigTest {

    @Test
    void defaultsMatchTheLocalComposeStack() {
        JobConfig c = JobConfig.from(Map.of(), Map.of());

        assertThat(c.kafkaBootstrapServers()).isEqualTo("kafka:19092");
        assertThat(c.transactionsTopic()).isEqualTo("transaction.events");
        assertThat(c.watermarkOutOfOrderness()).isEqualTo(Duration.ofSeconds(30));
        assertThat(c.allowedLateness()).isZero();
        assertThat(c.thresholds().maxTransactionsPerMinute()).isEqualTo(5);
    }

    @Test
    void argumentsBeatEnvironmentWhichBeatsDefaults() {
        JobConfig c = JobConfig.from(
                Map.of("kafka.bootstrap-servers", "from-args:1", "risk.max-transactions-per-minute", "9"),
                Map.of("KAFKA_BOOTSTRAP_SERVERS", "from-env:2", "TOPIC_RISK", "risk-from-env"));

        assertThat(c.kafkaBootstrapServers()).isEqualTo("from-args:1");
        assertThat(c.riskTopic()).isEqualTo("risk-from-env");
        assertThat(c.thresholds().maxTransactionsPerMinute()).isEqualTo(9);
    }

    @Test
    void parsesKeyValueArguments() {
        assertThat(JobConfig.parseArgs(new String[] {"--a.b", "1", "--c", "2"})).containsEntry("a.b", "1").containsEntry("c", "2");
    }

    @Test
    void rejectsDanglingOrMalformedArguments() {
        assertThatThrownBy(() -> JobConfig.parseArgs(new String[] {"--a"})).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JobConfig.parseArgs(new String[] {"a", "1"})).isInstanceOf(IllegalArgumentException.class);
    }
}
