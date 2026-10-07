package io.github.jlmc.fraud.it;

import io.github.jlmc.fraud.application.port.out.ValidationRuleProviderFactory;
import io.github.jlmc.fraud.bootstrap.FraudJob;
import io.github.jlmc.fraud.it.support.JobHarness;
import io.github.jlmc.fraud.it.support.KafkaFixture;
import io.github.jlmc.fraud.it.support.KafkaFixture.Topics;
import io.github.jlmc.fraud.it.support.PostgresFixture;
import io.github.jlmc.fraud.it.support.PostgresFixture.Database;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionValidationRule;
import io.github.jlmc.fraud.validation.ValidationContext;
import io.github.jlmc.fraud.validation.ValidationResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static io.github.jlmc.fraud.it.support.Messages.T0;
import static io.github.jlmc.fraud.it.support.Messages.tx;
import static io.github.jlmc.fraud.it.support.Messages.watermarkPushers;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A task failure in the middle of a high-risk burst: the job restarts from its last checkpoint (source offsets, dedup and
 * risk state) and Kafka's transactional sinks abort what was not committed. There must still be exactly one alert.
 *
 * <p>The failure is a test-only plugin that throws an {@link Error} (the application layer turns every
 * {@link RuntimeException} of a plugin into a rejection, so only an Error reaches Flink) once, for one marker transaction.
 */
class AlertRecoveryIT {

    private static final AtomicBoolean ALREADY_CRASHED = new AtomicBoolean();

    /** Fails the task the first time it sees the marker transaction. */
    public static class CrashOnceRule implements TransactionValidationRule {
        @Override
        public ValidationResult validate(Transaction transaction, ValidationContext context) {
            if ("crash-marker".equals(transaction.transactionId()) && ALREADY_CRASHED.compareAndSet(false, true)) {
                throw new Error("simulated task failure");
            }
            return ValidationResult.valid();
        }
    }

    private JobHarness job;

    @AfterEach
    void tearDown() throws Exception {
        if (job != null) {
            job.close();
        }
    }

    @Test
    void aRestartInTheMiddleOfAHighRiskBurstStillProducesExactlyOneAlert() throws Exception {
        ALREADY_CRASHED.set(false);
        Topics topics = KafkaFixture.newTopics();
        Database db = PostgresFixture.newDatabase();
        ValidationRuleProviderFactory realRulesPlusCrash = () -> {
            var real = FraudJob.defaultRules().create();
            return () -> {
                List<TransactionValidationRule> all = new ArrayList<>(real.rules());
                all.add(new CrashOnceRule());
                return all;
            };
        };
        job = JobHarness.start(JobHarness.defaults(topics, db), realRulesPlusCrash, new Configuration());
        job.diagnoseWith(() -> "alerts: " + JobHarness.ids(KafkaFixture.readAllJson(topics.alerts()))
                + " risk: " + JobHarness.ids(KafkaFixture.readAllJson(topics.risk())) + " fraud_alerts rows: " + db.count("fraud_alerts"));

        String[] burst = IntStream.rangeClosed(1, 6)
                .mapToObj(i -> tx("rec-" + i, "customer-rec", "1000", "EUR", "PT", "m", T0.plusSeconds(5L * (i - 1))))
                .toArray(String[]::new);
        KafkaFixture.send(topics.events(), burst);
        KafkaFixture.send(topics.events(), tx("crash-marker", "customer-other", T0));   // fails the task once, mid-stream
        KafkaFixture.send(topics.events(), watermarkPushers(T0.plusSeconds(300)).payloads());

        job.await("the task failure to have happened", ALREADY_CRASHED::get);
        job.await("the alert after recovery", () -> !KafkaFixture.readAllJson(topics.alerts()).isEmpty());
        job.await("the job running again", () -> job.status() == JobStatus.RUNNING);
        job.await("the alert row", () -> db.count("fraud_alerts") == 1);
        Thread.sleep(Duration.ofSeconds(5).toMillis());     // room for a wrongly replayed duplicate to show up

        assertThat(JobHarness.ids(KafkaFixture.readAllJson(topics.alerts()))).containsExactly("rec-6");
        assertThat(JobHarness.ids(KafkaFixture.readAllJson(topics.risk())).stream().filter(id -> id.startsWith("rec-")))
                .as("one risk result per transaction, none duplicated by the replay").containsExactlyInAnyOrder(
                        "rec-1", "rec-2", "rec-3", "rec-4", "rec-5", "rec-6");
        assertThat(db.count("fraud_alerts")).isEqualTo(1);
    }
}
