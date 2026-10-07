package io.github.jlmc.fraud.it.support;

import io.github.jlmc.fraud.bootstrap.FraudJob;
import io.github.jlmc.fraud.bootstrap.JobConfig;
import io.github.jlmc.fraud.application.port.out.ValidationRuleProviderFactory;
import io.github.jlmc.fraud.it.support.KafkaFixture.Topics;
import io.github.jlmc.fraud.it.support.PostgresFixture.Database;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.awaitility.Awaitility;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Runs the real job ({@link FraudJob#build}) on an in-JVM Flink mini cluster against the Testcontainers Kafka and
 * PostgreSQL. Short watermark delay and idleness by default so tests are quick; individual tests override them.
 */
public final class JobHarness implements AutoCloseable {

    private final JobClient client;
    private java.util.function.Supplier<String> diagnostics = () -> "";

    private JobHarness(JobClient client) {
        this.client = client;
    }

    public static Map<String, String> defaults(Topics topics, Database db) {
        Map<String, String> args = new HashMap<>();
        args.put("kafka.bootstrap-servers", KafkaFixture.bootstrapServers());
        args.put("topic.transactions", topics.events());
        args.put("topic.risk", topics.risk());
        args.put("topic.invalid", topics.invalid());
        args.put("topic.alerts", topics.alerts());
        args.put("kafka.consumer-group", topics.consumerGroup());
        args.put("kafka.transactional-id-prefix", "it-" + topics.consumerGroup());
        args.put("kafka.alerts-transactional-id-prefix", "it-alerts-" + topics.consumerGroup());
        args.put("watermark.out-of-orderness-seconds", "1");
        args.put("watermark.idleness-seconds", "1");
        args.put("postgres.url", db.jdbcUrl());
        args.put("postgres.user", db.user());
        args.put("postgres.password", db.password());
        args.put("postgres.pool-size", "2");
        args.put("postgres.flush-interval-ms", "100");
        return args;
    }

    public static JobHarness start(Map<String, String> args) throws Exception {
        return start(args, FraudJob.defaultRules(), new Configuration());
    }

    public static JobHarness start(Map<String, String> args, ValidationRuleProviderFactory rules, Configuration extra) throws Exception {
        Configuration conf = new Configuration(extra);
        conf.setString("execution.checkpointing.interval", "500ms");       // the Kafka sink commits on checkpoints
        conf.setString("restart-strategy.type", "fixed-delay");
        conf.setString("restart-strategy.fixed-delay.attempts", "3");
        conf.setString("restart-strategy.fixed-delay.delay", "1s");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        FraudJob.build(env, JobConfig.from(args, Map.of()), rules);
        return new JobHarness(env.executeAsync("it-" + args.get("kafka.consumer-group")));
    }

    /** What to print when a wait times out: the state of the topics and tables the test is looking at. */
    public void diagnoseWith(java.util.function.Supplier<String> diagnostics) {
        this.diagnostics = diagnostics;
    }

    public JobStatus status() throws Exception {
        return client.getJobStatus().get();
    }

    /**
     * Waits for a condition, and fails fast with the job status when the job itself died, instead of timing out
     * after a minute with a message that says nothing.
     */
    public void await(String what, Callable<Boolean> condition) {
        try {
            Awaitility.await(what).atMost(Duration.ofSeconds(Long.getLong("it.await.seconds", 60))).pollInterval(Duration.ofMillis(300)).until(() -> {
                JobStatus status = status();
                if (status == JobStatus.FAILED || status == JobStatus.FINISHED || status == JobStatus.CANCELED) {
                    throw new AssertionError("the job is " + status + " while waiting for: " + what);
                }
                return condition.call();
            });
        } catch (org.awaitility.core.ConditionTimeoutException e) {
            String status;
            try {
                status = String.valueOf(status());
            } catch (Exception statusFailure) {
                status = "unknown (" + statusFailure + ")";
            }
            throw new AssertionError("timed out waiting for: " + what + "\njob status: " + status + "\n" + diagnostics.get(), e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            client.cancel().get();
        } catch (Exception ignored) {
            // already finished or failed: nothing to cancel
        }
    }

    /** Ids of the risk results seen so far in the given topic's output. */
    public static List<String> ids(List<Map<String, Object>> records) {
        return records.stream().map(r -> String.valueOf(r.get("transactionId"))).toList();
    }
}
