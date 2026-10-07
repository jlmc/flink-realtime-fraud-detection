package io.github.jlmc.fraud.bootstrap;

import io.github.jlmc.fraud.adapter.in.kafka.KafkaTransactionSource;
import io.github.jlmc.fraud.adapter.out.kafka.KafkaSinks;
import io.github.jlmc.fraud.adapter.out.persistence.BatchingRepositorySink;
import io.github.jlmc.fraud.adapter.out.persistence.JdbcTransactionRepository;
import io.github.jlmc.fraud.adapter.out.persistence.PoolConfig;
import io.github.jlmc.fraud.adapter.out.persistence.PooledDataSources;
import io.github.jlmc.fraud.adapter.out.persistence.RetryPolicy;
import io.github.jlmc.fraud.adapter.out.plugin.ServiceLoaderValidationRuleProvider;
import io.github.jlmc.fraud.application.model.RiskOutcome;
import io.github.jlmc.fraud.application.port.out.TransactionRepositoryFactory;
import io.github.jlmc.fraud.application.port.out.ValidationRuleProviderFactory;
import io.github.jlmc.fraud.validation.RiskResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/** Entry point. Checkpointing, state backend and restart strategy come from the cluster configuration. */
public final class FraudJob {

    private FraudJob() {
    }

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.from(JobConfig.parseArgs(args), System.getenv());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env, config, defaultRules());
        env.execute("transaction-fraud-risk");
    }

    /**
     * Plugins are looked up on the TaskManager with the context classloader (docs/spikes/S1-plugin-classloading.md).
     * Fails fast when none is found.
     */
    public static ValidationRuleProviderFactory defaultRules() {
        return () -> new ServiceLoaderValidationRuleProvider(Thread.currentThread().getContextClassLoader(), true);
    }

    /**
     * Defines the whole dataflow on {@code env}. {@code main} and the integration tests share this method, so the tests run
     * the job exactly as it is deployed, only with another environment, configuration and rule source.
     */
    public static void build(StreamExecutionEnvironment env, JobConfig config, ValidationRuleProviderFactory rules) {
        var source = env.fromSource(
                KafkaTransactionSource.create(config.kafkaBootstrapServers(), config.transactionsTopic(), config.consumerGroup()),
                WatermarkStrategy.noWatermarks(), "kafka-transactions")
                .uid("kafka-transactions");

        PipelineAssembler.Streams streams = PipelineAssembler.assemble(source, config, rules);

        streams.outcomes()
                .map(RiskOutcome::result)
                .returns(RiskResult.class)
                .sinkTo(KafkaSinks.riskResults(config.kafkaBootstrapServers(), config.riskTopic(),
                        config.transactionalIdPrefix(), config.kafkaTransactionTimeoutMs()))
                .name("kafka-risk-events").uid("kafka-risk-events");

        streams.invalid()
                .sinkTo(KafkaSinks.invalidEvents(config.kafkaBootstrapServers(), config.invalidTopic()))
                .name("kafka-invalid-events").uid("kafka-invalid-events");

        JobConfig.PostgresConfig pg = config.postgres();
        PoolConfig pool = new PoolConfig(pg.url(), pg.user(), pg.password(), pg.poolSize(), pg.connectionTimeoutMillis());
        // One pool per TaskManager, shared by all sink subtasks running there (see PooledDataSources).
        TransactionRepositoryFactory repositories = () -> new JdbcTransactionRepository(PooledDataSources.acquire(pool));
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(pg.maxRetrySeconds()), Duration.ofMillis(200), Duration.ofSeconds(5), 2.0);
        streams.persistable()
                .sinkTo(new BatchingRepositorySink(repositories, pg.batchSize(), pg.flushIntervalMillis(), retry))
                .name("postgres").uid("postgres");
    }
}
