package io.github.jlmc.fraud.bootstrap;

import io.github.jlmc.fraud.adapter.in.kafka.KafkaTransactionSource;
import io.github.jlmc.fraud.adapter.out.kafka.KafkaSinks;
import io.github.jlmc.fraud.adapter.out.plugin.ServiceLoaderValidationRuleProvider;
import io.github.jlmc.fraud.application.model.RiskOutcome;
import io.github.jlmc.fraud.application.port.out.ValidationRuleProviderFactory;
import io.github.jlmc.fraud.validation.RiskResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Entry point. Checkpointing, state backend and restart strategy come from the cluster configuration. */
public final class FraudJob {

    private FraudJob() {
    }

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.from(JobConfig.parseArgs(args), System.getenv());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Plugins are looked up on the TaskManager with the context classloader (docs/spikes/S1-plugin-classloading.md).
        ValidationRuleProviderFactory rules =
                () -> new ServiceLoaderValidationRuleProvider(Thread.currentThread().getContextClassLoader(), true);

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

        // streams.late() and the PostgreSQL sink are attached in the persistence milestone.

        env.execute("transaction-fraud-risk");
    }
}
