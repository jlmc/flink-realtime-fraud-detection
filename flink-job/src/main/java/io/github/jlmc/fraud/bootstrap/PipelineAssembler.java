package io.github.jlmc.fraud.bootstrap;

import io.github.jlmc.fraud.adapter.in.flink.DeduplicationFunction;
import io.github.jlmc.fraud.adapter.in.flink.PipelineTags;
import io.github.jlmc.fraud.adapter.in.flink.RiskEvaluationFunction;
import io.github.jlmc.fraud.adapter.in.flink.ValidationProcessFunction;
import io.github.jlmc.fraud.application.model.IncomingMessage;
import io.github.jlmc.fraud.application.model.InvalidEvent;
import io.github.jlmc.fraud.application.model.RiskOutcome;
import io.github.jlmc.fraud.application.port.out.ValidationRuleProviderFactory;
import io.github.jlmc.fraud.validation.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Composition root of the dataflow: the only place that knows the order of the stages.
 *
 * <pre>
 * messages -> validate --invalid--> (invalid events)
 *                |
 *              keyBy(transactionId) -> dedup -> watermarks -> keyBy(customerId) -> risk --late--> (late transactions)
 *                                                                                    |
 *                                                                                  outcomes
 * </pre>
 */
public final class PipelineAssembler {

    /** The three streams a caller attaches sinks to. */
    public record Streams(
            DataStream<RiskOutcome> outcomes,
            DataStream<InvalidEvent> invalid,
            DataStream<Transaction> late) {
    }

    private PipelineAssembler() {
    }

    public static Streams assemble(DataStream<IncomingMessage> messages, JobConfig config, ValidationRuleProviderFactory rules) {
        SingleOutputStreamOperator<Transaction> valid = messages
                .process(new ValidationProcessFunction(rules))
                .name("validate")
                .uid("validate");

        SingleOutputStreamOperator<Transaction> unique = valid
                .keyBy(new TransactionIdKey())
                .process(new DeduplicationFunction(config.dedupRetention()))
                .name("deduplicate")
                .uid("deduplicate");

        WatermarkStrategy<Transaction> watermarks = WatermarkStrategy
                .<Transaction>forBoundedOutOfOrderness(config.watermarkOutOfOrderness())
                .withTimestampAssigner((transaction, previous) -> transaction.timestamp().toEpochMilli())
                .withIdleness(config.watermarkIdleness());

        SingleOutputStreamOperator<RiskOutcome> outcomes = unique
                .assignTimestampsAndWatermarks(watermarks)
                .name("event-time")
                .uid("event-time")
                .keyBy(new CustomerIdKey())
                .process(new RiskEvaluationFunction(config.thresholds(), config.allowedLateness()))
                .name("risk-evaluation")
                .uid("risk-evaluation");

        return new Streams(outcomes,
                valid.getSideOutput(PipelineTags.INVALID),
                outcomes.getSideOutput(PipelineTags.LATE));
    }

    public static class TransactionIdKey implements KeySelector<Transaction, String> {
        @Override
        public String getKey(Transaction transaction) {
            return transaction.transactionId();
        }
    }

    public static class CustomerIdKey implements KeySelector<Transaction, String> {
        @Override
        public String getKey(Transaction transaction) {
            return transaction.customerId();
        }
    }
}
