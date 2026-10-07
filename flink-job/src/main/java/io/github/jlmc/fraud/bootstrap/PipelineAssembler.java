package io.github.jlmc.fraud.bootstrap;

import io.github.jlmc.fraud.adapter.in.flink.DeduplicationFunction;
import io.github.jlmc.fraud.adapter.in.flink.PipelineTags;
import io.github.jlmc.fraud.adapter.in.flink.RiskEvaluationFunction;
import io.github.jlmc.fraud.adapter.in.flink.ValidationProcessFunction;
import io.github.jlmc.fraud.application.model.HighRiskFraudAlert;
import io.github.jlmc.fraud.application.model.IncomingMessage;
import io.github.jlmc.fraud.application.model.InvalidEvent;
import io.github.jlmc.fraud.application.model.PersistableEvent;
import io.github.jlmc.fraud.application.model.RiskOutcome;
import io.github.jlmc.fraud.application.port.out.ValidationRuleProviderFactory;
import io.github.jlmc.fraud.application.usecase.PersistableEvents;
import io.github.jlmc.fraud.validation.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
 *                                                                                  outcomes ---+--- (late) ---> persistable --> PostgreSQL
 *                                                                                    |
 *                                                                                  alerts (HIGH only) ---> fraud.high-risk.alerts
 * </pre>
 */
public final class PipelineAssembler {

    /** The streams a caller attaches sinks to. */
    public record Streams(
            DataStream<RiskOutcome> outcomes,
            DataStream<InvalidEvent> invalid,
            DataStream<Transaction> late,
            DataStream<PersistableEvent> persistable,
            DataStream<HighRiskFraudAlert> alerts) {
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

        DataStream<Transaction> late = outcomes.getSideOutput(PipelineTags.LATE);

        DataStream<PersistableEvent> persistable = outcomes
                .map(new ProcessedToPersistable())
                .returns(TypeInformation.of(PersistableEvent.class))
                .name("to-persistable-processed")
                .uid("to-persistable-processed")
                .union(late.map(new LateToPersistable())
                        .returns(TypeInformation.of(PersistableEvent.class))
                        .name("to-persistable-late")
                        .uid("to-persistable-late"));

        DataStream<HighRiskFraudAlert> alerts = outcomes.getSideOutput(PipelineTags.ALERTS);

        return new Streams(outcomes, valid.getSideOutput(PipelineTags.INVALID), late, persistable, alerts);
    }

    public static class ProcessedToPersistable implements MapFunction<RiskOutcome, PersistableEvent> {
        @Override
        public PersistableEvent map(RiskOutcome outcome) {
            return PersistableEvents.processed(outcome);
        }
    }

    public static class LateToPersistable implements MapFunction<Transaction, PersistableEvent> {
        @Override
        public PersistableEvent map(Transaction transaction) {
            return PersistableEvents.late(transaction);
        }
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
