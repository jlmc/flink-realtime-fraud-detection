package io.github.jlmc.fraud.adapter.in.flink;

import io.github.jlmc.fraud.application.model.RiskOutcome;
import io.github.jlmc.fraud.domain.risk.RiskThresholds;
import io.github.jlmc.fraud.validation.Transaction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static io.github.jlmc.fraud.testsupport.Transactions.millis;
import static io.github.jlmc.fraud.testsupport.Transactions.tx;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the operator with a hand-controlled watermark: event-time behaviour (ordering, lateness, state cleanup,
 * recovery) is asserted deterministically, without sleeping.
 */
class RiskEvaluationFunctionTest {

    private KeyedOneInputStreamOperatorTestHarness<String, Transaction, RiskOutcome> harness;

    private static KeyedOneInputStreamOperatorTestHarness<String, Transaction, RiskOutcome> newHarness(Duration allowedLateness) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new RiskEvaluationFunction(RiskThresholds.defaults(), allowedLateness)),
                Transaction::customerId, Types.STRING);
    }

    private void start(Duration allowedLateness) throws Exception {
        harness = newHarness(allowedLateness);
        harness.open();
    }

    @AfterEach
    void close() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    private void send(Transaction t) throws Exception {
        harness.processElement(new StreamRecord<>(t, t.timestamp().toEpochMilli()));
    }

    private List<String> outputIds() {
        return harness.extractOutputValues().stream().map(o -> o.transaction().transactionId()).toList();
    }

    @Test
    void nothingIsEmittedUntilTheWatermarkReachesTheTransaction() throws Exception {
        start(Duration.ZERO);
        send(tx("a", "c1", 10, "5", "PT"));

        harness.processWatermark(millis(9));
        assertThat(outputIds()).isEmpty();

        harness.processWatermark(millis(10));
        assertThat(outputIds()).containsExactly("a");
    }

    @Test
    void orderedEventsAreEvaluatedInOrder() throws Exception {
        start(Duration.ZERO);
        send(tx("a", "c1", 0, "5", "PT"));
        send(tx("b", "c1", 10, "5", "PT"));
        send(tx("c", "c1", 20, "5", "PT"));

        harness.processWatermark(millis(30));

        assertThat(outputIds()).containsExactly("a", "b", "c");
    }

    @Test
    void outOfOrderEventsAreEvaluatedInEventTimeOrderNotArrivalOrder() throws Exception {
        start(Duration.ZERO);
        send(tx("c", "c1", 20, "5", "PT"));
        send(tx("a", "c1", 0, "5", "PT"));
        send(tx("b", "c1", 10, "5", "PT"));

        harness.processWatermark(millis(30));

        assertThat(outputIds()).containsExactly("a", "b", "c");
    }

    @Test
    void outOfOrderDoesNotChangeTheRiskResult() throws Exception {
        // A -> B -> A country pattern must be found even if the middle event arrives last
        start(Duration.ZERO);
        send(tx("first", "c1", 0, "5", "PT"));
        send(tx("third", "c1", 120, "5", "PT"));
        send(tx("second", "c1", 60, "5", "US"));

        harness.processWatermark(millis(200));

        assertThat(harness.extractOutputValues().get(2).result().reasons()).contains("SUSPICIOUS_COUNTRY_CHANGE");
    }

    @Test
    void sameTimestampIsOrderedByTransactionIdForDeterminism() throws Exception {
        start(Duration.ZERO);
        send(tx("b", "c1", 5, "5", "PT"));
        send(tx("a", "c1", 5, "5", "PT"));

        harness.processWatermark(millis(5));

        assertThat(outputIds()).containsExactly("a", "b");
    }

    @Test
    void eventsBehindTheWatermarkAreLateAndNotEvaluated() throws Exception {
        start(Duration.ZERO);
        harness.processWatermark(millis(100));

        send(tx("late", "c1", 50, "5", "PT"));
        send(tx("exactly-at-watermark", "c1", 100, "5", "PT"));
        harness.processWatermark(millis(200));

        assertThat(outputIds()).isEmpty();
        assertThat(harness.getSideOutput(PipelineTags.LATE)).extracting(r -> r.getValue().transactionId())
                .containsExactly("late", "exactly-at-watermark");
    }

    @Test
    void lateEventWithinAllowedLatenessIsStillEvaluatedImmediately() throws Exception {
        start(Duration.ofSeconds(60));
        harness.processWatermark(millis(100));

        send(tx("tolerated", "c1", 70, "5", "PT"));   // 30s behind, tolerated
        send(tx("too-late", "c1", 30, "5", "PT"));    // 70s behind, rejected

        assertThat(outputIds()).containsExactly("tolerated");
        assertThat(harness.getSideOutput(PipelineTags.LATE)).extracting(r -> r.getValue().transactionId()).containsExactly("too-late");
    }

    @Test
    void customersAreIndependent() throws Exception {
        start(Duration.ZERO);
        for (int i = 0; i < 5; i++) {
            send(tx("x" + i, "busy", i, "1", "PT"));
        }
        send(tx("y", "quiet", 5, "1", "PT"));
        send(tx("x5", "busy", 6, "1", "PT"));

        harness.processWatermark(millis(60));

        var byId = harness.extractOutputValues().stream().collect(java.util.stream.Collectors.toMap(o -> o.transaction().transactionId(), o -> o));
        assertThat(byId.get("x5").result().reasons()).contains("HIGH_TRANSACTION_VELOCITY");
        assertThat(byId.get("y").result().reasons()).isEmpty();
    }

    @Test
    void historyIsRestoredFromASnapshot() throws Exception {
        start(Duration.ZERO);
        for (int i = 0; i < 5; i++) {
            send(tx("before-" + i, "c1", i, "1", "PT"));
        }
        harness.processWatermark(millis(10));
        assertThat(harness.extractOutputValues()).hasSize(5);
        var snapshot = harness.snapshot(1L, 1L);
        harness.close();

        // a brand new operator instance, as after a TaskManager failure
        harness = newHarness(Duration.ZERO);
        harness.initializeState(snapshot);
        harness.open();
        harness.processWatermark(millis(10));
        send(tx("after", "c1", 20, "1", "PT"));
        harness.processWatermark(millis(30));

        assertThat(harness.extractOutputValues()).singleElement().satisfies(o -> {
            assertThat(o.transaction().transactionId()).isEqualTo("after");
            assertThat(o.result().reasons()).contains("HIGH_TRANSACTION_VELOCITY"); // 6th within a minute: needs restored state
        });
    }

    @Test
    void pendingTransactionsAreRestoredFromASnapshotAndFireAfterRecovery() throws Exception {
        start(Duration.ZERO);
        send(tx("pending", "c1", 50, "1", "PT"));
        var snapshot = harness.snapshot(1L, 1L);
        harness.close();

        harness = newHarness(Duration.ZERO);
        harness.initializeState(snapshot);
        harness.open();
        harness.processWatermark(millis(60));

        assertThat(outputIds()).containsExactly("pending");
    }

    @Test
    void stateIsEvictedOnceTheRetentionHasPassed() throws Exception {
        start(Duration.ZERO);
        send(tx("a", "c1", 0, "5", "PT"));
        harness.processWatermark(millis(1));
        assertThat(harness.numKeyedStateEntries()).isPositive();

        harness.processWatermark(millis(RiskThresholds.defaults().historyRetention().toSeconds() + 10));

        assertThat(harness.numKeyedStateEntries()).isZero();
        assertThat(harness.numEventTimeTimers()).isZero();
    }
}
