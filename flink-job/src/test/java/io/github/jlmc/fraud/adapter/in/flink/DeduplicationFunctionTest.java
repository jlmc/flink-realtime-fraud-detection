package io.github.jlmc.fraud.adapter.in.flink;

import io.github.jlmc.fraud.validation.Transaction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.github.jlmc.fraud.testsupport.Transactions.tx;
import static org.assertj.core.api.Assertions.assertThat;

class DeduplicationFunctionTest {

    private KeyedOneInputStreamOperatorTestHarness<String, Transaction, Transaction> harness;

    @BeforeEach
    void open() throws Exception {
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new DeduplicationFunction(Duration.ofHours(24))),
                Transaction::transactionId, Types.STRING);
        harness.open();
    }

    @AfterEach
    void close() throws Exception {
        harness.close();
    }

    @Test
    void theFirstOccurrenceWinsAndDuplicatesAreDropped() throws Exception {
        harness.processElement(new StreamRecord<>(tx("t1", "c", 0, "10", "PT")));
        harness.processElement(new StreamRecord<>(tx("t1", "c", 0, "10", "PT")));
        harness.processElement(new StreamRecord<>(tx("t2", "c", 1, "10", "PT")));
        harness.processElement(new StreamRecord<>(tx("t1", "c", 2, "999", "US"))); // same id, different content

        assertThat(harness.extractOutputValues()).extracting(Transaction::transactionId).containsExactly("t1", "t2");
        assertThat(harness.extractOutputValues().get(0).amount()).isEqualByComparingTo("10");
    }

    @Test
    void duplicateDetectionSurvivesASnapshotRestore() throws Exception {
        harness.processElement(new StreamRecord<>(tx("t1", "c", 0, "10", "PT")));
        var snapshot = harness.snapshot(1L, 1L);
        harness.close();

        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new DeduplicationFunction(Duration.ofHours(24))),
                Transaction::transactionId, Types.STRING);
        harness.initializeState(snapshot);
        harness.open();
        harness.processElement(new StreamRecord<>(tx("t1", "c", 0, "10", "PT")));
        harness.processElement(new StreamRecord<>(tx("t2", "c", 0, "10", "PT")));

        assertThat(harness.extractOutputValues()).extracting(Transaction::transactionId).containsExactly("t2");
    }
}
