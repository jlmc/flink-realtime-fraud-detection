package io.github.jlmc.fraud.adapter.out.persistence;

import io.github.jlmc.fraud.application.model.PersistableEvent;
import io.github.jlmc.fraud.application.model.TransactionStatus;
import io.github.jlmc.fraud.testsupport.InMemoryRepository;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.github.jlmc.fraud.testsupport.Transactions.tx;
import static org.assertj.core.api.Assertions.assertThat;

/** The Flink plumbing around the persister, on a real local mini cluster (same JVM, so a static repository is visible). */
class BatchingRepositorySinkTest {

    private static final InMemoryRepository REPOSITORY = new InMemoryRepository();

    @BeforeEach
    void reset() {
        REPOSITORY.stored.clear();
        REPOSITORY.batchSizes.clear();
        REPOSITORY.calls = 0;
    }

    private static List<PersistableEvent> events(int count) {
        List<PersistableEvent> events = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            events.add(new PersistableEvent(tx("t" + i, "c", i, "10", "PT"), TransactionStatus.PROCESSED, null, false));
        }
        return events;
    }

    @Test
    void everythingIsPersistedWhenTheInputEndsEvenIfTheLastBatchIsNotFull() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromData(events(7), TypeInformation.of(PersistableEvent.class))
                .sinkTo(new BatchingRepositorySink(() -> REPOSITORY, 3, 10_000, RetryPolicy.defaults()));

        env.execute("sink-test");

        assertThat(REPOSITORY.stored).hasSize(7);
        assertThat(REPOSITORY.batchSizes).containsExactly(3, 3, 1);
    }

    @Test
    void duplicatedInputStillLeavesOneRowPerTransaction() throws Exception {
        List<PersistableEvent> doubled = new ArrayList<>(events(5));
        doubled.addAll(events(5)); // the replay a recovery would cause
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromData(doubled, TypeInformation.of(PersistableEvent.class))
                .sinkTo(new BatchingRepositorySink(() -> REPOSITORY, 4, 10_000, RetryPolicy.defaults()));

        env.execute("sink-idempotency-test");

        assertThat(REPOSITORY.stored).hasSize(5);
    }
}
