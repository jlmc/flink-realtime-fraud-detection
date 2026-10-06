package io.github.jlmc.fraud.adapter.in.flink;

import io.github.jlmc.fraud.validation.Transaction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Drops transactions whose {@code transactionId} was already seen. Keyed by transactionId; the first occurrence wins.
 *
 * <p>The "seen" marker lives in managed keyed state with a TTL, so state is bounded. Flink's state TTL is measured in
 * PROCESSING time: a duplicate that arrives after the retention has elapsed (wall clock) is treated as new, and
 * PostgreSQL's primary key is the last line of defence.
 */
public class DeduplicationFunction extends KeyedProcessFunction<String, Transaction, Transaction> {

    private final Duration retention;

    private transient ValueState<Boolean> seen;
    private transient Counter duplicates;

    public DeduplicationFunction(Duration retention) {
        this.retention = retention;
    }

    @Override
    public void open(OpenContext openContext) {
        var descriptor = new ValueStateDescriptor<>("seen-transaction-id", Boolean.class);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(retention)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build());
        this.seen = getRuntimeContext().getState(descriptor);
        this.duplicates = getRuntimeContext().getMetricGroup().counter("duplicates_dropped");
    }

    @Override
    public void processElement(Transaction transaction, Context ctx, Collector<Transaction> out) throws Exception {
        if (seen.value() != null) {
            duplicates.inc();
            return;
        }
        seen.update(Boolean.TRUE);
        out.collect(transaction);
    }
}
