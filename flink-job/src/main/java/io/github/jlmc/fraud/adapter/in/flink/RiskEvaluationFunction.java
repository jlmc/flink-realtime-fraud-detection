package io.github.jlmc.fraud.adapter.in.flink;

import io.github.jlmc.fraud.application.model.HighRiskFraudAlert;
import io.github.jlmc.fraud.application.model.RiskOutcome;
import io.github.jlmc.fraud.application.port.in.EvaluateRiskUseCase;
import io.github.jlmc.fraud.application.usecase.EvaluateRiskService;
import io.github.jlmc.fraud.domain.history.CustomerHistory;
import io.github.jlmc.fraud.domain.history.HistoryEntry;
import io.github.jlmc.fraud.domain.risk.FraudAlertPolicy;
import io.github.jlmc.fraud.domain.risk.RiskRules;
import io.github.jlmc.fraud.domain.risk.RiskThresholds;
import io.github.jlmc.fraud.validation.Transaction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Event-time risk evaluation, keyed by customerId. This class only translates Flink concepts (keyed state, event-time
 * timers, watermark, side output) into calls to the domain/application layers; it contains no business rules.
 *
 * <p>How it stays correct with out-of-order input:
 * <ol>
 *   <li>An arriving transaction is NOT evaluated immediately. It is buffered by event timestamp and an event-time
 *       timer is registered for that timestamp.</li>
 *   <li>The timer fires when the watermark reaches the timestamp, i.e. when (up to the configured out-of-orderness)
 *       nothing older can still arrive. Timers fire in timestamp order, so each customer's transactions are
 *       evaluated in event-time order whatever the arrival order was. The cost is latency: results appear
 *       {@code out-of-orderness} after event time.</li>
 *   <li>A transaction older than {@code watermark - allowedLateness} is late: it is not evaluated, it goes to
 *       {@link PipelineTags#LATE}. Within the allowed lateness it is still evaluated, immediately, against the history
 *       that exists at that moment.</li>
 *   <li>A result that {@link FraudAlertPolicy} considers high risk is also emitted, once, to {@link PipelineTags#ALERTS}.</li>
 *   <li>History older than the retention is evicted by an event-time timer, so idle customers do not keep state.</li>
 * </ol>
 */
public class RiskEvaluationFunction extends KeyedProcessFunction<String, Transaction, RiskOutcome> {

    private final RiskThresholds thresholds;
    private final Duration allowedLateness;

    private transient EvaluateRiskUseCase useCase;
    private transient ListState<HistoryEntry> historyState;
    private transient MapState<Long, List<Transaction>> pending;
    private transient Counter evaluated;
    private transient Counter alerts;
    private transient Counter late;
    private transient Counter tolerated;

    public RiskEvaluationFunction(RiskThresholds thresholds, Duration allowedLateness) {
        this.thresholds = thresholds;
        this.allowedLateness = allowedLateness;
    }

    @Override
    public void open(OpenContext openContext) {
        this.useCase = new EvaluateRiskService(RiskRules.from(thresholds));
        this.historyState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("customer-history", TypeInformation.of(HistoryEntry.class)));
        this.pending = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("pending-by-timestamp", Types.LONG, TypeInformation.of(new TypeHint<List<Transaction>>() { })));
        var metrics = getRuntimeContext().getMetricGroup();
        this.evaluated = metrics.counter("risk_evaluated");
        this.alerts = metrics.counter("high_risk_alerts");
        this.late = metrics.counter("late_events");
        this.tolerated = metrics.counter("late_events_tolerated");
    }

    @Override
    public void processElement(Transaction transaction, Context ctx, Collector<RiskOutcome> out) throws Exception {
        long timestamp = transaction.timestamp().toEpochMilli();
        long watermark = ctx.timerService().currentWatermark();

        if (timestamp <= watermark) {
            if (timestamp <= watermark - allowedLateness.toMillis()) {
                late.inc();
                ctx.output(PipelineTags.LATE, transaction);
                return;
            }
            tolerated.inc();
            evaluate(transaction, out, ctx);
            return;
        }

        List<Transaction> bucket = pending.get(timestamp);
        if (bucket == null) {
            bucket = new ArrayList<>();
        }
        bucket.add(transaction);
        pending.put(timestamp, bucket);
        ctx.timerService().registerEventTimeTimer(timestamp);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RiskOutcome> out) throws Exception {
        List<Transaction> bucket = pending.get(timestamp);
        if (bucket != null) {
            pending.remove(timestamp);
            bucket.sort(Comparator.comparing(Transaction::transactionId)); // deterministic order for equal timestamps
            for (Transaction transaction : bucket) {
                evaluate(transaction, out, ctx);
            }
        }
        CustomerHistory remaining = loadHistory().evictOlderThan(timestamp, thresholds.historyRetention());
        if (remaining.isEmpty()) {
            historyState.clear();
        } else {
            historyState.update(remaining.entries());
        }
    }

    private void evaluate(Transaction transaction, Collector<RiskOutcome> out, Context ctx) throws Exception {
        CustomerHistory history = loadHistory();
        RiskOutcome outcome = new RiskOutcome(transaction, useCase.evaluate(transaction, history.contextFor(transaction)));
        out.collect(outcome);
        if (FraudAlertPolicy.shouldAlert(outcome.result())) {
            // same evaluation, second output: the alert is a projection of the result, not another engine
            ctx.output(PipelineTags.ALERTS, HighRiskFraudAlert.from(outcome.result()));
            alerts.inc();
        }
        evaluated.inc();

        CustomerHistory updated = history.append(
                new HistoryEntry(transaction.timestamp().toEpochMilli(), transaction.amount(), transaction.country()),
                thresholds.historyRetention(), thresholds.maxHistoryEntries());
        historyState.update(updated.entries());
        // eviction: fires once the watermark passes the retention horizon of this entry
        ctx.timerService().registerEventTimeTimer(transaction.timestamp().toEpochMilli() + thresholds.historyRetention().toMillis() + 1);
    }

    private CustomerHistory loadHistory() throws Exception {
        List<HistoryEntry> entries = new ArrayList<>();
        for (HistoryEntry e : historyState.get()) {
            entries.add(e);
        }
        return new CustomerHistory(entries);
    }
}
