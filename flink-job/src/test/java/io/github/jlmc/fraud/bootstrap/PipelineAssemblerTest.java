package io.github.jlmc.fraud.bootstrap;

import io.github.jlmc.fraud.adapter.in.kafka.TransactionJsonParser;
import io.github.jlmc.fraud.application.model.IncomingMessage;
import io.github.jlmc.fraud.testsupport.RejectNegativeAmountRule;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole dataflow (validate, deduplicate, event time, risk) on a real local Flink mini cluster with parallelism 2.
 * The bounded source ends with a final watermark, which fires every pending event-time timer.
 */
class PipelineAssemblerTest {

    private static String json(String id, String customer, String time, String amount, String country) {
        return """
                {
                  "transactionId": "%s",
                  "customerId": "%s",
                  "merchantId": "m",
                  "amount": %s,
                  "currency": "EUR",
                  "country": "%s",
                  "timestamp": "2026-10-06T%sZ"
                }
                """.formatted(id, customer, amount, country, time);
    }

    private static IncomingMessage message(String payload, long offset) {
        return TransactionJsonParser.parse(payload.getBytes(StandardCharsets.UTF_8), "transaction.events", 0, offset);
    }

    /** Runs the pipeline over the payloads and returns every output as a tagged line. */
    private static List<String> run(String... payloads) throws Exception {
        List<IncomingMessage> messages = new ArrayList<>();
        for (int i = 0; i < payloads.length; i++) {
            messages.add(message(payloads[i], i));
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<IncomingMessage> source = env.fromData(messages, TypeInformation.of(IncomingMessage.class));

        JobConfig config = JobConfig.from(Map.of(), Map.of());
        PipelineAssembler.Streams streams = PipelineAssembler.assemble(source, config,
                () -> () -> List.of(new RejectNegativeAmountRule()));

        DataStream<String> risk = streams.outcomes()
                .map(o -> "RISK " + o.transaction().transactionId() + " " + o.result().riskLevel() + " " + o.result().reasons())
                .returns(Types.STRING);
        DataStream<String> invalid = streams.invalid()
                .map(e -> "INVALID " + e.source() + " " + e.code() + " " + e.transactionId())
                .returns(Types.STRING);
        DataStream<String> late = streams.late()
                .map(t -> "LATE " + t.transactionId())
                .returns(Types.STRING);
        DataStream<String> persist = streams.persistable()
                .map(p -> "PERSIST " + p.transaction().transactionId() + " " + p.status() + " alert=" + p.raiseAlert())
                .returns(Types.STRING);

        DataStream<String> alerts = streams.alerts()
                .map(a -> "ALERT " + a.alertId() + " " + a.riskScore() + " " + a.reasons())
                .returns(Types.STRING);

        List<String> lines = new ArrayList<>();
        try (CloseableIterator<String> it = risk.union(invalid, late, persist, alerts).executeAndCollect("pipeline-test")) {
            it.forEachRemaining(lines::add);
        }
        return lines;
    }

    @Test
    void aNormalTransactionIsScoredLow() throws Exception {
        assertThat(run(json("t1", "c1", "13:00:00", "25.00", "PT")))
                .containsExactlyInAnyOrder("RISK t1 LOW []", "PERSIST t1 PROCESSED alert=false");
    }

    @Test
    void duplicatesProduceASingleResult() throws Exception {
        var out = run(json("t1", "c1", "13:00:00", "25", "PT"), json("t1", "c1", "13:00:00", "25", "PT"),
                json("t1", "c1", "13:00:00", "25", "PT"));

        assertThat(out).containsExactlyInAnyOrder("RISK t1 LOW []", "PERSIST t1 PROCESSED alert=false");
    }

    @Test
    void invalidAndPoisonMessagesAreRoutedAndDoNotStopTheJob() throws Exception {
        var out = run(
                "this is poison",
                json("bad", "c1", "13:00:00", "-5", "PT"),
                json("ok", "c1", "13:00:01", "10", "PT"),
                "{\"transactionId\":\"no-customer\"}");

        assertThat(out).contains("RISK ok LOW []");
        assertThat(out).anyMatch(l -> l.startsWith("INVALID DESERIALIZATION MALFORMED_PAYLOAD"));
        assertThat(out).anyMatch(l -> l.startsWith("INVALID VALIDATION NEGATIVE_AMOUNT bad"));
        assertThat(out).anyMatch(l -> l.startsWith("INVALID VALIDATION") && l.contains("PIPELINE_CUSTOMER_ID_MISSING"));
        assertThat(out).contains("PERSIST ok PROCESSED alert=false");
        assertThat(out).hasSize(5); // ok: RISK + PERSIST, plus 3 invalid
    }

    @Test
    void outOfOrderArrivalStillDetectsTheCountryPattern() throws Exception {
        var out = run(
                json("third", "c1", "13:02:00", "5", "PT"),
                json("first", "c1", "13:00:00", "5", "PT"),
                json("second", "c1", "13:01:00", "5", "US"));

        assertThat(out).contains("RISK third MEDIUM [SUSPICIOUS_COUNTRY_CHANGE]");
        assertThat(out.stream().filter(l -> l.startsWith("RISK"))).hasSize(3);
        assertThat(out.stream().filter(l -> l.startsWith("PERSIST"))).hasSize(3);
    }

    @Test
    void sixTransactionsInAMinuteTriggerVelocityForTheLastOne() throws Exception {
        var out = run(
                json("t6", "c1", "13:00:25", "1", "PT"), json("t1", "c1", "13:00:00", "1", "PT"),
                json("t3", "c1", "13:00:10", "1", "PT"), json("t2", "c1", "13:00:05", "1", "PT"),
                json("t5", "c1", "13:00:20", "1", "PT"), json("t4", "c1", "13:00:15", "1", "PT"));

        assertThat(out).contains("RISK t6 MEDIUM [HIGH_TRANSACTION_VELOCITY]");
        assertThat(out.stream().filter(l -> l.startsWith("RISK") && l.contains("HIGH_TRANSACTION_VELOCITY"))).hasSize(1);
    }

    @Test
    void aHighRiskBurstRaisesOneAlertWithEveryReasonAndNormalTransactionsRaiseNone() throws Exception {
        // 6 x 1000 EUR in 25 s: velocity (40) + spending (40) on the sixth = 80 = HIGH
        var out = run(
                json("b1", "c1", "13:00:00", "1000", "PT"), json("b2", "c1", "13:00:05", "1000", "PT"),
                json("b3", "c1", "13:00:10", "1000", "PT"), json("b4", "c1", "13:00:15", "1000", "PT"),
                json("b5", "c1", "13:00:20", "1000", "PT"), json("b6", "c1", "13:00:25", "1000", "PT"),
                json("calm", "c2", "13:00:00", "5", "PT"));

        assertThat(out.stream().filter(l -> l.startsWith("ALERT")))
                .containsExactly("ALERT high-risk-v1-b6 80 [HIGH_TRANSACTION_VELOCITY, HIGH_SPENDING_VELOCITY]");
        assertThat(out.stream().filter(l -> l.startsWith("RISK"))).as("risk results are unaffected").hasSize(7);
    }
}
