package io.github.jlmc.fraud.it;

import io.github.jlmc.fraud.adapter.in.kafka.TransactionJsonParser;
import io.github.jlmc.fraud.application.model.IncomingMessage;
import io.github.jlmc.fraud.bootstrap.FraudJob;
import io.github.jlmc.fraud.bootstrap.JobConfig;
import io.github.jlmc.fraud.bootstrap.PipelineAssembler;
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
 * The whole dataflow with the REAL rule plugins (validation and fraud) found through ServiceLoader, on an in-JVM mini
 * cluster: no containers, so it runs with the unit tests. Proves that every rule JAR is discovered and that each
 * rule works on the context the pipeline builds, end to end.
 */
class FraudRulesPipelineTest {

    private static String json(String id, String customer, String time, String amount, String country, String status) {
        return """
                {"transactionId":"%s","customerId":"%s","merchantId":"m","amount":%s,"currency":"EUR","country":"%s",
                 "paymentStatus":%s,"timestamp":"2026-10-06T%sZ"}""".formatted(id, customer, amount, country,
                status == null ? "null" : "\"" + status + "\"", time);
    }

    private static String json(String id, String customer, String time, String amount, String country) {
        return json(id, customer, time, amount, country, null);
    }

    private static List<String> run(Map<String, String> settings, String... payloads) throws Exception {
        List<IncomingMessage> messages = new ArrayList<>();
        for (int i = 0; i < payloads.length; i++) {
            messages.add(TransactionJsonParser.parse(payloads[i].getBytes(StandardCharsets.UTF_8), "transaction.events", 0, i));
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<IncomingMessage> source = env.fromData(messages, TypeInformation.of(IncomingMessage.class));

        PipelineAssembler.Streams streams = PipelineAssembler.assemble(source, JobConfig.from(settings, Map.of()),
                FraudJob.defaultRules(), FraudJob.defaultRiskRules());

        DataStream<String> risk = streams.outcomes()
                .map(o -> "RISK " + o.transaction().transactionId() + " " + o.result().riskLevel() + " " + o.result().reasons())
                .returns(Types.STRING);
        DataStream<String> alerts = streams.alerts()
                .map(a -> "ALERT " + a.alertId() + " " + a.riskScore() + " " + a.reasons())
                .returns(Types.STRING);
        DataStream<String> invalid = streams.invalid()
                .map(e -> "INVALID " + e.code() + " " + e.transactionId())
                .returns(Types.STRING);

        List<String> lines = new ArrayList<>();
        try (CloseableIterator<String> it = risk.union(alerts, invalid).executeAndCollect("fraud-rules-test")) {
            it.forEachRemaining(lines::add);
        }
        return lines;
    }

    private static List<String> run(String... payloads) throws Exception {
        return run(Map.of(), payloads);
    }

    @Test
    void aNormalTransactionIsLow() throws Exception {
        assertThat(run(json("t1", "c1", "13:00:00", "25", "PT"))).containsExactly("RISK t1 LOW []");
    }

    @Test
    void sixTransactionsInAMinuteTriggerTheVelocityRule() throws Exception {
        var out = run(
                json("t6", "c1", "13:00:25", "1", "PT"), json("t1", "c1", "13:00:00", "1", "PT"),
                json("t3", "c1", "13:00:10", "1", "PT"), json("t2", "c1", "13:00:05", "1", "PT"),
                json("t5", "c1", "13:00:20", "1", "PT"), json("t4", "c1", "13:00:15", "1", "PT"));

        assertThat(out).contains("RISK t6 MEDIUM [HIGH_TRANSACTION_VELOCITY]");
    }

    @Test
    void aHighRiskBurstRaisesOneAlertWithReasonsInRuleNameOrder() throws Exception {
        var out = run(
                json("b1", "c1", "13:00:00", "1000", "PT"), json("b2", "c1", "13:00:05", "1000", "PT"),
                json("b3", "c1", "13:00:10", "1000", "PT"), json("b4", "c1", "13:00:15", "1000", "PT"),
                json("b5", "c1", "13:00:20", "1000", "PT"), json("b6", "c1", "13:00:25", "1000", "PT"));

        assertThat(out.stream().filter(l -> l.startsWith("ALERT")))
                .containsExactly("ALERT high-risk-v1-b6 80 [HIGH_SPENDING_VELOCITY, HIGH_TRANSACTION_VELOCITY]");
    }

    @Test
    void aLargeAmountAfterAShortHistoryIsUnusual() throws Exception {
        var out = run(json("a1", "c1", "13:00:00", "20", "PT"), json("a2", "c1", "13:10:00", "30", "PT"),
                json("a3", "c1", "13:20:00", "25", "PT"), json("a4", "c1", "13:30:00", "900", "PT"));

        assertThat(out).contains("RISK a4 LOW [UNUSUAL_AMOUNT]");
    }

    @Test
    void anotherCountryMinutesAfterThePreviousOneIsGeographicallyImpossible() throws Exception {
        var out = run(json("pt", "c1", "13:00:00", "5", "PT"), json("us", "c1", "13:03:00", "5", "US"));

        assertThat(out).contains("RISK pt LOW []", "RISK us MEDIUM [GEOGRAPHIC_IMPOSSIBILITY]");
    }

    @Test
    void outOfOrderArrivalStillFindsTheImpossibleTrip() throws Exception {
        var out = run(json("third", "c1", "13:02:00", "5", "PT"), json("first", "c1", "13:00:00", "5", "PT"),
                json("second", "c1", "13:01:00", "5", "US"));

        assertThat(out).contains("RISK second MEDIUM [GEOGRAPHIC_IMPOSSIBILITY]", "RISK third MEDIUM [GEOGRAPHIC_IMPOSSIBILITY]");
    }

    @Test
    void theTravelWindowIsConfigurable() throws Exception {
        var payloads = new String[] {json("pt", "c1", "13:00:00", "5", "PT"), json("us", "c1", "13:03:00", "5", "US")};

        assertThat(run(Map.of("risk.impossible-travel-minutes", "2"), payloads)).contains("RISK us LOW []");
    }

    @Test
    void aNewCountryWithAHighAmountIsFlaggedWhenTheTripIsPlausible() throws Exception {
        var out = run(
                json("n1", "c1", "13:00:00", "20", "PT"), json("n2", "c1", "13:01:00", "20", "PT"),
                json("n3", "c1", "13:02:00", "20", "PT"), json("n4", "c1", "13:20:00", "100", "US"));

        assertThat(out).contains("RISK n4 MEDIUM [NEW_COUNTRY_HIGH_AMOUNT]");
    }

    @Test
    void anApprovalAfterThreeDeclinedAttemptsIsFlagged() throws Exception {
        var out = run(
                json("d1", "c1", "13:00:00", "10", "PT", "DECLINED"), json("d2", "c1", "13:00:05", "10", "PT", "DECLINED"),
                json("d3", "c1", "13:00:10", "10", "PT", "DECLINED"), json("ok", "c1", "13:00:20", "10", "PT"));

        assertThat(out).contains("RISK d1 LOW []", "RISK d2 LOW []", "RISK d3 LOW []",
                "RISK ok MEDIUM [FAILED_ATTEMPTS_THEN_SUCCESS]");
    }

    @Test
    void declinedAttemptsCountAsAttemptsButSpendNothing() throws Exception {
        // five declined 9000 attempts and one approved 10: 6 attempts in a minute (velocity), but only 10 spent
        var out = run(
                json("x1", "c1", "13:00:00", "9000", "PT", "DECLINED"), json("x2", "c1", "13:00:05", "9000", "PT", "DECLINED"),
                json("x3", "c1", "13:00:10", "9000", "PT", "DECLINED"), json("x4", "c1", "13:00:15", "9000", "PT", "DECLINED"),
                json("x5", "c1", "13:00:20", "9000", "PT", "DECLINED"), json("ok", "c1", "13:00:25", "10", "PT"));

        assertThat(out).contains("RISK ok HIGH [FAILED_ATTEMPTS_THEN_SUCCESS, HIGH_TRANSACTION_VELOCITY]");   // 40 + 40
        assertThat(out.stream().filter(l -> l.contains("HIGH_SPENDING_VELOCITY"))).isEmpty();
    }

    @Test
    void anUnknownPaymentStatusIsRejectedAsMalformed() throws Exception {
        assertThat(run(json("x", "c1", "13:00:00", "10", "PT", "MAYBE"))).singleElement().asString().startsWith("INVALID MALFORMED_PAYLOAD");
    }
}
