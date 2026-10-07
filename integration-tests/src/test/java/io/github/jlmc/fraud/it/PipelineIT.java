package io.github.jlmc.fraud.it;

import io.github.jlmc.fraud.it.support.JobHarness;
import io.github.jlmc.fraud.it.support.KafkaFixture;
import io.github.jlmc.fraud.it.support.KafkaFixture.Topics;
import io.github.jlmc.fraud.it.support.PostgresFixture;
import io.github.jlmc.fraud.it.support.PostgresFixture.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static io.github.jlmc.fraud.it.support.Messages.T0;
import static io.github.jlmc.fraud.it.support.Messages.tx;
import static io.github.jlmc.fraud.it.support.Messages.watermarkPushers;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole job (real Kafka source and sinks, real PostgreSQL, the three real validation plugins found by ServiceLoader)
 * running on an in-JVM mini cluster. Every test has its own topics, consumer group and database.
 */
class PipelineIT {

    private Topics topics;
    private Database db;
    private JobHarness job;

    @BeforeEach
    void setUp() {
        topics = KafkaFixture.newTopics();
        db = PostgresFixture.newDatabase();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (job != null) {
            job.close();
        }
    }

    private void startJob() throws Exception {
        startJob(Map.of());
    }

    private void startJob(Map<String, String> overrides) throws Exception {
        Map<String, String> args = JobHarness.defaults(topics, db);
        args.putAll(overrides);
        job = JobHarness.start(args);
        job.diagnoseWith(() -> "risk topic:    " + JobHarness.ids(KafkaFixture.readAllJson(topics.risk()))
                + "\nalerts topic:  " + JobHarness.ids(KafkaFixture.readAllJson(topics.alerts()))
                + "\ninvalid topic: " + KafkaFixture.readAllJson(topics.invalid()).size() + " records"
                + "\ninput topic:   " + KafkaFixture.readAll(topics.events()).size() + " records"
                + "\ndatabase:      transactions=" + db.count("transactions") + " risk_scores=" + db.count("risk_scores")
                + " fraud_alerts=" + db.count("fraud_alerts"));
    }

    /**
     * Pushes the watermark past everything sent before. It does not wait for anything: the pushers cannot be answered
     * themselves (the watermark trails the newest event), so each test waits for the result it is interested in.
     */
    private void pushWatermark(java.time.Instant at) {
        KafkaFixture.send(topics.events(), watermarkPushers(at).payloads());
    }

    private boolean riskTopicHas(String transactionId) {
        return JobHarness.ids(KafkaFixture.readAllJson(topics.risk())).contains(transactionId);
    }

    private long countWhere(String where) throws Exception {
        try (var c = db.connect(); var s = c.createStatement(); ResultSet rs = s.executeQuery("select count(*) from transactions where " + where)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void aValidTransactionFlowsFromKafkaToPostgresAndBackToKafka() throws Exception {
        startJob();

        KafkaFixture.send(topics.events(), tx("t-1", "customer-1", T0));
        pushWatermark(T0.plusSeconds(120));

        job.await("the row in PostgreSQL", () -> countWhere("transaction_id = 't-1' and status = 'PROCESSED'") == 1);
        job.await("the result in Kafka", () -> riskTopicHas("t-1"));
        var results = KafkaFixture.readAllJson(topics.risk()).stream().filter(r -> "t-1".equals(r.get("transactionId"))).toList();
        assertThat(results).singleElement().satisfies(r -> {
            assertThat(r.get("customerId")).isEqualTo("customer-1");
            assertThat(r.get("riskLevel")).isEqualTo("LOW");
            assertThat(r.get("riskScore")).isEqualTo(0);
            assertThat(r.get("timestamp")).isEqualTo(T0.toString());
        });
        assertThat(db.count("risk_scores")).as("score rows for t-1 and the pushers").isGreaterThanOrEqualTo(1);
    }

    @Test
    void invalidAndPoisonMessagesGoToTheInvalidTopicAndAreNeverPersisted() throws Exception {
        startJob();

        KafkaFixture.send(topics.events(),
                tx("neg", "c", "-5", "EUR", "PT", "m", T0),                                  // MinimumAmountRule
                tx("jpy", "c", "10", "JPY", "PT", "m", T0),                                  // SupportedCurrencyRule
                """
                {"transactionId": "fields", "amount": 10}""",                              // RequiredFieldsRule + pipeline invariants
                "this is not json {",                                                        // poison
                tx("ok", "c-ok", T0));                                                       // a good one in the middle must still pass
        pushWatermark(T0.plusSeconds(120));

        job.await("four invalid events", () -> KafkaFixture.readAllJson(topics.invalid()).size() == 4);
        var invalid = KafkaFixture.readAllJson(topics.invalid());
        assertThat(invalid).extracting(r -> r.get("code").toString()).anyMatch(c -> c.contains("AMOUNT_NOT_POSITIVE"))
                .anyMatch(c -> c.contains("CURRENCY_NOT_SUPPORTED")).anyMatch(c -> c.contains("REQUIRED_FIELD_MISSING"))
                .anyMatch(c -> c.equals("MALFORMED_PAYLOAD"));
        assertThat(countWhere("transaction_id in ('neg','jpy','fields')")).isZero();
        job.await("the good transaction", () -> countWhere("transaction_id = 'ok'") == 1);
        assertThat(job.status().isGloballyTerminalState()).as("poison must not stop the job").isFalse();
    }

    @Test
    void aDuplicateProducesExactlyOneRowAndOneResult() throws Exception {
        startJob();

        String same = tx("dup-1", "customer-d", T0);
        KafkaFixture.send(topics.events(), same, same, same);
        pushWatermark(T0.plusSeconds(120));

        job.await("the result in Kafka", () -> riskTopicHas("dup-1"));
        job.await("the row", () -> countWhere("transaction_id = 'dup-1'") == 1);
        assertThat(countWhere("transaction_id = 'dup-1'")).isEqualTo(1);
        assertThat(JobHarness.ids(KafkaFixture.readAllJson(topics.risk())).stream().filter("dup-1"::equals).count())
                .as("results in Kafka, not just rows in PostgreSQL: this is what proves deduplication happened in Flink").isEqualTo(1);
    }

    @Test
    void outOfOrderEventsAreEvaluatedInEventTimeOrder() throws Exception {
        startJob(Map.of("watermark.out-of-orderness-seconds", "30"));

        KafkaFixture.send(topics.events(),
                tx("ooo-c", "customer-o", T0.plusSeconds(20)),   // arrives first, happened last
                tx("ooo-a", "customer-o", T0),
                tx("ooo-b", "customer-o", T0.plusSeconds(10)));
        pushWatermark(T0.plusSeconds(300));

        job.await("three results", () -> JobHarness.ids(KafkaFixture.readAllJson(topics.risk())).containsAll(List.of("ooo-a", "ooo-b", "ooo-c")));
        assertThat(JobHarness.ids(KafkaFixture.readAllJson(topics.risk())).stream().filter(id -> id.startsWith("ooo-")).toList())
                .containsExactly("ooo-a", "ooo-b", "ooo-c");
    }

    @Test
    void theCountryPatternIsFoundEvenWhenTheMiddleEventArrivesLast() throws Exception {
        startJob(Map.of("watermark.out-of-orderness-seconds", "30"));

        KafkaFixture.send(topics.events(),
                tx("c-1", "customer-c", "40", "EUR", "PT", "m", T0),
                tx("c-3", "customer-c", "40", "EUR", "PT", "m", T0.plusSeconds(120)),
                tx("c-2", "customer-c", "40", "EUR", "US", "m", T0.plusSeconds(60)));
        pushWatermark(T0.plusSeconds(400));

        job.await("the alerting result", () -> KafkaFixture.readAllJson(topics.risk()).stream()
                .anyMatch(r -> "c-3".equals(r.get("transactionId")) && r.get("reasons").toString().contains("GEOGRAPHIC_IMPOSSIBILITY")));
    }

    @Test
    void aLateEventIsStoredAsLateAndNeverEvaluated() throws Exception {
        startJob();

        KafkaFixture.send(topics.events(), tx("on-time", "customer-l", T0));
        pushWatermark(T0.plusSeconds(600));          // the watermark is now far past T0
        job.await("the on-time result, which proves the watermark passed T0", () -> riskTopicHas("on-time"));

        KafkaFixture.send(topics.events(), tx("late-1", "customer-l", T0.minusSeconds(60)));

        job.await("the LATE row", () -> countWhere("transaction_id = 'late-1' and status = 'LATE'") == 1);
        try (var c = db.connect(); var s = c.createStatement(); ResultSet rs = s.executeQuery("select count(*) from risk_scores where transaction_id = 'late-1'")) {
            rs.next();
            assertThat(rs.getLong(1)).as("a late transaction is stored but not risk-evaluated").isZero();
        }
        assertThat(JobHarness.ids(KafkaFixture.readAllJson(topics.risk()))).doesNotContain("late-1");
    }

    @Test
    void aHighRiskBurstRaisesExactlyOneAlert() throws Exception {
        startJob();

        // six transactions of 1000 within 25 s: transaction velocity (40) + spending velocity (40) on the sixth = HIGH
        String[] burst = IntStream.rangeClosed(1, 6)
                .mapToObj(i -> tx("burst-" + i, "customer-b", "1000", "EUR", "PT", "m", T0.plusSeconds(5L * (i - 1))))
                .toArray(String[]::new);
        KafkaFixture.send(topics.events(), burst);
        pushWatermark(T0.plusSeconds(300));

        job.await("the alert", () -> db.count("fraud_alerts") == 1);
        try (var c = db.connect(); var s = c.createStatement(); ResultSet rs = s.executeQuery("select transaction_id, risk_score from fraud_alerts")) {
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("burst-6");
            assertThat(rs.getInt(2)).isEqualTo(80);
        }
    }

    private String[] burst(String prefix, String customer) {
        return IntStream.rangeClosed(1, 6)
                .mapToObj(i -> tx(prefix + i, customer, "1000", "EUR", "PT", "m", T0.plusSeconds(5L * (i - 1))))
                .toArray(String[]::new);
    }

    @Test
    void aNormalTransactionProducesAResultButNoAlert() throws Exception {
        startJob();

        KafkaFixture.send(topics.events(), tx("calm-1", "customer-calm", T0));
        pushWatermark(T0.plusSeconds(120));

        job.await("the risk result", () -> riskTopicHas("calm-1"));
        job.await("the PostgreSQL row", () -> countWhere("transaction_id = 'calm-1'") == 1);
        assertThat(KafkaFixture.readAllJson(topics.alerts())).as("no alert for a LOW result").isEmpty();
    }

    @Test
    void aHighRiskTransactionProducesExactlyOneAlertWithEveryReason() throws Exception {
        startJob();

        KafkaFixture.send(topics.events(), burst("hr-", "customer-hr"));
        pushWatermark(T0.plusSeconds(300));

        job.await("the alert", () -> !KafkaFixture.readAllJson(topics.alerts()).isEmpty());
        job.await("the sixth risk result", () -> riskTopicHas("hr-6"));
        var alerts = KafkaFixture.readAllJson(topics.alerts());
        assertThat(alerts).singleElement().satisfies(a -> {
            assertThat(a.get("alertId")).isEqualTo("high-risk-v1-hr-6");
            assertThat(a.get("transactionId")).isEqualTo("hr-6");
            assertThat(a.get("customerId")).isEqualTo("customer-hr");
            assertThat(a.get("riskScore")).isEqualTo(80);
            assertThat(a.get("riskLevel")).isEqualTo("HIGH");
            assertThat(a.get("reasons")).isEqualTo(List.of("HIGH_SPENDING_VELOCITY", "HIGH_TRANSACTION_VELOCITY"));
            assertThat(a.get("transactionTimestamp")).isEqualTo(T0.plusSeconds(25).toString());
        });
        assertThat(KafkaFixture.readAllJson(topics.risk()).stream().filter(r -> "hr-6".equals(r.get("transactionId"))))
                .as("the risk result is still published, the alert is additional").hasSize(1);
        job.await("the alert row in PostgreSQL", () -> db.count("fraud_alerts") == 1);
    }

    @Test
    void theAlertThresholdIsExactlyScoreSeventy() throws Exception {
        startJob();

        // three small transactions give the customer an average of 10; then a large one.
        // 6000 in ten minutes: spending velocity (40) + amount anomaly (30) = 70 = HIGH, the lowest score that alerts.
        // 4000 in ten minutes: amount anomaly (30) only = below the threshold.
        KafkaFixture.send(topics.events(),
                tx("at-1", "customer-at", "10", "EUR", "PT", "m", T0), tx("at-2", "customer-at", "10", "EUR", "PT", "m", T0.plusSeconds(1)),
                tx("at-3", "customer-at", "10", "EUR", "PT", "m", T0.plusSeconds(2)),
                tx("at-exact", "customer-at", "5970", "EUR", "PT", "m", T0.plusSeconds(3)),
                tx("bt-1", "customer-bt", "10", "EUR", "PT", "m", T0), tx("bt-2", "customer-bt", "10", "EUR", "PT", "m", T0.plusSeconds(1)),
                tx("bt-3", "customer-bt", "10", "EUR", "PT", "m", T0.plusSeconds(2)),
                tx("bt-below", "customer-bt", "4000", "EUR", "PT", "m", T0.plusSeconds(3)));
        pushWatermark(T0.plusSeconds(300));

        job.await("both results", () -> riskTopicHas("at-exact") && riskTopicHas("bt-below"));
        var scores = KafkaFixture.readAllJson(topics.risk()).stream()
                .collect(java.util.stream.Collectors.toMap(r -> r.get("transactionId").toString(), r -> (Integer) r.get("riskScore"), (a, b) -> a));
        assertThat(scores).containsEntry("at-exact", 70).containsEntry("bt-below", 30);
        job.await("the alert at exactly 70", () -> JobHarness.ids(KafkaFixture.readAllJson(topics.alerts())).contains("at-exact"));
        assertThat(JobHarness.ids(KafkaFixture.readAllJson(topics.alerts()))).containsExactly("at-exact");
    }

    @Test
    void aMediumRiskResultIsNotAnAlert() throws Exception {
        startJob();

        KafkaFixture.send(topics.events(),
                tx("m-1", "customer-m", "40", "EUR", "PT", "m", T0),
                tx("m-2", "customer-m", "40", "EUR", "US", "m", T0.plusSeconds(60)),
                tx("m-3", "customer-m", "40", "EUR", "PT", "m", T0.plusSeconds(120)));   // GEOGRAPHIC_IMPOSSIBILITY = 50
        pushWatermark(T0.plusSeconds(400));

        job.await("the medium result", () -> KafkaFixture.readAllJson(topics.risk()).stream()
                .anyMatch(r -> "m-3".equals(r.get("transactionId")) && "MEDIUM".equals(r.get("riskLevel"))));
        assertThat(KafkaFixture.readAllJson(topics.alerts())).isEmpty();
    }

    @Test
    void aDuplicatedHighRiskTransactionDoesNotDuplicateTheAlert() throws Exception {
        startJob();

        String[] once = burst("dd-", "customer-dd");
        KafkaFixture.send(topics.events(), once);
        KafkaFixture.send(topics.events(), once);       // Kafka redelivery / producer retry of the whole burst
        pushWatermark(T0.plusSeconds(300));

        job.await("the alert", () -> !KafkaFixture.readAllJson(topics.alerts()).isEmpty());
        job.await("the sixth risk result", () -> riskTopicHas("dd-6"));
        assertThat(JobHarness.ids(KafkaFixture.readAllJson(topics.alerts()))).containsExactly("dd-6");
    }

    @Test
    void invalidTransactionsNeverProduceAlerts() throws Exception {
        startJob();

        KafkaFixture.send(topics.events(), tx("neg", "c", "-5", "EUR", "PT", "m", T0), "this is not json {");
        pushWatermark(T0.plusSeconds(120));

        job.await("two invalid events", () -> KafkaFixture.readAllJson(topics.invalid()).size() == 2);
        assertThat(KafkaFixture.readAllJson(topics.alerts())).isEmpty();
    }

    @Test
    void aPostgresStallDoesNotLoseDataTheJobSimplyWaits() throws Exception {
        startJob();
        KafkaFixture.send(topics.events(), tx("warm-up", "customer-w", T0));
        pushWatermark(T0.plusSeconds(60));
        job.await("the warm-up row", () -> countWhere("transaction_id = 'warm-up'") == 1);

        PostgresFixture.pause();
        try {
            String[] during = IntStream.rangeClosed(1, 20)
                    .mapToObj(i -> tx("during-" + i, "customer-" + (i % 5), T0.plusSeconds(100 + i)))
                    .toArray(String[]::new);
            KafkaFixture.send(topics.events(), during);
            KafkaFixture.send(topics.events(), watermarkPushers(T0.plusSeconds(900)).payloads());
            Thread.sleep(Duration.ofSeconds(8).toMillis());       // long enough for batches to be stuck in the database
            assertThat(job.status().isGloballyTerminalState()).as("the job must wait, not die").isFalse();
        } finally {
            PostgresFixture.unpause();
        }

        job.await("all 20 rows after the stall", () -> countWhere("transaction_id like 'during-%'") == 20);
        assertThat(countWhere("transaction_id like 'during-%'")).isEqualTo(20);
        assertThat(JobHarness.ids(KafkaFixture.readAllJson(topics.risk())).stream().filter(id -> id.startsWith("during-")).count()).isEqualTo(20);
    }
}
