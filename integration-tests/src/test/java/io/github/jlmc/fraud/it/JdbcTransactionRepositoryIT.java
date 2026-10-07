package io.github.jlmc.fraud.it;

import io.github.jlmc.fraud.adapter.out.persistence.JdbcTransactionRepository;
import io.github.jlmc.fraud.adapter.out.persistence.PoolConfig;
import io.github.jlmc.fraud.adapter.out.persistence.PooledDataSources;
import io.github.jlmc.fraud.application.model.PersistableEvent;
import io.github.jlmc.fraud.application.port.out.PersistenceException;
import io.github.jlmc.fraud.it.support.PostgresFixture;
import io.github.jlmc.fraud.it.support.PostgresFixture.Database;
import io.github.jlmc.fraud.validation.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static io.github.jlmc.fraud.it.support.Events.late;
import static io.github.jlmc.fraud.it.support.Events.processed;
import static io.github.jlmc.fraud.it.support.Events.tx;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The repository against a real PostgreSQL with the real migrations. Each test has its own database. */
class JdbcTransactionRepositoryIT {

    private Database db;
    private JdbcTransactionRepository repository;

    @BeforeEach
    void setUp() {
        db = PostgresFixture.newDatabase();
        repository = new JdbcTransactionRepository(PooledDataSources.acquire(
                new PoolConfig(db.jdbcUrl(), db.user(), db.password(), 2, 2000)));
    }

    @AfterEach
    void tearDown() {
        repository.close();
    }

    @Test
    void writesTheTransactionTheScoreAndTheAlertOfAHighRiskEvent() throws Exception {
        repository.saveAll(List.of(processed("t1", 80)));

        assertThat(db.count("transactions")).isEqualTo(1);
        assertThat(db.count("risk_scores")).isEqualTo(1);
        assertThat(db.count("fraud_alerts")).isEqualTo(1);
        try (var c = db.connect(); var s = c.createStatement();
             ResultSet rs = s.executeQuery("select t.amount, t.currency, t.status, t.event_time, r.risk_score, r.risk_level, r.reasons::text "
                     + "from transactions t join risk_scores r using (transaction_id)")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal(1)).isEqualByComparingTo("950.00");
            assertThat(rs.getString(2).trim()).isEqualTo("EUR");
            assertThat(rs.getString(3)).isEqualTo("PROCESSED");
            assertThat(rs.getObject(4, java.time.OffsetDateTime.class).toInstant().toString()).isEqualTo("2026-10-06T13:00:00Z");
            assertThat(rs.getInt(5)).isEqualTo(80);
            assertThat(rs.getString(6)).isEqualTo("HIGH");
            assertThat(rs.getString(7)).contains("HIGH_TRANSACTION_VELOCITY");
        }
    }

    @Test
    void onlyHighRiskRaisesAnAlert() {
        repository.saveAll(List.of(processed("low", 0), processed("medium", 50), processed("high", 90)));

        assertThat(db.count("risk_scores")).isEqualTo(3);
        assertThat(db.count("fraud_alerts")).isEqualTo(1);
    }

    @Test
    void aLateTransactionIsStoredWithoutScoreOrAlert() {
        repository.saveAll(List.of(late("late-1")));

        assertThat(db.count("transactions")).isEqualTo(1);
        assertThat(db.count("risk_scores")).isZero();
        assertThat(db.count("fraud_alerts")).isZero();
    }

    @Test
    void thePaymentStatusIsStoredAndDefaultsToApproved() throws Exception {
        Transaction declined = new Transaction("declined-1", "customer-42", "merchant-10", new java.math.BigDecimal("12.00"), "EUR", "PT",
                io.github.jlmc.fraud.it.support.Events.T0, io.github.jlmc.fraud.validation.PaymentStatus.DECLINED);

        repository.saveAll(List.of(new PersistableEvent(declined, late("x").status(), null, false), late("approved-1")));

        try (var c = db.connect(); var s = c.createStatement();
             ResultSet rs = s.executeQuery("select transaction_id, payment_status from transactions order by transaction_id")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("approved-1");
            assertThat(rs.getString(2)).isEqualTo("APPROVED");     // no status in the payload means approved
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(2)).isEqualTo("DECLINED");
        }
    }

    @Test
    void replayingTheSameEventsLeavesExactlyOneRowPerTable() {
        List<PersistableEvent> batch = List.of(processed("t1", 80), processed("t2", 10));

        repository.saveAll(batch);
        repository.saveAll(batch);   // a replay after a recovery
        repository.saveAll(List.of(processed("t1", 80)));

        assertThat(db.count("transactions")).isEqualTo(2);
        assertThat(db.count("risk_scores")).isEqualTo(2);
        assertThat(db.count("fraud_alerts")).isEqualTo(1);
    }

    @Test
    void theFirstWriteWinsOnAConflict() throws Exception {
        repository.saveAll(List.of(processed("t1", 10)));
        repository.saveAll(List.of(processed("t1", 90)));

        try (var c = db.connect(); var s = c.createStatement(); ResultSet rs = s.executeQuery("select risk_score from risk_scores")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(10);
        }
        assertThat(db.count("fraud_alerts")).as("the second write was ignored entirely").isZero();
    }

    @Test
    void anAlertAlwaysAgreesWithTheScoreOnRecordEvenWhenAReplayDisagrees() {
        repository.saveAll(List.of(processed("t1", 10)));     // first write: LOW, no alert
        repository.saveAll(List.of(processed("t1", 90)));     // a replay that now says HIGH must not create an alert

        assertThat(db.count("fraud_alerts")).isZero();
    }

    @Test
    void anAlertIsKeptWhenAReplayAgrees() {
        repository.saveAll(List.of(processed("t1", 90)));
        repository.saveAll(List.of(processed("t1", 90)));

        assertThat(db.count("fraud_alerts")).isEqualTo(1);
    }

    @Test
    void aRecordThatBreaksAnIntegrityConstraintIsRejectedNotSystemic() {
        Transaction noMerchant = new Transaction("bad", "c", null, BigDecimal.TEN, "EUR", "PT", tx("x").timestamp());
        PersistableEvent bad = new PersistableEvent(noMerchant, late("x").status(), null, false);

        assertThatThrownBy(() -> repository.saveAll(List.of(bad)))
                .isInstanceOfSatisfying(PersistenceException.class, e -> assertThat(e.isRecordRejected()).isTrue())
                .hasMessageContaining("merchant_id");   // SQLSTATE 23502, NOT NULL
    }

    @Test
    void aValueThatIsTooLongIsRejectedNotSystemic() {
        Transaction longCountry = new Transaction("bad", "c", "m", BigDecimal.TEN, "EUR", "PRT", tx("x").timestamp());   // varchar(2)

        assertThatThrownBy(() -> repository.saveAll(List.of(new PersistableEvent(longCountry, late("x").status(), null, false))))
                .isInstanceOfSatisfying(PersistenceException.class, e -> assertThat(e.isRecordRejected()).isTrue());
    }

    @Test
    void aBatchIsAtomicOneBadRecordRollsBackTheGoodOnes() {
        Transaction noMerchant = new Transaction("bad", "c", null, BigDecimal.TEN, "EUR", "PT", tx("x").timestamp());
        PersistableEvent bad = new PersistableEvent(noMerchant, late("x").status(), null, false);

        assertThatThrownBy(() -> repository.saveAll(List.of(processed("good-1", 10), bad, processed("good-2", 10))))
                .isInstanceOf(PersistenceException.class);

        assertThat(db.count("transactions")).as("nothing of the failed batch may remain").isZero();
        assertThat(db.count("risk_scores")).isZero();
    }

    @Test
    void aMissingTableIsASystemFailureNeverSkippedAndTheTransactionTableRollsBack() {
        db.execute("drop table risk_scores");   // a missing migration: must not look like a bad record

        assertThatThrownBy(() -> repository.saveAll(List.of(processed("t1", 10))))
                .isInstanceOfSatisfying(PersistenceException.class, e -> assertThat(e.isRecordRejected()).isFalse())
                .hasMessageContaining("risk_scores");

        assertThat(db.count("transactions")).as("the insert that succeeded before the failure was rolled back").isZero();
    }

    @Test
    void theSchemaMatchesWhatTheRepositoryWrites() throws SQLException {
        // the real migrations are applied by the fixture; this documents the constraints the adapter relies on
        try (var c = db.connect(); var s = c.createStatement();
             ResultSet rs = s.executeQuery("select count(*) from information_schema.table_constraints "
                     + "where constraint_type = 'PRIMARY KEY' and table_name in ('transactions','risk_scores','fraud_alerts')")) {
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(3);
        }
    }
}
