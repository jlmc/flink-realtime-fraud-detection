package io.github.jlmc.fraud.testsupport;

import io.github.jlmc.fraud.validation.Transaction;

import java.math.BigDecimal;
import java.time.Instant;

public final class Transactions {

    public static final Instant T0 = Instant.parse("2026-10-06T13:00:00Z");

    private Transactions() {
    }

    public static Transaction tx(String id, String customer, long secondsAfterT0, String amount, String country) {
        return new Transaction(id, customer, "merchant-10", new BigDecimal(amount), "EUR", country, T0.plusSeconds(secondsAfterT0));
    }

    public static long millis(long secondsAfterT0) {
        return T0.plusSeconds(secondsAfterT0).toEpochMilli();
    }
}
