package io.github.jlmc.fraud.it.support;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** JSON payloads as the producers would send them. Event times are fixed instants, never tied to the wall clock. */
public final class Messages {

    public static final Instant T0 = Instant.parse("2026-10-06T13:00:00Z");
    private static final AtomicInteger PUSH = new AtomicInteger();

    private Messages() {
    }

    public static String tx(String id, String customer, String amount, String currency, String country, String merchant, Instant at) {
        return """
                {
                  "transactionId": "%s",
                  "customerId": "%s",
                  "merchantId": "%s",
                  "amount": %s,
                  "currency": "%s",
                  "country": "%s",
                  "timestamp": "%s"
                }
                """.formatted(id, customer, merchant, amount, currency, country, at);
    }

    public static String tx(String id, String customer, Instant at) {
        return tx(id, customer, "25.00", "EUR", "PT", "merchant-10", at);
    }

    /** The payloads to send and the transaction ids the job will answer with. */
    public record Pushers(String[] payloads, List<String> ids) {
    }

    /**
     * Events far ahead in event time on throw-away customers. The watermark follows the newest event, so these push it
     * past everything sent before and make the job emit the results that were waiting for it.
     */
    public static Pushers watermarkPushers(Instant at) {
        String[] payloads = new String[6];
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < payloads.length; i++) {
            String id = "push-" + PUSH.incrementAndGet();
            ids.add(id);
            payloads[i] = tx(id, "pusher-" + i, at);
        }
        return new Pushers(payloads, ids);
    }
}
