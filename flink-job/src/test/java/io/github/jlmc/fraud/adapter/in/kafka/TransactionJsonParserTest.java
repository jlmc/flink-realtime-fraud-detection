package io.github.jlmc.fraud.adapter.in.kafka;

import io.github.jlmc.fraud.application.model.IncomingMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionJsonParserTest {

    private static IncomingMessage parse(String json) {
        return TransactionJsonParser.parse(json == null ? null : json.getBytes(StandardCharsets.UTF_8), "transaction.events", 3, 42L);
    }

    @Test
    void parsesTheDocumentedPayload() {
        IncomingMessage m = parse("""
                {"transactionId":"tx-123","customerId":"customer-42","merchantId":"merchant-10",
                 "amount":950.00,"currency":"EUR","country":"PT","timestamp":"2026-10-06T13:10:01Z"}""");

        assertThat(m.isParsed()).isTrue();
        assertThat(m.error()).isNull();
        assertThat(m.transaction().transactionId()).isEqualTo("tx-123");
        assertThat(m.transaction().amount()).isEqualByComparingTo("950.00");
        assertThat(m.transaction().timestamp()).isEqualTo(Instant.parse("2026-10-06T13:10:01Z"));
        assertThat(m.topic()).isEqualTo("transaction.events");
        assertThat(m.partition()).isEqualTo(3);
        assertThat(m.offset()).isEqualTo(42L);
    }

    @Test
    void keepsDecimalPrecision() {
        assertThat(parse("{\"amount\":0.1}").transaction().amount().toPlainString()).isEqualTo("0.1");
    }

    @Test
    void ignoresUnknownFields() {
        assertThat(parse("{\"transactionId\":\"t\",\"somethingNew\":true}").isParsed()).isTrue();
    }

    @Test
    void missingFieldsAreNotAParseError() {
        IncomingMessage m = parse("{\"transactionId\":\"t\"}");

        assertThat(m.isParsed()).isTrue();
        assertThat(m.transaction().customerId()).isNull();
        assertThat(m.transaction().amount()).isNull();
        assertThat(m.transaction().timestamp()).isNull();
    }

    @Test
    void poisonPayloadsNeverThrow() {
        for (String poison : new String[] {"not json at all", "{\"amount\":", "[1,2,3]", "\"text\"", "{\"amount\":\"abc\"}",
                "{\"timestamp\":\"yesterday\"}", "", null}) {
            IncomingMessage m = parse(poison);

            assertThat(m.isParsed()).as("payload: %s", poison).isFalse();
            assertThat(m.error()).as("payload: %s", poison).startsWith(TransactionJsonParser.ERROR_PREFIX);
        }
    }

    @Test
    void errorMessageIsSingleLineAndPayloadIsKept() {
        IncomingMessage m = parse("{\"amount\":\"abc\"}");

        assertThat(m.error()).doesNotContain("\n");
        assertThat(m.payload()).isEqualTo("{\"amount\":\"abc\"}");
    }

    @Test
    void binaryGarbageIsHandled() {
        IncomingMessage m = TransactionJsonParser.parse(new byte[] {(byte) 0xff, (byte) 0xfe, 0x00, 0x01}, "t", 0, 0);

        assertThat(m.isParsed()).isFalse();
    }

    @Test
    void aMissingPaymentStatusMeansApproved() {
        var t = parse("{\"transactionId\":\"t\",\"amount\":1}").transaction();

        assertThat(t.paymentStatus()).isNull();
        assertThat(t.isDeclined()).isFalse();
    }

    @Test
    void readsADeclinedPaymentStatus() {
        var t = parse("{\"transactionId\":\"t\",\"paymentStatus\":\"DECLINED\"}").transaction();

        assertThat(t.paymentStatus()).isEqualTo(io.github.jlmc.fraud.validation.PaymentStatus.DECLINED);
        assertThat(t.isDeclined()).isTrue();
    }

    @Test
    void anUnknownPaymentStatusIsAMalformedPayload() {
        IncomingMessage m = parse("{\"transactionId\":\"t\",\"paymentStatus\":\"MAYBE\"}");

        assertThat(m.isParsed()).isFalse();
        assertThat(m.error()).startsWith(TransactionJsonParser.ERROR_PREFIX);
    }
}
