package io.github.jlmc.fraud.adapter.in.flink;

import io.github.jlmc.fraud.adapter.in.kafka.TransactionJsonParser;
import io.github.jlmc.fraud.application.model.IncomingMessage;
import io.github.jlmc.fraud.application.model.InvalidEvent;
import io.github.jlmc.fraud.testsupport.RejectNegativeAmountRule;
import io.github.jlmc.fraud.validation.Transaction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ValidationProcessFunctionTest {

    private OneInputStreamOperatorTestHarness<IncomingMessage, Transaction> harness;

    @BeforeEach
    void open() throws Exception {
        harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(new ValidationProcessFunction(() -> () -> List.of(new RejectNegativeAmountRule()))));
        harness.open();
    }

    @AfterEach
    void close() throws Exception {
        harness.close();
    }

    private static IncomingMessage message(String json) {
        return TransactionJsonParser.parse(json.getBytes(StandardCharsets.UTF_8), "transaction.events", 0, 1);
    }

    private static final String VALID = """
            {
              "transactionId": "t1",
              "customerId": "c",
              "amount": 10,
              "timestamp": "2026-10-06T13:00:00Z"
            }
            """;

    @Test
    void validTransactionGoesDownstream() throws Exception {
        harness.processElement(new StreamRecord<>(message(VALID)));

        assertThat(harness.extractOutputValues()).extracting(Transaction::transactionId).containsExactly("t1");
        assertThat(harness.getSideOutput(PipelineTags.INVALID)).isNull();
    }

    @Test
    void pluginRejectionGoesToTheInvalidSideOutput() throws Exception {
        harness.processElement(new StreamRecord<>(message("""
                {
                  "transactionId": "t2",
                  "customerId": "c",
                  "amount": -5,
                  "timestamp": "2026-10-06T13:00:00Z"
                }
                """)));

        assertThat(harness.extractOutputValues()).isEmpty();
        InvalidEvent event = harness.getSideOutput(PipelineTags.INVALID).poll().getValue();
        assertThat(event.source()).isEqualTo(InvalidEvent.SOURCE_VALIDATION);
        assertThat(event.code()).contains("NEGATIVE_AMOUNT");
        assertThat(event.transactionId()).isEqualTo("t2");
    }

    @Test
    void pipelineInvariantsRejectATransactionWithoutCustomerEvenIfPluginsAcceptIt() throws Exception {
        harness.processElement(new StreamRecord<>(message("{\"transactionId\":\"t3\",\"amount\":1}")));

        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(harness.getSideOutput(PipelineTags.INVALID).poll().getValue().code())
                .contains("PIPELINE_CUSTOMER_ID_MISSING").contains("PIPELINE_TIMESTAMP_MISSING");
    }

    @Test
    void poisonMessageGoesToTheInvalidSideOutputWithItsCoordinates() throws Exception {
        harness.processElement(new StreamRecord<>(message("this is not json")));

        assertThat(harness.extractOutputValues()).isEmpty();
        InvalidEvent event = harness.getSideOutput(PipelineTags.INVALID).poll().getValue();
        assertThat(event.source()).isEqualTo(InvalidEvent.SOURCE_DESERIALIZATION);
        assertThat(event.payload()).isEqualTo("this is not json");
        assertThat(event.topic()).isEqualTo("transaction.events");
        assertThat(event.offset()).isEqualTo(1L);
    }
}
