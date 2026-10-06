package io.github.jlmc.fraud.adapter.in.flink;

import io.github.jlmc.fraud.application.model.InvalidEvent;
import io.github.jlmc.fraud.validation.Transaction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

public final class PipelineTags {

    /** Messages that could not be parsed or failed validation. */
    public static final OutputTag<InvalidEvent> INVALID = new OutputTag<>("invalid", TypeInformation.of(InvalidEvent.class));

    /** Transactions that arrived behind the watermark (beyond the tolerated lateness). */
    public static final OutputTag<Transaction> LATE = new OutputTag<>("late", TypeInformation.of(Transaction.class));

    private PipelineTags() {
    }
}
