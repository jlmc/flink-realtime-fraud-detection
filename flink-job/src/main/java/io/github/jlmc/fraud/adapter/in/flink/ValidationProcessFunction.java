package io.github.jlmc.fraud.adapter.in.flink;

import io.github.jlmc.fraud.application.model.IncomingMessage;
import io.github.jlmc.fraud.application.model.InvalidEvent;
import io.github.jlmc.fraud.application.port.in.ValidateTransactionUseCase;
import io.github.jlmc.fraud.application.port.out.ValidationRuleProviderFactory;
import io.github.jlmc.fraud.application.usecase.ValidateTransactionService;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.ValidationContext;
import io.github.jlmc.fraud.validation.ValidationResult;
import io.github.jlmc.fraud.validation.Violation;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.stream.Collectors;

/**
 * Flink adapter around {@link ValidateTransactionUseCase}: valid transactions go downstream, everything else
 * goes to the {@link PipelineTags#INVALID} side output. The plugin lookup happens here, in {@code open()}, on the
 * TaskManager.
 */
public class ValidationProcessFunction extends ProcessFunction<IncomingMessage, Transaction> {

    private static final Logger LOG = LoggerFactory.getLogger(ValidationProcessFunction.class);

    private final ValidationRuleProviderFactory providerFactory;

    private transient ValidateTransactionUseCase useCase;
    private transient Counter accepted;
    private transient Counter rejected;
    private transient Counter malformed;

    public ValidationProcessFunction(ValidationRuleProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public void open(OpenContext openContext) {
        var provider = providerFactory.create();
        LOG.info("Validation rules loaded: {}", provider.rules().stream().map(r -> r.name()).toList());
        this.useCase = new ValidateTransactionService(provider);
        var metrics = getRuntimeContext().getMetricGroup();
        this.accepted = metrics.counter("transactions_valid");
        this.rejected = metrics.counter("transactions_rejected");
        this.malformed = metrics.counter("messages_malformed");
    }

    @Override
    public void processElement(IncomingMessage message, Context ctx, Collector<Transaction> out) {
        if (!message.isParsed()) {
            malformed.inc();
            ctx.output(PipelineTags.INVALID, new InvalidEvent(InvalidEvent.SOURCE_DESERIALIZATION,
                    "MALFORMED_PAYLOAD", message.error(), null, message.payload(),
                    message.topic(), message.partition(), message.offset(), Instant.now()));
            return;
        }
        Transaction transaction = message.transaction();
        ValidationResult result = useCase.validate(transaction, new ValidationContext(Instant.now()));
        if (result.isValid()) {
            accepted.inc();
            out.collect(transaction);
        } else {
            rejected.inc();
            ctx.output(PipelineTags.INVALID, new InvalidEvent(InvalidEvent.SOURCE_VALIDATION,
                    result.violations().stream().map(Violation::code).collect(Collectors.joining(",")),
                    result.violations().stream().map(Violation::message).collect(Collectors.joining("; ")),
                    transaction.transactionId(), message.payload(),
                    message.topic(), message.partition(), message.offset(), Instant.now()));
        }
    }
}
