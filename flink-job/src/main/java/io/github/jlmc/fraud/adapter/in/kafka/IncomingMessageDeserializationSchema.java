package io.github.jlmc.fraud.adapter.in.kafka;

import io.github.jlmc.fraud.application.model.IncomingMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Never throws: see {@link TransactionJsonParser}. A throwing deserializer would restart the job in a loop. */
public class IncomingMessageDeserializationSchema implements KafkaRecordDeserializationSchema<IncomingMessage> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<IncomingMessage> out) {
        out.collect(TransactionJsonParser.parse(record.value(), record.topic(), record.partition(), record.offset()));
    }

    @Override
    public TypeInformation<IncomingMessage> getProducedType() {
        return TypeInformation.of(IncomingMessage.class);
    }
}
