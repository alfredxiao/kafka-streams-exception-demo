package xiaoyf.kafka.streams.exception.handlers.production;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;

import static xiaoyf.kafka.streams.exception.helper.Constants.DEMO_DLQ_TOPIC;
import static xiaoyf.kafka.streams.exception.helper.Constants.DLQ_PRODUCER_ID;

@Slf4j
public class IgnoreRecordTooLargeProductionExceptionHandler implements ProductionExceptionHandler {
    KafkaProducer<byte[], byte[]> dlqProducer;

    public ProductionExceptionHandlerResponse handle(final ProducerRecord<byte[], byte[]> record,
                                                     final Exception exception) {
        if (exception instanceof RecordTooLargeException) {
            log.warn("Message too large, ignore and continue");
            return ProductionExceptionHandlerResponse.CONTINUE;
        } else {
            log.error("Message too large, stop processing entirely");
            return ProductionExceptionHandlerResponse.FAIL;
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.info("IgnoreRecordTooLargeProductionExceptionHandler sees configs:{}", configs);
        dlqProducer = (KafkaProducer<byte[], byte[]>) configs.get(DLQ_PRODUCER_ID);
    }
}