package xiaoyf.kafka.streams.exception.handlers.production;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;

import static xiaoyf.kafka.streams.exception.helper.Constants.DEMO_DLQ_TOPIC;
import static xiaoyf.kafka.streams.exception.helper.Constants.DLQ_PRODUCER_ID;

@Slf4j
public class LogAndContinueProductionExceptionHandler implements ProductionExceptionHandler {
    KafkaProducer<byte[], byte[]> dlqProducer;

    public ProductionExceptionHandlerResponse handle(final ProducerRecord<byte[], byte[]> record,
                                                     final Exception exception) {

        log.error("production exception from key {}, forward to DLQ and continue processing", new String(record.key()), exception);
        try {
            dlqProducer.send(new ProducerRecord<>(DEMO_DLQ_TOPIC, record.key(), record.value())).get();
        } catch (Exception e) {
            log.error("error in forwarding message to DLQ", e);
        }
        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.info("LogAndContinueProductionExceptionHandler sees configs:{}", configs);
        dlqProducer = (KafkaProducer<byte[], byte[]>) configs.get(DLQ_PRODUCER_ID);
    }
}