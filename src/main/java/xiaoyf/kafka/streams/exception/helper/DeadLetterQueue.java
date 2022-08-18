package xiaoyf.kafka.streams.exception.helper;

import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

import static xiaoyf.kafka.streams.exception.helper.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.kafka.streams.exception.helper.Constants.M;
import static xiaoyf.kafka.streams.exception.helper.Constants.SCHEMA_REGISTRY_URL;

@UtilityClass
public class DeadLetterQueue {

    // note this requires change on broker to allow larger message to be sent
    public static KafkaProducer<byte[], byte[]> createDeadLetterQueueProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, M * 3);

        return new KafkaProducer<>(props);
    }
}
