package xiaoyf.kafka.streams.exception.producer;

import demo.model.DemoInput;
import demo.model.RequestType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static xiaoyf.kafka.streams.exception.helper.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.kafka.streams.exception.helper.Constants.DEMO_INPUT_TOPIC;
import static xiaoyf.kafka.streams.exception.helper.Constants.SCHEMA_REGISTRY_URL;

public class DemoInputPublisher {

    static DemoInput NORMAL_MESSAGE = DemoInput.newBuilder()
            .setMessage("normal message")
            .setRequest(RequestType.NoException)
            .build();
    static DemoInput LARGE_MESSAGE = DemoInput.newBuilder()
            .setMessage("generate large message")
            .setRequest(RequestType.LargeMessage)
            .build();
    static DemoInput RUNTIME_EXCEPTION_MESSAGE = DemoInput.newBuilder()
            .setMessage("runtime error message")
            .setRequest(RequestType.RuntimeException)
            .build();

    public static void main(String[] args) throws Exception {
        send(RUNTIME_EXCEPTION_MESSAGE);
        //sentNonAvroMessage();
    }
    
    private static void send(DemoInput input) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put("auto.register.schemas", true);

        try (KafkaProducer<String, DemoInput> producer = new KafkaProducer<>(props)){
            String key = "key-" + System.currentTimeMillis();

            producer.send(new ProducerRecord<>(DEMO_INPUT_TOPIC, key, input)).get();
        }
    }
    
    private static void sentNonAvroMessage() throws Exception{
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);

            try (KafkaProducer<String, String> errorProducer = new KafkaProducer<>(props)) {
                errorProducer.send(new ProducerRecord<>(DEMO_INPUT_TOPIC, "rubbish", "rubbish")).get();
            }
    }
}
