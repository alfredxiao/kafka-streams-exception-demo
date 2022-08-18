package xiaoyf.kafka.streams.exception.helper;

public interface Constants {

    String DEMO_INPUT_TOPIC = "exception-demo-input";
    String DEMO_OUTPUT_TOPIC = "exception-demo-output";
    String DEMO_DLQ_TOPIC = "exception-demo-dlq";

    String BOOTSTRAP_SERVERS = "localhost:9092";
    String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    String DLQ_PRODUCER_ID = "__CUSTOM_DLQ_PRODUCER";
    int K = 1024;
    int M = 1024 * K;
}
