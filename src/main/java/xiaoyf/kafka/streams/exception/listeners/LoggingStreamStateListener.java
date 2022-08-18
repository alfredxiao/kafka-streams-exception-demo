package xiaoyf.kafka.streams.exception.listeners;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;

@Slf4j
public class LoggingStreamStateListener implements KafkaStreams.StateListener {
    private final KafkaStreams kafkaStreams;

    public LoggingStreamStateListener(final KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    @Override
    public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
        log.info("Streams state change, old: {}, new: {}", oldState, newState);
        log.info("meta for local threads: {}", kafkaStreams.metadataForLocalThreads().toString());
    }
}
