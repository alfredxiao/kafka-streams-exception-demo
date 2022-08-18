package xiaoyf.kafka.streams.exception.handlers.uncaught;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

@Slf4j
public class ReplaceThreadStreamsUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {
    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        log.info("UNCAUGHT EXCEPTION, class: {}, message: {}", exception.getClass(), exception.getMessage());
        log.info("Will replace thread");
        return StreamThreadExceptionResponse.REPLACE_THREAD;
    }
}
