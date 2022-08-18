package xiaoyf.kafka.streams.exception.handlers.uncaught;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

@Slf4j
public class ShutdownClientStreamsUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {
    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        log.info("UNCAUGHT EXCEPTION, class: {}, message: {}", exception.getClass(), exception.getMessage());
        log.info("Will shutdown this stream client");
        return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    }
}
