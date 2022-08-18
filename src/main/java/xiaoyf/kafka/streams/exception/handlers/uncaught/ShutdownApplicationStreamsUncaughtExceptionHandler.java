package xiaoyf.kafka.streams.exception.handlers.uncaught;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

@Slf4j
public class ShutdownApplicationStreamsUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {
    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        log.info("UNCAUGHT EXCEPTION, class: {}, message: {}", exception.getClass(), exception.getMessage());
        log.info("Will shutdown application-id");
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    }
}
