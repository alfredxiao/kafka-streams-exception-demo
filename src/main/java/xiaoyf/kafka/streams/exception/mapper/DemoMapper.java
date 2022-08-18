package xiaoyf.kafka.streams.exception.mapper;

import demo.model.DemoInput;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.streams.kstream.ValueMapper;

import static xiaoyf.kafka.streams.exception.helper.Constants.K;
import static xiaoyf.kafka.streams.exception.helper.Constants.M;

public class DemoMapper implements ValueMapper<DemoInput, String> {
    @Override
    public String apply(DemoInput tx) {
        switch (tx.getRequest()) {
            case LargeMessage:
                return RandomStringUtils.randomAlphanumeric(M + K);
            case RuntimeException:
                throw new RuntimeException("Mapper error occurred for input message id " + tx.getMessage());
            default:
                return tx.getMessage();
        }
    }
}
