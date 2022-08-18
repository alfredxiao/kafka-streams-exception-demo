/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package xiaoyf.kafka.streams.exception.handlers.deserialization;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static xiaoyf.kafka.streams.exception.helper.Constants.DEMO_DLQ_TOPIC;
import static xiaoyf.kafka.streams.exception.helper.Constants.DLQ_PRODUCER_ID;

/**
 * Deserialization handler that logs a deserialization exception and then
 * signals the processing pipeline to continue processing more records.
 */
public class LogAndContinueExceptionHandler implements DeserializationExceptionHandler {
    KafkaProducer<byte[], byte[]> dlqProducer;

    private static final Logger log = LoggerFactory.getLogger(LogAndContinueExceptionHandler.class);

    @Override
    public DeserializationHandlerResponse handle(final ProcessorContext context,
                                                 final ConsumerRecord<byte[], byte[]> record,
                                                 final Exception exception) {

        log.warn("Exception caught during Deserialization, " +
                 "taskId: {}, topic: {}, partition: {}, offset: {}",
                 context.taskId(), record.topic(), record.partition(), record.offset(),
                 exception);
        log.info("will forward to DLQ");

        try {
            dlqProducer.send(new ProducerRecord<>(DEMO_DLQ_TOPIC, record.key(), record.value())).get();
        } catch (Exception e) {
            log.error("error in forwarding message to DLQ", e);
        }

        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        log.info("LogAndContinueExceptionHandler sees configs:{}", configs);
        dlqProducer = (KafkaProducer<byte[], byte[]>) configs.get(DLQ_PRODUCER_ID);
    }
}
