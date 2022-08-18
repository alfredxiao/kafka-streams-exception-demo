/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package xiaoyf.kafka.streams.exception.application;

import demo.model.DemoInput;
import io.confluent.common.utils.TestUtils;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import xiaoyf.kafka.streams.exception.handlers.deserialization.LogAndContinueExceptionHandler;
import xiaoyf.kafka.streams.exception.handlers.production.LogAndContinueProductionExceptionHandler;
import xiaoyf.kafka.streams.exception.handlers.uncaught.ReplaceThreadStreamsUncaughtExceptionHandler;
import xiaoyf.kafka.streams.exception.listeners.LoggingStreamStateListener;
import xiaoyf.kafka.streams.exception.mapper.DemoMapper;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static xiaoyf.kafka.streams.exception.helper.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.kafka.streams.exception.helper.Constants.DEMO_OUTPUT_TOPIC;
import static xiaoyf.kafka.streams.exception.helper.Constants.DEMO_INPUT_TOPIC;
import static xiaoyf.kafka.streams.exception.helper.Constants.DLQ_PRODUCER_ID;
import static xiaoyf.kafka.streams.exception.helper.Constants.SCHEMA_REGISTRY_URL;
import static xiaoyf.kafka.streams.exception.helper.DeadLetterQueue.createDeadLetterQueueProducer;

@Slf4j
public class ExceptionDemoApplication {

  public static void main(final String[] args) {
    // Configure the Streams application.
    final Properties streamsConfiguration = getStreamsConfiguration();

    // Define the processing topology of the Streams application.
    final StreamsBuilder builder = new StreamsBuilder();
    createExceptionDemoStream(builder);

    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

    streams.setUncaughtExceptionHandler(new ReplaceThreadStreamsUncaughtExceptionHandler());
    streams.setStateListener(new LoggingStreamStateListener(streams));

//    streams.cleanUp();
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  static Properties getStreamsConfiguration() {
    final Properties props = new Properties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-exception-demo");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

    props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
    props.put("auto.register.schemas", true);

    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

    // Use a temporary directory for storing state, which will be automatically removed after the test.
    props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    props.put(DLQ_PRODUCER_ID, createDeadLetterQueueProducer());

    // set production exception handler
    props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueProductionExceptionHandler.class);
    props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

    // set deserialization exception

    return props;
  }

  static void createExceptionDemoStream(final StreamsBuilder builder) {
    final Serde<DemoInput> valueSpecificAvroSerde = new SpecificAvroSerde<>();
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", SCHEMA_REGISTRY_URL);
    valueSpecificAvroSerde.configure(serdeConfig, false);

    final KStream<String, DemoInput> transactions = builder.stream(DEMO_INPUT_TOPIC, Consumed.with(Serdes.String(), valueSpecificAvroSerde));

    transactions
            .peek((k, tx) -> {
              log.info("Processing input record key {}, value {}", k, tx);
            })
            .mapValues(new DemoMapper())
            .to(DEMO_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
  }
}
