package kafka.streams.sample.consumer;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KStreamBuilderFactoryBean;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

@EnableKafka
@Configuration
public class KafkaStreamsConfig {

  private static final String appName = "myKafkaStreamsApp";
  private Serde<String> stringSerde = Serdes.String();

  @Bean
  public KStreamBuilderFactoryBean anotherKStreamBuilderFactoryBean() {
    Map<String, Object> props = kStreamsConfigs();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaStreamsConfig.appName + "_stream2");
    return new KStreamBuilderFactoryBean(props);
  }

  @Bean
  public KStreamBuilderFactoryBean avrokStreamBuilderFactoryBean() {
    Map<String, Object> props = kStreamsConfigs();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaStreamsConfig.appName + "_avroStream");
    return new KStreamBuilderFactoryBean(props);
  }

  @Bean
  public KStreamBuilderFactoryBean kStreamBuilderFactoryBean() {
    Map<String, Object> props = kStreamsConfigs();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaStreamsConfig.appName + "_stream1");
    return new KStreamBuilderFactoryBean(props);
  }

  @Bean
  public Map<String, Object> kStreamsConfigs() {

    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "myKafkaStreamsApp");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
    props.put(StreamsConfig.STATE_DIR_CONFIG, "tmp/kafka-streams");
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        WallclockTimestampExtractor.class.getName());
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost:8081");
    return props;
  }
}
