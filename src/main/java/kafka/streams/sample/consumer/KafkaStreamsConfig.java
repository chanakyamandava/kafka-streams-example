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
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.core.KStreamBuilderFactoryBean;

@EnableKafka
@Configuration
public class KafkaStreamsConfig {

  private Serde<String> stringSerde = Serdes.String();

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public StreamsConfig kStreamsConfigs() {

    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "myKafkaStreamsApp");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
    props.put(StreamsConfig.STATE_DIR_CONFIG, "tmp/kafka-streams");
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        WallclockTimestampExtractor.class.getName());
    return new StreamsConfig(props);
  }

  @Bean
  public KStreamBuilderFactoryBean kStreamBuilderFactoryBean() {
    return new KStreamBuilderFactoryBean(kStreamsConfigs());
  }

  @Bean
  public KStreamBuilderFactoryBean anotherKStreamBuilderFactoryBean() {
    return new KStreamBuilderFactoryBean(kStreamsConfigs());
  }
}
