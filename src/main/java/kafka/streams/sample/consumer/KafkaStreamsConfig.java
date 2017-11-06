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

@EnableKafka
@Configuration
public class KafkaStreamsConfig {

  private Serde<String> stringSerde = Serdes.String();

  @Bean
  public Map<String, Object> kStreamsConfigs() {

    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "myKafkaStreamsApp");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
    props.put(StreamsConfig.STATE_DIR_CONFIG, "tmp/kafka-streams");
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        WallclockTimestampExtractor.class.getName());
    return props;
  }

  @Bean
  public KStreamBuilderFactoryBean kStreamBuilderFactoryBean() {
    Map<String, Object> props = kStreamsConfigs();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "myKafkaStreamsApp" + "_stream1");
    return new KStreamBuilderFactoryBean(props);
  }

  @Bean
  public KStreamBuilderFactoryBean anotherKStreamBuilderFactoryBean() {
    Map<String, Object> props = kStreamsConfigs();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "myKafkaStreamsApp" + "_stream2");
    return new KStreamBuilderFactoryBean(props);
  }
}
