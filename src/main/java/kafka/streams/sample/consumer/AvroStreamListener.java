package kafka.streams.sample.consumer;

import java.util.Collections;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KStreamBuilderFactoryBean;
import org.springframework.stereotype.Component;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kafka.streams.sample.avro.Message;

@Component
public class AvroStreamListener {
  private static final Logger log = LoggerFactory.getLogger(AvroStreamListener.class);
  private KStream<String, Message> avroKStream;
  @Autowired
  @Qualifier("avrokStreamBuilderFactoryBean")
  private KStreamBuilderFactoryBean kStreamBuilderFactoryBean;
  private KStreamBuilder avroKStreamBuilder;
  private KafkaStreams avroStream;
  private Serde<String> stringSerde = Serdes.String();

  @PreDestroy
  public void closeStream() {
    log.debug("the state of avroStream before close in PreDestroy: {}", avroStream.state());

    if (avroStream.state().isRunning()) {
      avroStream.close();
      avroStream.cleanUp();
    }
    log.debug("the state of avroStream after close in PreDestroy: {}", avroStream.state());

  }

  @PostConstruct
  public void injectStream() {
    Serde<Message> messageSerde = new SpecificAvroSerde<>();
    boolean isKeySerde = false;
    messageSerde
        .configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "http://localhost:8081/"), isKeySerde);
    try {
      kStreamBuilderFactoryBean.setAutoStartup(false);
      avroKStreamBuilder = kStreamBuilderFactoryBean.getObject();
      avroKStream = avroKStreamBuilder.stream(stringSerde, messageSerde, "avro.topic");
      avroKStream.foreach((key, value) -> {
        log.info("The value is : {}", value);
      });
    } catch (Exception e) {
      log.error("Caught an exception in avroStream: {}", e);
    }
  }

  @EventListener(ContextRefreshedEvent.class)
  public void runStream() {
    log.debug("is kStreamBuilderFactoryBean running before starting streams? {}",
        kStreamBuilderFactoryBean.isRunning());

    if (!kStreamBuilderFactoryBean.isRunning()) {
      kStreamBuilderFactoryBean.start();
      log.debug("is kStreamBuilderFactoryBean running after starting kstreambuilder? {}",
          kStreamBuilderFactoryBean.isRunning());
    }
    avroStream = kStreamBuilderFactoryBean.getKafkaStreams();
    log.debug("the state of avroStream after start: {}", avroStream.state());

  }
}
