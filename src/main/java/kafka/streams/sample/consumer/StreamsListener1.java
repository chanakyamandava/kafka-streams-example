package kafka.streams.sample.consumer;

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

@Component
public class StreamsListener1 {
  private static final Logger log = LoggerFactory.getLogger(StreamsListener1.class);
  private KStream<String, String> kStream1;
  @Autowired
  @Qualifier("kStreamBuilderFactoryBean")
  private KStreamBuilderFactoryBean kStreamBuilderFactoryBean;
  private KStreamBuilder kStreamBuilder1;
  private KafkaStreams streams1;
  private Serde<String> stringSerde = Serdes.String();

  @PostConstruct
  public void runStream() {
    try {
      kStreamBuilderFactoryBean.setAutoStartup(false);
      kStreamBuilder1 = kStreamBuilderFactoryBean.getObject();
      kStream1 = kStreamBuilder1.stream(stringSerde, stringSerde, "one.test");
      kStream1.foreach((key, value) -> {
        log.info("The value is : {}", value);
      });
    } catch (Exception e) {
      log.error("Caught an exception in stream1: {}", e);
    }
  }

  @EventListener(ContextRefreshedEvent.class)
  public void injectStream() {
    log.debug("is kStreamBuilderFactoryBean running before starting streams? {}",
        kStreamBuilderFactoryBean.isRunning());

    if (!kStreamBuilderFactoryBean.isRunning()) {
      kStreamBuilderFactoryBean.start();
      log.debug("is kStreamBuilderFactoryBean running after starting kstreambuilder? {}",
          kStreamBuilderFactoryBean.isRunning());
    }
    streams1 = kStreamBuilderFactoryBean.getKafkaStreams();
    log.debug("the state of streams1 after start: {}", streams1.state());

  }

  @PreDestroy
  public void closeStream() {
    log.debug("the state of streams1 before close in PreDestroy: {}", streams1.state());

    if (streams1.state().isRunning()) {
      streams1.close();
      streams1.cleanUp();
    }
    log.debug("the state of streams1 after close in PreDestroy: {}", streams1.state());

  }
}
