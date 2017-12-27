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

// @Component
public class StreamsListener2 {

  private static final Logger log = LoggerFactory.getLogger(StreamsListener2.class);
  private KStream<String, String> kStream2;
  @Autowired
  @Qualifier("anotherKStreamBuilderFactoryBean")
  private KStreamBuilderFactoryBean anotherKStreamBuilderFactoryBean;
  private KStreamBuilder kStreamBuilder2;
  private KafkaStreams streams2;
  private Serde<String> stringSerde = Serdes.String();

  @PreDestroy
  public void closeStream() {
    log.debug("the state of streams2 before close in PreDestroy: {}", streams2.state());

    if (streams2.state().isRunning()) {
      streams2.close();
      streams2.cleanUp();
    }
    log.debug("the state of streams2 after close in PreDestroy: {}", streams2.state());

  }

  @PostConstruct
  public void injectStream() {
    try {
      anotherKStreamBuilderFactoryBean.setAutoStartup(false);
      kStreamBuilder2 = anotherKStreamBuilderFactoryBean.getObject();
      kStream2 = kStreamBuilder2.stream(stringSerde, stringSerde, "two.test");
      kStream2.foreach((key, value) -> {
        log.info("The value is : {}", value);
      });
    } catch (Exception e) {
      log.error("Caught an exception in stream2: {}", e);
    }
  }

  @EventListener(ContextRefreshedEvent.class)
  public void runStream() {
    log.debug("is anotherKStreamBuilderFactoryBean running before starting streams? {}",
        anotherKStreamBuilderFactoryBean.isRunning());

    if (!anotherKStreamBuilderFactoryBean.isRunning()) {
      anotherKStreamBuilderFactoryBean.start();
      log.debug("is anotherKStreamBuilderFactoryBean running after starting kstreambuilder? {}",
          anotherKStreamBuilderFactoryBean.isRunning());

    }
    streams2 = anotherKStreamBuilderFactoryBean.getKafkaStreams();
    log.debug("the state of streams2 after start: {}", streams2.state());

  }
}
