package ma.oussama.kafka.handlers;

import ma.oussama.kafka.events.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Consumer, Supplier et traitement Kafka Streams (real time analytics).
 */
@Component
public class PageEventHandler {

    private static final Random RANDOM = new Random();

    /** Consumer : lit les PageEvent du topic T2. */
    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return event -> System.out.println("**** Evenement consomme : " + event);
    }

    /** Supplier : simule un capteur qui publie une visite toutes les 200 ms dans T3. */
    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> {
            String page = RANDOM.nextDouble() > 0.5 ? "P1" : "P2";
            String user = RANDOM.nextDouble() > 0.5 ? "U1" : "U2";
            long duration = 10 + RANDOM.nextInt(10000);
            return new PageEvent(page, user, new Date(), duration);
        };
    }

    /** Kafka Streams : compte les visites par page sur une fenetre glissante de 5 secondes. */
    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> kafkaStreamFunction() {
        return input -> input
                // Pour compter TOUTES les visites, laisser le filtre commente.
                // .filter((key, value) -> value.duration() > 100)
                .map((key, value) -> KeyValue.pair(value.name(), value.duration()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("count-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .map((windowedKey, count) -> KeyValue.pair(windowedKey.key(), count));
    }
}
