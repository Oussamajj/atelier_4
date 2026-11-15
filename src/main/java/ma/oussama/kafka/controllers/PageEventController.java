package ma.oussama.kafka.controllers;

import ma.oussama.kafka.events.PageEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * - /publish : publie un evenement dans un topic Kafka
 * - /analytics : pousse toutes les secondes les comptages calcules par Kafka Streams (SSE)
 */
@RestController
public class PageEventController {

    private static final String COUNT_STORE = "count-store";

    private final StreamBridge streamBridge;
    private final InteractiveQueryService interactiveQueryService;
    private final Random random = new Random();

    public PageEventController(StreamBridge streamBridge,
                               InteractiveQueryService interactiveQueryService) {
        this.streamBridge = streamBridge;
        this.interactiveQueryService = interactiveQueryService;
    }

    @GetMapping("/publish")
    public PageEvent publish(@RequestParam String name, @RequestParam String topic) {
        String user = random.nextDouble() > 0.5 ? "U1" : "U2";
        long duration = 10 + random.nextInt(10000);
        PageEvent event = new PageEvent(name, user, new Date(), duration);
        streamBridge.send(topic, event);
        return event;
    }

    @GetMapping(value = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics() {
        return Flux.interval(Duration.ofSeconds(1)).map(tick -> readCounts());
    }

    /** Lit le store materialise de Kafka Streams sur les 5 dernieres secondes. */
    private Map<String, Long> readCounts() {
        Map<String, Long> counts = new HashMap<>();
        try {
            ReadOnlyWindowStore<String, Long> store =
                    interactiveQueryService.getQueryableStore(
                            COUNT_STORE, QueryableStoreTypes.windowStore());
            if (store == null) {
                return counts;
            }
            Instant now = Instant.now();
            Instant from = now.minusSeconds(5);
            try (KeyValueIterator<Windowed<String>, Long> iterator = store.fetchAll(from, now)) {
                while (iterator.hasNext()) {
                    KeyValue<Windowed<String>, Long> next = iterator.next();
                    counts.merge(next.key.key(), next.value, Long::sum);
                }
            }
        } catch (Exception e) {
            // Le store n'est pas encore disponible au demarrage (Kafka Streams demarre en ~30 s).
        }
        return counts;
    }
}
