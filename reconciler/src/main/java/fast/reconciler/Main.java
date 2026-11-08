package fast.reconciler;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** FAST reconciler: Kafka Streams app + /metrics and /healthz on METRICS_PORT. */
public final class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ClassLoaderMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);

        Topology topology = ReconcilerTopology.build(config, new ReconcilerMetrics(registry, config));
        LOG.info("topology:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, streamsProperties(config));
        new KafkaStreamsMetrics(streams).bindTo(registry);
        streams.setStateListener((newState, oldState) -> LOG.info("state {} -> {}", oldState, newState));
        streams.setUncaughtExceptionHandler(e -> {
            LOG.error("stream thread failed, replacing it", e);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        HttpServer http = HttpServer.create(new InetSocketAddress(config.metricsPort()), 0);
        http.createContext("/metrics", exchange -> respond(exchange, 200, registry.scrape(),
                "text/plain; version=0.0.4; charset=utf-8"));
        http.createContext("/healthz", exchange -> {
            boolean ok = streams.state().isRunningOrRebalancing();
            respond(exchange, ok ? 200 : 503, streams.state().name(), "text/plain");
        });
        http.start();

        CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("shutting down");
            streams.close(Duration.ofSeconds(30));
            http.stop(0);
            done.countDown();
        }));

        streams.start();
        LOG.info("fast-reconciler started; kafka={} application.id={} inputs={} metrics=:{}",
                config.brokers(), config.applicationId(), config.inputTopics(), config.metricsPort());
        done.await();
    }

    static Properties streamsProperties(Config config) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationId());
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokers());
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, config.processingGuarantee());
        p.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, config.standbyReplicas());
        p.put(StreamsConfig.STATE_DIR_CONFIG, config.stateDir());
        // Single-broker lab: internal topics (repartition, changelog) cannot have more than one replica.
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        p.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        p.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "lz4");
        return p;
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status, String body, String type)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", type);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
