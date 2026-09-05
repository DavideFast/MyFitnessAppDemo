package analisi_immediata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import analisi_immediata.analisi.RilevatoreImmobilita;
import analisi_immediata.config.Configurazione;

/** Rilevamento immobilita con Kafka Streams: lo stato sopravvive a riavvii e rebalance. */
public class ConsumerKafka {

    public static void main(String[] args) {

        Configurazione.stampaRiepilogo();
        final String bootstrapServers = Configurazione.getKafkaBootstrapServers();
        final String topicIngresso = Configurazione.getKafkaTopic();
        final String nomeStore = Configurazione.getNomeStore();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Configurazione.getKafkaApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, Configurazione.getStateDir());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, org.apache.kafka.streams.errors.LogAndContinueExceptionHandler.class.getName());
        // Le scritture su ClickHouse avvengono sullo stream thread: senza margine un DB lento causa un rebalance
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
                Configurazione.getMaxPollIntervalMs());
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG),
                Configurazione.getMaxPollRecords());

        // Creazione dello state store persistente
        StoreBuilder<KeyValueStore<String, String>> store = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(nomeStore), Serdes.String(), Serdes.String());

        //Creazione della pipeline di elaborazione dei messaggi
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(store);

        // Lettura dei messaggi dal topic di ingresso
        KStream<String, String> sorgente = builder.stream(topicIngresso, Consumed.with(Serdes.String(), Serdes.String()));

        // Processamento dei messaggi con il rilevatore di immobilità
        sorgente.process(RilevatoreImmobilita::new, nomeStore);
        boolean waiting = true;

        AtomicReference<KafkaStreams> istanzaCorrente = new AtomicReference<>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            KafkaStreams streams = istanzaCorrente.get();
            if (streams != null) {
                streams.close();
            }
        }));

        while (waiting) {
            try {

                // Avvio del flusso di elaborazione
                final KafkaStreams streams  = new KafkaStreams(builder.build(), props);
                istanzaCorrente.set(streams);
                CountDownLatch shutdownLatch = new CountDownLatch(1);
                AtomicBoolean topicMancante = new AtomicBoolean(false);

                //Avvio del flusso di elaborazione
                streams.setStateListener((newState, oldState)->{System.out.println(">>> STATO KAFKA STREAMS CAMBIATO DA "+oldState+ " a " +newState);});
                streams.setUncaughtExceptionHandler((Throwable exception) -> {
                    if (isTopicSorgenteMancante(exception)) {
                        topicMancante.set(true);
                        System.out.println(">>> Topic sorgente " + topicIngresso
                                + " non ancora disponibile durante il rebalance: nuovo tentativo tra 10 secondi.");
                    } else {
                        System.out.println(">>> ERRORE CRITICO FINALE IN KAFKA STREAMS:");
                        exception.printStackTrace();
                    }
                    shutdownLatch.countDown();
                    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
                });
                System.out.println("Attendo che Kafka Streams si inizializzi e crei lo state store...");
                Thread.sleep(5000); // Attendo 5 secondi per permettere a Kafka Streams di inizializzarsi

                System.out.println("Collegato al cluster Kafka in " + bootstrapServers + ", in ascolto sul topic " + topicIngresso);
                streams.start();
                System.out.println("Rilevatore di immobilità in esecuzione. Premere Ctrl+C per terminare.");

                shutdownLatch.await();
                streams.close();
                istanzaCorrente.set(null);

                if (topicMancante.get()) {
                    Thread.sleep(10000);
                    continue;
                }

                System.out.println("Rilevatore di immobilità terminato.");
                waiting = false;

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                waiting = false;
            } catch (Exception e) {
                System.err.println("Errore durante l'esecuzione del rilevatore di immobilità: " + e.getMessage());
                try {
                    Thread.sleep(5000); // Attendo 5 secondi prima di riprovare
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    waiting = false;
                }
            }
        }
    }

    /** Il topic puo non esistere ancora se il consumer parte prima del job di creazione dei topic. */
    private static boolean isTopicSorgenteMancante(Throwable errore) {
        for (Throwable causa = errore; causa != null; causa = causa.getCause()) {
            if (causa instanceof MissingSourceTopicException) {
                return true;
            }
            if (causa.getCause() == causa) {
                break;
            }
        }
        return false;
    }
}
