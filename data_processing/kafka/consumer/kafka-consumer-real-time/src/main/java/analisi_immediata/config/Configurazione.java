package analisi_immediata.config;

/** Unico punto di lettura delle variabili d'ambiente iniettate da Kubernetes. */
public final class Configurazione {

    private Configurazione() {
    }

    // ------------------------------------------------------------
    // LETTURA VARIABILI
    // ------------------------------------------------------------

    private static String leggiStringa(String nome, String predefinito) {
        String valore = System.getenv(nome);
        return (valore == null || valore.isBlank()) ? predefinito : valore;
    }

    private static int leggiIntero(String nome, int predefinito) {
        String valore = System.getenv(nome);
        if (valore == null || valore.isBlank()) {
            return predefinito;
        }
        try {
            int numero = Integer.parseInt(valore.trim());
            return numero > 0 ? numero : predefinito;
        } catch (NumberFormatException e) {
            System.err.printf("Variabile %s non numerica (%s): uso il valore predefinito %d%n", nome, valore, predefinito);
            return predefinito;
        }
    }

    private static double leggiDouble(String nome, double predefinito) {
        String valore = System.getenv(nome);
        if (valore == null || valore.isBlank()) {
            return predefinito;
        }
        try {
            double numero = Double.parseDouble(valore.trim());
            return numero > 0 ? numero : predefinito;
        } catch (NumberFormatException e) {
            System.err.printf("Variabile %s non numerica (%s): uso il valore predefinito %.2f%n", nome, valore, predefinito);
            return predefinito;
        }
    }

    // ------------------------------------------------------------
    // ELABORAZIONE
    // ------------------------------------------------------------

    /** Deve combaciare con SAMPLE_INTERVAL del simulatore: sample_id conta campioni, non secondi. */
    private static final double SECONDI_PER_CAMPIONE = leggiDouble("SAMPLE_INTERVAL", 5.0);
    private static final int MAX_CAMPIONI = leggiIntero("MAX_CAMPIONI_BATCH", 2000);
    private static final int SECONDI_FLUSH_BUFFER = leggiIntero("SECONDI_FLUSH_BUFFER", 30);
    private static final int MAX_CAMPIONI_SOSPESI = leggiIntero("MAX_CAMPIONI_SOSPESI", 20000);
    private static final int FINESTRA_VELOCITA = leggiIntero("FINESTRA_VELOCITA", 5);
    private static final double SOGLIA_MOVIMENTO_M = leggiDouble("SOGLIA_MOVIMENTO_M", 10.0);
    private static final int SECONDI_IMMOBILE = leggiIntero("SECONDI_IMMOBILE", 150);
    private static final double SOGLIA_BPM_ALLARME = leggiDouble("SOGLIA_BPM_ALLARME", 180.0);
    private static final String DESTINATARIO_ALLARME = leggiStringa("DESTINATARIO_ALLARME", "+39 333 1234567");

    // ------------------------------------------------------------
    // KAFKA
    // ------------------------------------------------------------

    private static final String KAFKA_BOOTSTRAP_SERVERS = leggiStringa("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String KAFKA_TOPIC = leggiStringa("KAFKA_TOPIC", "heart-rate-events");
    private static final String KAFKA_APPLICATION_ID = leggiStringa("KAFKA_APPLICATION_ID", "rilevatore-immobilita");
    private static final String NOME_STORE = leggiStringa("NOME_STATE_STORE", "stato-sessioni");
    /** Deve puntare al volume montato nel pod, altrimenti RocksDB scrive in /tmp. */
    private static final String STATE_DIR = leggiStringa("KAFKA_STATE_DIR", "/var/lib/kafka-streams");
    private static final int MAX_POLL_INTERVAL_MS = leggiIntero("MAX_POLL_INTERVAL_MS", 600000);
    private static final int MAX_POLL_RECORDS = leggiIntero("MAX_POLL_RECORDS", 500);

    // ------------------------------------------------------------
    // DATABASE
    // ------------------------------------------------------------

    private static final String CLICKHOUSE_URL = leggiStringa("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/default");
    private static final String CLICKHOUSE_USER = leggiStringa("CLICKHOUSE_USER", "default");
    private static final String CLICKHOUSE_PASSWORD = leggiStringa("CLICKHOUSE_PASSWORD", "");
    private static final String POSTGRES_URL = leggiStringa("POSTGRES_URL", "jdbc:postgresql://localhost:5432/bigintensive");
    private static final String POSTGRES_USER = leggiStringa("POSTGRES_USER", "postgres");
    private static final String POSTGRES_PASSWORD = leggiStringa("POSTGRES_PASSWORD", "postgres");

    // ------------------------------------------------------------
    // GETTER PUBBLICI
    // ------------------------------------------------------------

    public static double getSecondiPerCampione() {
        return SECONDI_PER_CAMPIONE;
    }

    public static int getMaxCampioni() {
        return MAX_CAMPIONI;
    }

    /** Ogni quanti secondi svuotare il buffer anche se non ha raggiunto MAX_CAMPIONI. */
    public static int getSecondiFlushBuffer() {
        return SECONDI_FLUSH_BUFFER;
    }

    /** Tetto ai campioni trattenuti in memoria quando il database non risponde. */
    public static int getMaxCampioniSospesi() {
        return MAX_CAMPIONI_SOSPESI;
    }

    public static int getFinestraVelocita() {
        return FINESTRA_VELOCITA;
    }

    public static double getSogliaMovimentoM() {
        return SOGLIA_MOVIMENTO_M;
    }

    public static int getSecondiImmobile() {
        return SECONDI_IMMOBILE;
    }

    /** Soglia di immobilita convertita nel numero di campioni corrispondente. */
    public static int getCampioniImmobile() {
        return Math.max(1, (int) Math.round(SECONDI_IMMOBILE / SECONDI_PER_CAMPIONE));
    }

    public static double getSogliaBpmAllarme() {
        return SOGLIA_BPM_ALLARME;
    }

    public static String getDestinatarioAllarme() {
        return DESTINATARIO_ALLARME;
    }

    public static String getKafkaBootstrapServers() {
        return KAFKA_BOOTSTRAP_SERVERS;
    }

    public static String getKafkaTopic() {
        return KAFKA_TOPIC;
    }

    public static String getKafkaApplicationId() {
        return KAFKA_APPLICATION_ID;
    }

    public static String getNomeStore() {
        return NOME_STORE;
    }

    public static String getStateDir() {
        return STATE_DIR;
    }

    public static int getMaxPollIntervalMs() {
        return MAX_POLL_INTERVAL_MS;
    }

    public static int getMaxPollRecords() {
        return MAX_POLL_RECORDS;
    }

    public static String getClickhouseUrl() {
        return CLICKHOUSE_URL;
    }

    public static String getClickhouseUser() {
        return CLICKHOUSE_USER;
    }

    public static String getClickhousePassword() {
        return CLICKHOUSE_PASSWORD;
    }

    public static String getPostgresUrl() {
        return POSTGRES_URL;
    }

    public static String getPostgresUser() {
        return POSTGRES_USER;
    }

    public static String getPostgresPassword() {
        return POSTGRES_PASSWORD;
    }

    public static void stampaRiepilogo() {
        System.out.println("=========== CONFIGURAZIONE ===========");
        System.out.println("Kafka        : " + KAFKA_BOOTSTRAP_SERVERS + " topic=" + KAFKA_TOPIC + " appId=" + KAFKA_APPLICATION_ID);
        System.out.println("State dir    : " + STATE_DIR + " store=" + NOME_STORE);
        System.out.println("ClickHouse   : " + CLICKHOUSE_URL + " utente=" + CLICKHOUSE_USER);
        System.out.println("PostgreSQL   : " + POSTGRES_URL + " utente=" + POSTGRES_USER);
        System.out.printf("Campionamento: %.1f s/campione, finestra velocita %d campioni%n", SECONDI_PER_CAMPIONE, FINESTRA_VELOCITA);
        System.out.printf("Allarme      : fermo %d s (%d campioni) entro %.1f m, oltre %.0f bpm%n",
                SECONDI_IMMOBILE, getCampioniImmobile(), SOGLIA_MOVIMENTO_M, SOGLIA_BPM_ALLARME);
        System.out.println("Batch DB     : " + MAX_CAMPIONI + " campioni");
        System.out.println("======================================");
    }
}
