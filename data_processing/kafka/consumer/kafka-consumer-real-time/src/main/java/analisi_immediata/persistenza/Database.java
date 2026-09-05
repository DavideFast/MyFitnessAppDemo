package analisi_immediata.persistenza;
import analisi_immediata.config.Configurazione;
import analisi_immediata.modello.CampioneDaSalvare;
import analisi_immediata.modello.HeartRateSample;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** Non e thread-safe: va istanziata una volta per task di Kafka Streams. */
public class Database {

    private static final DateTimeFormatter FORMATO_TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private Connection connessioneClickhouse;
    private Connection connessionePostgres;

    public Database() {
    }

    private Connection clickhouse() throws SQLException {
        if (connessioneClickhouse == null || !connessioneClickhouse.isValid(2)) {
            chiudiSilenziosamente(connessioneClickhouse);
            connessioneClickhouse = DriverManager.getConnection(
                    Configurazione.getClickhouseUrl(),
                    Configurazione.getClickhouseUser(),
                    Configurazione.getClickhousePassword());
            System.out.println("Connessione a ClickHouse aperta: " + Configurazione.getClickhouseUrl());
        }
        return connessioneClickhouse;
    }

    private Connection postgres() throws SQLException {
        if (connessionePostgres == null || !connessionePostgres.isValid(2)) {
            chiudiSilenziosamente(connessionePostgres);
            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                throw new SQLException("Driver PostgreSQL non presente nel classpath", e);
            }
            connessionePostgres = DriverManager.getConnection(
                    Configurazione.getPostgresUrl(),
                    Configurazione.getPostgresUser(),
                    Configurazione.getPostgresPassword());
            System.out.println("Connessione a PostgreSQL aperta: " + Configurazione.getPostgresUrl());
        }
        return connessionePostgres;
    }

    private static void chiudiSilenziosamente(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (SQLException e) {
            // connessione gia caduta: niente da fare
        }
    }

    public void chiudi() {
        chiudiSilenziosamente(connessioneClickhouse);
        chiudiSilenziosamente(connessionePostgres);
        connessioneClickhouse = null;
        connessionePostgres = null;
    }

    /** @return false se il batch non e stato scritto, cosi il chiamante puo ritentare. */
    public boolean databaseBulkInsert(List<CampioneDaSalvare> campioni) {
        String sql = "INSERT INTO running_samples (sample_id, session_id, athlete_id, timestamp, velocity, heart_rate, latitude, longitude, altitude, temperature, cadence, event_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement stmt = clickhouse().prepareStatement(sql)) {
            for (CampioneDaSalvare campione : campioni) {
                HeartRateSample sample = campione.sample();
                OffsetDateTime odt = OffsetDateTime.parse(sample.timestamp());
                stmt.setInt(1, sample.sample_id());
                stmt.setLong(2, sample.session_id());
                stmt.setLong(3, sample.athlete_id());
                stmt.setString(4, odt.format(FORMATO_TIMESTAMP));
                stmt.setDouble(5, campione.velocita());
                stmt.setDouble(6, sample.heart_rate());
                stmt.setDouble(7, sample.latitude());
                stmt.setDouble(8, sample.longitude());
                stmt.setDouble(9, sample.altitude());
                stmt.setDouble(10, sample.temperature());
                stmt.setDouble(11, sample.cadence_spm());
                stmt.setString(12, sample.event_type());
                stmt.addBatch();
            }
            stmt.executeBatch();
            System.out.printf("Scritti %d campioni su ClickHouse%n", campioni.size());
            return true;
        } catch (SQLException e) {
            System.err.println("Errore durante l'inserimento batch nel database: " + e.getMessage());
            chiudiSilenziosamente(connessioneClickhouse);
            connessioneClickhouse = null;
            return false;
        }
    }

    public void databaseSummary(double velocitaMedia, double velocitaMax, double frequenzaCardiacaMedia, double frequenzaCardiacaMax, double cadenzaMedia, double distanzaTotale, int campioni, HeartRateSample sample, long timestamp) {
        System.out.printf("Scrittura riepilogo sessione per atleta %d, sessione %d, campioni %d%n", sample.athlete_id(), sample.session_id(), campioni);

        double distanzaKm = distanzaTotale / 1000.0;
        int durataMinuti = (int) Math.round(campioni * Configurazione.getSecondiPerCampione() / 60.0);

        // riepilogo_corse ha vincoli CHECK > 0 su tutte le misure: una sessione troppo corta non e inseribile
        if (velocitaMedia <= 0 || velocitaMax <= 0 || frequenzaCardiacaMedia <= 0 || frequenzaCardiacaMax <= 0
                || cadenzaMedia <= 0 || distanzaKm <= 0 || durataMinuti <= 0) {
            System.out.printf("Riepilogo scartato per atleta %d, sessione %d: valori non positivi%n",
                    sample.athlete_id(), sample.session_id());
            return;
        }

        String sql = "INSERT INTO riepilogo_corse (athlete_id, velocita_media, velocita_max, frequenza_media, frequenza_max, cadenza, distanza_km, durata_minuti, data_corsa) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement stmt = postgres().prepareStatement(sql)) {
            stmt.setInt(1, sample.athlete_id());
            stmt.setDouble(2, velocitaMedia);
            stmt.setDouble(3, velocitaMax);
            stmt.setDouble(4, frequenzaCardiacaMedia);
            stmt.setDouble(5, frequenzaCardiacaMax);
            stmt.setDouble(6, cadenzaMedia);
            stmt.setDouble(7, distanzaKm);
            stmt.setInt(8, durataMinuti);
            stmt.setDate(9, new java.sql.Date(timestamp));
            stmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Errore durante l'inserimento del riepilogo nel database: " + e.getMessage());
            chiudiSilenziosamente(connessionePostgres);
            connessionePostgres = null;
        }
    }
}