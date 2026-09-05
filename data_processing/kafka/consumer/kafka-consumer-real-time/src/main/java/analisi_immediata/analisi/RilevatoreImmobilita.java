package analisi_immediata.analisi;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import org.apache.kafka.streams.processor.api.Record;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import analisi_immediata.config.Configurazione;
import analisi_immediata.modello.CampioneDaSalvare;
import analisi_immediata.modello.HeartRateSample;
import analisi_immediata.modello.Posizione;
import analisi_immediata.modello.StatoSessione;
import analisi_immediata.persistenza.Database;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class RilevatoreImmobilita implements Processor<String, String, String, String> {
    
    private KeyValueStore<String, String> store;
    private ProcessorContext<String, String> context;
    
    private final Database db = new Database();
    private final List<CampioneDaSalvare> campioniDB = new ArrayList<>();
    private long campioniScartati = 0;
    private static final ObjectMapper MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private void svuotaBuffer() {
        if (campioniDB.isEmpty()) {
            return;
        }
        List<CampioneDaSalvare> batch = new ArrayList<>(campioniDB);
        campioniDB.clear();
        if (db.databaseBulkInsert(batch)) {
            return;
        }
        // Il tetto evita che un database irraggiungibile faccia crescere il buffer fino all'OutOfMemory
        if (batch.size() + campioniDB.size() <= Configurazione.getMaxCampioniSospesi()) {
            campioniDB.addAll(0, batch);
            System.err.printf("Batch di %d campioni rimesso in coda per un nuovo tentativo%n", batch.size());
        } else {
            System.err.printf("Batch di %d campioni scartato: superato il limite di %d campioni in sospeso%n",
                    batch.size(), Configurazione.getMaxCampioniSospesi());
        }
    }

    private void salvaStato(String chiave, StatoSessione stato) {
        try {
            store.put(chiave, MAPPER.writeValueAsString(stato));
        } catch (JsonProcessingException e) {
            System.err.println("Stato non salvabile per " + chiave + ": " + e.getMessage());
        }
    }

    private StatoSessione leggiStato(String chiave) {
        String json = store.get(chiave);
        if (json == null) {
            return null;
        }
        try {
            return MAPPER.readValue(json, StatoSessione.class);
        } catch (JsonProcessingException e) {
            // Ripartire da zero e l'unica opzione, ma va segnalato: gli aggregati della sessione vanno persi
            System.err.println("Stato illeggibile per " + chiave + ", sessione riavviata: " + e.getMessage());
            return null;
        }
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        this.store = context.getStateStore(Configurazione.getNomeStore());
        // Senza timer i campioni residui resterebbero in memoria finche non si arriva al batch pieno
        context.schedule(Duration.ofSeconds(Configurazione.getSecondiFlushBuffer()),
                PunctuationType.WALL_CLOCK_TIME, timestamp -> svuotaBuffer());
    }

    @Override
    public void close() {
        svuotaBuffer();
        db.chiudi();
    }

    @Override
    public void process(Record<String, String> record) {

        HeartRateSample sample;
        try {
            sample = MAPPER.readValue(record.value(), HeartRateSample.class);
        } catch (JsonProcessingException e) {
            campioniScartati++;
            if (campioniScartati % 1000 == 1) {
                System.err.printf("Messaggio malformato scartato (%d totali): %s%n", campioniScartati, e.getOriginalMessage());
            }
            return;
        }

        try {
            if ("end".equals(sample.event_type()) || "session_end".equals(sample.event_type())) {
                System.out.printf("Evento di fine ricevuto per atleta %d, sessione %d%n",
                        sample.athlete_id(), sample.session_id());
            }
            String chiave = record.key();
            if (chiave == null || chiave.isBlank()) {
                chiave = sample.athlete_id() + "-" + sample.session_id();
            }

            //Se non c'è uno stato precedente lo creo e esco
            StatoSessione precedente = leggiStato(chiave);
            if (precedente == null) {
                List<Posizione> storicoIniziale = new ArrayList<>();
                storicoIniziale.add(new Posizione(sample.latitude(), sample.longitude(), sample.sample_id()));
                salvaStato(chiave, new StatoSessione(sample.latitude(), sample.longitude(), sample.sample_id(), false,
                        0.0, 0.0, 0.0,
                        sample.heart_rate(), sample.heart_rate(), sample.cadence_spm(), 1, 0, storicoIniziale));
                return;
            }

            // Calcolo lo spostamento tra la posizione precedente e quella dell'evento corrente
            double spostamento = CalcoliMatematici.distanzaMetri(precedente.latitudine(), precedente.longitudine(),sample.latitude(), sample.longitude());
            if(sample.sample_id() == precedente.indice()) {
                System.out.printf("Campione duplicato per atleta %d, sessione %d, sample_id %d%n", sample.athlete_id(), sample.session_id(), sample.sample_id());
            }

            // Velocita sulla finestra dei 5 campioni precedenti, in m/s
            List<Posizione> storico = new ArrayList<>(precedente.storico());
            // L'ultimo elemento e ancora il campione precedente: serve per la distanza percorsa
            Posizione campionePrecedente = storico.isEmpty() ? null : storico.get(storico.size() - 1);
            storico.add(new Posizione(sample.latitude(), sample.longitude(), sample.sample_id()));
            double velocitaTratto = 0;
            double sommaVelocita = precedente.sommaVelocita();
            double velocitaMax = precedente.velocitaMax();
            int campioniVelocita = precedente.campioniVelocita();
            if (storico.size() > Configurazione.getFinestraVelocita()) {
                Posizione riferimento = storico.remove(0);
                int deltaCampioni = sample.sample_id() - riferimento.indice();
                if (deltaCampioni > 0) {
                    double distanzaFinestra = CalcoliMatematici.distanzaMetri(riferimento.latitudine(), riferimento.longitudine(),
                            sample.latitude(), sample.longitude());
                    velocitaTratto = distanzaFinestra / (deltaCampioni * Configurazione.getSecondiPerCampione());
                    sommaVelocita += velocitaTratto;
                    velocitaMax = Math.max(velocitaMax, velocitaTratto);
                    campioniVelocita++;
                }
            }

            double passo = campionePrecedente == null ? 0.0
                    : CalcoliMatematici.distanzaMetri(campionePrecedente.latitudine(), campionePrecedente.longitudine(),
                            sample.latitude(), sample.longitude());
            double distanzaTotale = precedente.distanzaTotale() + passo;
            double sommaFrequenza = precedente.sommaFrequenza() + sample.heart_rate();
            double frequenzaMax = Math.max(precedente.frequenzaMax(), sample.heart_rate());
            double sommaCadenza = precedente.sommaCadenza() + sample.cadence_spm();
            int campioni = precedente.campioni() + 1;

            campioniDB.add(new CampioneDaSalvare(sample, velocitaTratto));
            if (campioniDB.size() >= Configurazione.getMaxCampioni()) {
                svuotaBuffer();
            }
            // Il simulatore usa "end"; manteniamo compatibilita con il precedente "session_end".
            if ("end".equals(sample.event_type()) || "session_end".equals(sample.event_type())) {
                System.out.printf("Fine sessione: velocitaMedia=%.4f, velocitaMax=%.4f, frequenzaMedia=%.4f, "
                                + "frequenzaMax=%.4f, cadenzaMedia=%.4f, distanzaMetri=%.4f, campioni=%d, durataMinuti=%d%n",
                        campioniVelocita > 0 ? sommaVelocita / campioniVelocita : 0.0,
                        velocitaMax,
                        sommaFrequenza / campioni,
                        frequenzaMax,
                        sommaCadenza / campioni,
                        distanzaTotale,
                        campioni,
                        (int) Math.round(campioni * Configurazione.getSecondiPerCampione() / 60.0));
                db.databaseSummary(campioniVelocita > 0 ? sommaVelocita / campioniVelocita : 0.0, velocitaMax,
                        sommaFrequenza / campioni, frequenzaMax,
                        sommaCadenza / campioni, distanzaTotale, campioni,
                        sample, context.currentStreamTimeMs());
                store.delete(chiave);
                return;
            }

            // Se lo spostamento è maggiore della soglia, aggiorno la posizione di riferimento
            if (spostamento > Configurazione.getSogliaMovimentoM()) {
                salvaStato(chiave, new StatoSessione(sample.latitude(), sample.longitude(), sample.sample_id(), false,
                        distanzaTotale, sommaVelocita, velocitaMax, sommaFrequenza, frequenzaMax, sommaCadenza,
                        campioni, campioniVelocita, storico));
                return;
            }

            // Calcolo da quanti campioni l'atleta e fermo
            int campioniFermo = sample.sample_id() - precedente.indice();
            boolean allarmeInviato = precedente.allarmeInviato();
            String messaggio = null;
            if (campioniFermo >= Configurazione.getCampioniImmobile() && !allarmeInviato
                    && sample.heart_rate() > Configurazione.getSogliaBpmAllarme()) {
                allarmeInviato = true;
                messaggio = String.format("Atleta %s fermo da %.0f s in posizione %.6f, %.6f (bpm %.0f)",
                        sample.athlete_id(), campioniFermo * Configurazione.getSecondiPerCampione(),
                        sample.latitude(), sample.longitude(), sample.heart_rate());
            }

            // La posizione di riferimento resta quella precedente: serve a misurare l'immobilita cumulata
            salvaStato(chiave, new StatoSessione(precedente.latitudine(), precedente.longitudine(), precedente.indice(), allarmeInviato,
                    distanzaTotale, sommaVelocita, velocitaMax, sommaFrequenza, frequenzaMax, sommaCadenza,
                    campioni, campioniVelocita, storico));

            if (messaggio != null) {
                AllarmeNotifier.invia(messaggio);
                context.forward(record.withValue(messaggio));
            }
        } catch (RuntimeException e) {
            // Un bug non deve fermare la topologia, ma nemmeno sparire in silenzio
            System.err.printf("Errore elaborando il campione dell'atleta %d, sessione %d: %s%n",
                    sample.athlete_id(), sample.session_id(), e);
            e.printStackTrace();
        }
    }
}

