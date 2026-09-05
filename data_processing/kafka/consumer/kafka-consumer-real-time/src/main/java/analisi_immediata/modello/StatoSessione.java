package analisi_immediata.modello;
import java.util.List;
/** Stato persistito nello state store, serializzato come JSON. Gli aggregati sono per (atleta, sessione). */
public record StatoSessione(
        double latitudine,
        double longitudine,
        int indice,
        boolean allarmeInviato,
        double distanzaTotale,
        double sommaVelocita,
        double velocitaMax,
        double sommaFrequenza,
        double frequenzaMax,
        double sommaCadenza,
        int campioni,
        int campioniVelocita,
        List<Posizione> storico) {
}