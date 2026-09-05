package analisi_immediata.modello;

/** La velocita e calcolata sul tratto del singolo campione, quindi va conservata per campione. */
public record CampioneDaSalvare(HeartRateSample sample, double velocita) {
}
