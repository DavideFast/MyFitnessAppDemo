package analisi_immediata.modello;

// I nomi dei campi devono combaciare con le chiavi JSON prodotte da heart_rate_simulator.py
public record HeartRateSample(
         int athlete_id,
         String sport,
         double heart_rate,
         double cadence_spm,
         double latitude,
         double longitude,
         String timestamp,
         double altitude,
         double temperature,
         int sample_id,
         long session_id,
         String event_type) {
}
