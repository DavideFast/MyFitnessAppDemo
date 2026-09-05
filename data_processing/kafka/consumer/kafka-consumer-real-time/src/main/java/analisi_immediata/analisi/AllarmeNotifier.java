package analisi_immediata.analisi;

import analisi_immediata.config.Configurazione;

/** Notifica simulata: stampa a schermo al posto di inviare un SMS reale. */
public final class AllarmeNotifier {

    private AllarmeNotifier() {
    }

    public static void invia(String messaggio) {
        System.out.println("=========== ALLARME ===========");
        System.out.println("SMS simulato verso " + Configurazione.getDestinatarioAllarme());
        System.out.println(messaggio);
        System.out.println("===============================");
    }
}
