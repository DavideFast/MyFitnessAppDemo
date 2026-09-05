# Simulatore corse

E' possibile simulare il comportamento degli utenti, generando dati di frequenza cardiaca e non solo durante le corse. Il simulatore è configurabile tramite vari parametri, come il numero di atleti, l'intervallo di campionamento dei dati, la durata minima e massima delle sessioni di corsa e dei periodi di riposo.

Nel docker file sono specificati dei parametri adatti ad una dimostrazione pratica.
In particolare si consiglia di utilizzare intervalli di corse dai 2 ai 5 minuti così da poter osservare una intera sessione di corsa.

```dockerfile
ARG NUM_ATHLETES=20000
ARG SAMPLE_INTERVAL=5.0
ARG DANGER_EVENT_PROBABILITY=0.05
ARG DANGER_MIN_SAMPLES=6
ARG DANGER_MAX_SAMPLES=18
ARG MIN_SESSION_MINUTES=2
ARG MAX_SESSION_MINUTES=5
ARG MIN_REST_MINUTES=5
ARG MAX_REST_MINUTES=6
ARG INITIAL_START_WINDOW_MINUTES=1
```

Durante una sessione puo' essere generato un evento `danger_scenario`. Per la durata configurata la velocita' diminuisce drasticamente, mentre battito cardiaco e cadenza cambiano in modo coerente con una situazione anomala.

I parametri sono configurabili tramite variabili d'ambiente:

- `DANGER_EVENT_PROBABILITY`: probabilita' dell'evento per ogni nuova sessione, da `0` a `1`;
- `DANGER_MIN_SAMPLES` e `DANGER_MAX_SAMPLES`: durata casuale dell'evento in campioni.
