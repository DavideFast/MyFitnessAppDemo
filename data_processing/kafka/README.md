# Kafka configuration

Questa cartella centralizza la configurazione dei topic Kafka in modo ordinato e riutilizzabile.

## Struttura

- `topics.json`: definizioni canoniche dei topic da creare
- `scripts/create-topics.sh`: script per ambiente Linux/macOS
- `scripts/create-topics.ps1`: script per Windows PowerShell

## Topic principali

I topic attualmente previsti sono:

- `demo-events`
- `heart-rate-events`
- `workout-events`
- `smartwatch-status`
- `spark-analytics`
- `system-events`

## Creazione topic

### k3s (percorso consigliato)

Il cluster k3s e' il percorso principale del progetto. I topic vanno creati direttamente nel pod Kafka tramite `kubectl exec`.

```bash
bash ./k3s/create-kafka-topics.sh
```

Per usare un namespace o bootstrap server diverso:

```bash
bash ./k3s/create-kafka-topics.sh --namespace bigintensive --bootstrap-server kafka:19092
```

### Windows PowerShell

```powershell
.\streaming\kafka\scripts\create-topics.ps1 -BootstrapServer localhost:9094
```

## Convenzioni

- ogni topic ha un numero di partizioni e una replication factor definiti qui
- i parametri di retention e cleanup sono centralizzati in una singola configurazione
- aggiungere nuovi topic richiede solo una modifica in `topics.json`

Questo mantiene la gestione Kafka più professionale e più facile da mantenere nel tempo.
