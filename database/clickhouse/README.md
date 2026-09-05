# CLICKHOUSE

## Comandi utili per la gestione del database Clickhouse

Comandi utili per la gestione del database Clickhouse sono:

1. Accedere al pod Clickhouse:

```bash
# Avviare il client ClickHouse sul database applicativo
sudo kubectl exec -n bigintensive -it clickhouse-shard-1-0 -- clickhouse-client --database bigintensive
```

2. Eseguire query SQL:

```sql
SHOW DATABASES;
```

```sql
SHOW TABLES FROM bigintensive;
```

```sql
SELECT * FROM bigintensive.<nome_tabella> LIMIT 10;
```

## Struttura del database Clickhouse

Ogni tabella nel database Clickhouse è progettata per memorizzare dati specifici relativi alle corse degli utenti. La struttura delle tabelle è ottimizzata per l'analisi dei dati in tempo reale, consentendo query rapide e efficienti.

Le tabelle principali nel database Clickhouse includono:

- **allenamenti**: memorizza i dati relativi agli allenamenti degli utenti, inclusi i parametri di frequenza cardiaca, velocità e distanza percorsa.
- **allenamenti_raw**: memorizza i dati grezzi degli allenamenti degli utenti, inclusi i parametri di frequenza cardiaca, velocità e distanza percorsa, senza alcuna elaborazione o aggregazione.
- **running_samples**: memorizza i campioni di dati relativi alle corse degli utenti, inclusi i parametri di frequenza cardiaca, velocità e distanza percorsa, raccolti durante le sessioni di allenamento.

Per ogni tabella esiste la versione distribuita e la versione locale (`_local`). Le tabelle distribuite sono il punto di accesso dell'applicazione; le tabelle locali `ReplicatedMergeTree` memorizzano i dati sui singoli server ClickHouse.
