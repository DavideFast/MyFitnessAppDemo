# POSTGRESQL

## Comandi utili per la gestione del database PostgreSQL

1. Accedere al pod PostgreSQL:

```bash
# Avviare il container PostgreSQL
sudo kubectl exec -it -n bigintensive postgresql-0 -- psql -U <nome_utente> -d <nome_database>
```

2. Eseguire query SQL:

```sql
SELECT * FROM <nome_tabella>;
```

3. Vedere peso del database nel suo intero (dimensione totale):

```sql
SELECT pg_size_pretty(pg_database_size('<nome_database>'));
```

## Struttura del database PostgreSQL

Il database comprende le seguenti tabelle:

- **athletes**: memorizza le informazioni degli utenti, come nome, cognome, email e password.
- **allenamenti**: memorizza i dati relativi agli allenamenti degli utenti,
- **anthropometric_values**: memorizza i valori antropometrici degli utenti, come altezza, peso e età.
- **riepilogo_corse**: memorizza i dati riepilogativi delle corse degli utenti.
- **esercizi**: memorizza l'elenco degli esercizi disponibili per gli allenamenti degli utenti.
