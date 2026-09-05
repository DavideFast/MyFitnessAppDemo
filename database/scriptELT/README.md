# Script per effettuare l'ELT dei dati ricevuti dai client

Questo script è stato creato per effettuare l'ELT (Extract, Load, Transform) dei dati ricevuti dai client e memorizzati nel database PostgreSQL verso il database Clickhouse destinato all'analisi massiva dei dati.

Lo script viene eseguito in due step:

1. **Estrazione e caricamento dei dati**: i dati vengono estratti dal database PostgreSQL e caricati nel database Clickhouse.
2. **Trasformazione dei dati**: i dati vengono trasformati nel database Clickhouse per ottimizzare le query e le analisi dei dati.

Vengono estratti da Clickhouse e non in Postgresql poichè Clickhouse è ottimizzato per l'analisi dei dati in tempo reale e consente di eseguire query complesse in modo efficiente. Altrimenti doveva essere lo script python in se per se a fare l'estrazione dei dati da Postgresql e caricarli in Clickhouse, ma questo avrebbe comportato un carico maggiore sul server e tempi di esecuzione più lunghi.
