# Spark su k3s

Questo modulo contiene il job Spark Python di analisi popolazione runner e i file di supporto per eseguirlo su Kubernetes.

## Stato attuale

- Runtime Spark: 3.5.3
- Modalita' principale: SparkApplication (Spark Operator) avviata dal backend
- Namespace: bigintensive
- Job principale: RunningPopolationAnalysis.py

## Struttura essenziale

```text
data_processing/spark/
├── Dockerfile
├── README.md
├── submit-job.sh
└── jobs/
    ├── RunningPopolationAnalysis.py
    └── config.py
```

## Componenti Kubernetes coinvolti

- Manifest Spark/Jupyter: k3s/05-spark-and-jupyter.yaml
- Trigger job dal backend: endpoint /api/v1/startRunningPopulation
- Configurazione base Spark in ConfigMap spark-config

## Avvio del job di analisi

Il percorso standard e' chiamare il backend, che crea una SparkApplication con:

- image: davidefast/bigintensive-sparkwithdependencies:latest
- sparkVersion: 3.5.3
- mainApplicationFile: local:///opt/jobs/RunningPopolationAnalysis.py

Endpoint:

```text
POST /api/v1/startRunningPopulation
```

## Configurazione Spark effettiva (job RunningPopulation)

Parametri principali usati dal backend:

- spark.dynamicAllocation.enabled=true
- spark.dynamicAllocation.minExecutors=1
- spark.dynamicAllocation.maxExecutors=4
- spark.executor.cores=2
- spark.executor.memory=2g
- spark.sql.shuffle.partitions=10
- spark.driver.extraClassPath=/opt/spark/jars/clickhouse-jdbc-0.6.3-all.jar:/opt/spark/jars/postgresql-42.7.2.jar
- spark.executor.extraClassPath=/opt/spark/jars/clickhouse-jdbc-0.6.3-all.jar:/opt/spark/jars/postgresql-42.7.2.jar

## Build immagine Spark

Il Dockerfile locale prepara un'immagine Spark con:

- job Python in /opt/jobs
- driver JDBC PostgreSQL e ClickHouse
- dipendenze Python di analisi

Esempio build:

```bash
cd data_processing/spark
docker build -t davidefast/bigintensive-sparkwithdependencies:latest .
```

## Jupyter (opzionale)

Jupyter e' disponibile nel cluster per esplorazione e benchmark notebook.

- URL tipico: http://jupyter.bigintensive.local
- Token configurato nel manifest corrente: bigintensive

Nota: il job production RunningPopulation e' avviato dal backend via SparkApplication, non dipende da Jupyter.

## Sorgenti dati usate dal job

- ClickHouse (tabella running_samples)
- PostgreSQL (tabella anthropometric_values)

I dettagli di connessione sono centralizzati in jobs/config.py tramite variabili ambiente.

## Operazioni utili

```bash
# SparkApplication nel namespace
kubectl get sparkapplications -n bigintensive

# Pod driver/executor Spark
kubectl get pods -n bigintensive -l spark-role=driver
kubectl get pods -n bigintensive -l spark-role=executor

# Log driver
kubectl logs -n bigintensive <driver-pod>
```

## Troubleshooting rapido

SparkApplication non parte:

- verificare Spark Operator nel namespace spark-operator
- verificare serviceAccount spark e RBAC in k3s/05-spark-and-jupyter.yaml
- verificare immagine davidefast/bigintensive-sparkwithdependencies:latest disponibile

Errori JDBC:

- verificare presenza jar in /opt/spark/jars dentro l'immagine
- verificare credenziali/env da ConfigMap e Secret bigintensive

Executor non scalano:

- verificare spark.dynamicAllocation.* nella SparkApplication creata
- verificare risorse nodo disponibili (cpu/mem)
