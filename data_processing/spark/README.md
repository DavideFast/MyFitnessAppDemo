# Spark on k3s

This module contains the Spark Python job for runner population analysis and the supporting files to run it on Kubernetes.

## Actual Status

- Runtime Spark: 3.5.3
- Main mode: SparkApplication (Spark Operator) started by the backend
- Namespace: bigintensive
- Job principale: RunningPopolationAnalysis.py

## Essential Structure

```text
data_processing/spark/
├── Dockerfile
├── README.md
├── submit-job.sh
└── jobs/
    ├── RunningPopolationAnalysis.py
    └── config.py
```

## Involved Kubernetes Components

- Manifest Spark/Jupyter: k3s/05-spark-and-jupyter.yaml
- Trigger job dal backend: endpoint /api/v1/startRunningPopulation
- Configurazione base Spark in ConfigMap spark-config

## Starting the Analysis Job

Il percorso standard e' chiamare il backend, che crea una SparkApplication con:

- image: davidefast/bigintensive-sparkwithdependencies:latest
- sparkVersion: 3.5.3
- mainApplicationFile: local:///opt/jobs/RunningPopolationAnalysis.py

Endpoint:

```text
POST /api/v1/startRunningPopulation
```

## Actual Spark Configuration (RunningPopulation job)

Parametri principali usati dal backend:

- spark.dynamicAllocation.enabled=true
- spark.dynamicAllocation.minExecutors=1
- spark.dynamicAllocation.maxExecutors=4
- spark.executor.cores=2
- spark.executor.memory=2g
- spark.sql.shuffle.partitions=10
- spark.driver.extraClassPath=/opt/spark/jars/clickhouse-jdbc-0.6.3-all.jar:/opt/spark/jars/postgresql-42.7.2.jar
- spark.executor.extraClassPath=/opt/spark/jars/clickhouse-jdbc-0.6.3-all.jar:/opt/spark/jars/postgresql-42.7.2.jar

## Build Spark Image

Il Dockerfile locale prepara un'immagine Spark con:

- job Python in /opt/jobs
- driver JDBC PostgreSQL e ClickHouse
- dipendenze Python di analisi

Esempio build:

```bash
cd data_processing/spark
docker build -t davidefast/bigintensive-sparkwithdependencies:latest .
```

## Jupyter (optional)

Jupyter e' disponibile nel cluster per esplorazione e benchmark notebook.

- URL tipico: http://jupyter.bigintensive.local
- Token configurato nel manifest corrente: bigintensive

Note: the production RunningPopulation job is started by the backend via SparkApplication, it does not depend on Jupyter.

## Data Sources Used by the Job

- ClickHouse (tabella running_samples)
- PostgreSQL (tabella anthropometric_values)

I dettagli di connessione sono centralizzati in jobs/config.py tramite variabili ambiente.

## Useful Operations

```bash
# SparkApplication nel namespace
kubectl get sparkapplications -n bigintensive

# Pod driver/executor Spark
kubectl get pods -n bigintensive -l spark-role=driver
kubectl get pods -n bigintensive -l spark-role=executor

# Log driver
kubectl logs -n bigintensive <driver-pod>
```

## Quick Troubleshooting

SparkApplication does not start:

- check Spark Operator in the spark-operator namespace
- check serviceAccount spark and RBAC in k3s/05-spark-and-jupyter.yaml
- check that the image davidefast/bigintensive-sparkwithdependencies:latest is available

JDBC Errors:

- check that the jars are present in /opt/spark/jars inside the image
- check credentials/env from ConfigMap and Secret bigintensive

Executors do not scale:

- check spark.dynamicAllocation.\* in the created SparkApplication
- check available node resources (cpu/mem)
