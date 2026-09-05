# Spark Native Kubernetes Setup

## Context

This project aim to simulate a real-time data processing pipeline related to a Fitness App.
We assume that in this app are signed up 500'000 athletes.
The parts of this system are:

- **Citus**: A distributed PostgreSQL database to store athletes data
- **Clickhouse**: A columnar database for storing the results of Spark jobs

- **Kafka**: A distributed streaming platform to handle real-time metrics from athletes
- **Spark**: A distributed data processing engine to analyze the data from Citus and Kafka

- **Frontend**: A web application to visualize the results of Spark jobs
- **Backend**: A REST API to serve the results of Spark jobs to the frontend

- **k3s**: A lightweight Kubernetes distribution to run the cluster on a single machine

- **Jupyter**: A web-based interactive computing platform to submit Spark jobs and analyze the data

## Overview

Spark è configurato in **native Kubernetes mode** nel cluster k3s. Questo significa:

- **Driver Pod**: Crea il pod driver quando sottometti un job
- **Executor Pods**: Kubernetes crea dinamicamente i pod executor e li distribuisce sui nodi disponibili
- **Auto-scaling**: Gli executor scale automaticamente in base ai lavori
- **No Spark Master**: Non c'è un Spark master centralizzato, tutto gestito da Kubernetes

Il Kubeflow Spark Operator è installato nel namespace `spark-operator`. Rimane pronto a gestire risorse `SparkApplication`, ma non esegue analisi finché un client autorizzato non crea esplicitamente un job.

## Accesso a Jupyter

Jupyter è un ambiente opzionale per sviluppo, esplorazione e test manuali. Puoi anche sottomettere job Spark da altri client autorizzati, senza che Jupyter sia coinvolto.

### URL

```
http://jupyter.bigintensive.local:8888
```

Se accedi direttamente dall'IP:

```
http://192.168.1.x:8888
```

### Token

Il token è salvato in:

```bash
kubectl -n bigintensive get secret bigintensive-secrets -o jsonpath='{.data.JUPYTER_TOKEN}' | base64 -d
```

Di default: `changeme`

## Configurazione Spark

La configurazione Spark è salvata in `ConfigMap spark-config`:

```bash
kubectl -n bigintensive get configmap spark-config -o yaml
```

### Parametri chiave

| Parametro                    | Valore                                 | Significato                        |
| ---------------------------- | -------------------------------------- | ---------------------------------- |
| `spark.master`               | `k8s://https://kubernetes.default:443` | Native Kubernetes mode             |
| `spark.kubernetes.namespace` | `bigintensive`                         | Namespace per pods driver/executor |
| `spark.executor.cores`       | `2`                                    | CPU per executor                   |
| `spark.executor.memory`      | `2g`                                   | RAM per executor                   |
| `spark.dynamicAllocation.enabled` | `true`                            | Crea e rimuove executor in base al carico |
| `spark.dynamicAllocation.minExecutors` | `1`                           | Numero minimo di executor          |
| `spark.dynamicAllocation.maxExecutors` | `4`                           | Limite massimo di executor         |
| `spark.driver.memory`        | `2g`                                   | RAM del driver                     |
| `spark.driver.cores`         | `2`                                    | CPU del driver                     |

## Usare Spark da Jupyter

### 1. Avviare il notebook di inizializzazione

Nel browser, vai a `http://jupyter.bigintensive.local` e apri:

```
work/01_spark_initialization.ipynb
```

### 2. Creare SparkSession

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.master", "k8s://https://kubernetes.default:443") \
    .config("spark.kubernetes.namespace", "bigintensive") \
    .getOrCreate()
```

### 3. Leggere da Citus

```python
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://citus-coordinator:5432/bigintensive") \
    .option("dbtable", "public.athletes") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .load()

df.show()
```

### 4. Leggere da Kafka

```python
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "athletes-metrics") \
    .load()
```

### 5. Analisi e Join

```python
# Join athletes with metrics
result = athletes.join(metrics, "athlete_id") \
    .groupBy("athlete_id") \
    .agg({"potenza_sviluppata": "avg"})

result.show()
```

## Monitorare i Pod Executor

Mentre un job è in esecuzione, puoi vedere i pod executor:

```bash
kubectl -n bigintensive get pods -l spark-role=executor
```

Esempio output:

```
NAME                        READY   STATUS    RESTARTS   AGE
myapp-exec-1                1/1     Running   0          10s
myapp-exec-2                1/1     Running   0          10s
```

Quando il job finisce, i pod executor vengono terminati automaticamente.

## Distribuire i Pod Executor

I pod executor vengono distribuiti su tutti i nodi disponibili. Attualmente:

- **VM1 (server)**: Ospita il driver e ha `bigintensive.io/local-images: true`
- **VM2 (agent)**: Disponibile per executor pods

I driver pod rimangono su VM1 (perché hanno immagini locali), ma gli executor possono distribuirsi.

Se vuoi forzare una distribuzione specifica, modifica il manifest:

```yaml
spec:
  template:
    spec:
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                labelSelector:
                  matchExpressions:
                    - key: spark-role
                      operator: In
                      values:
                        - executor
                topologyKey: kubernetes.io/hostname
```

## Connessioni alle risorse

Le variabili d'ambiente sono già configurate nel pod Jupyter:

| Variabile                 | Valore              |
| ------------------------- | ------------------- |
| `CITUS_HOST`              | `citus-coordinator` |
| `CITUS_PORT`              | `5432`              |
| `CITUS_USER`              | `postgres`          |
| `CITUS_PASSWORD`          | `postgres`          |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:19092`       |

Usale nei tuoi notebook:

```python
import os
citus_host = os.getenv('CITUS_HOST')
kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
```

## Debug e Troubleshooting

### Vedere i log del driver

```bash
kubectl -n bigintensive logs <driver-pod-name>
```

### Vedere i log degli executor

```bash
kubectl -n bigintensive logs <executor-pod-name>
```

### Verificare le risorse

```bash
# Vedere tutti i pod in bigintensive
kubectl -n bigintensive get pods

# Vedere le risorse richieste
kubectl -n bigintensive top pods
```

### Pulire i pod bloccati

Se un pod rimane in stato Pending:

```bash
kubectl -n bigintensive delete pod <pod-name>
```

## Limitazioni e Note

1. **Java/Scala**: I notebook Jupyter usano Python. Per usare Scala, devi sottomettere job via `spark-submit` con contenitori Scala.

2. **Immagini Docker**: Attualmente usiamo `python:3.11-slim`. Se hai librerie specializzate, crea un'immagine custom:

   ```dockerfile
   FROM python:3.11-slim
   RUN pip install pyspark numpy pandas scikit-learn
   ```

3. **Persistenza**: I notebook sono su `emptyDir`, quindi si perdono quando il pod viene ricreato. Per persistenza, usa un PersistentVolume.

## Prossimi passi

1. **Aggiungere librerie Python**: Modifica la Dockerfile di Jupyter per includere le tue dipendenze
2. **Creare notebook specializzati**: Aggiungi notebook per specifici workload (ETL, ML, etc.)
3. **Impostare monitoring**: Aggiungi Prometheus/Grafana per monitorare i job Spark
4. **Configurare storage persistente**: Usa PersistentVolumes per salvare notebook e risultati
