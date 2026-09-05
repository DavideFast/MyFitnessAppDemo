# Kafka Consumer Real-Time Analysis

Kafka Consumer for real-time analysis of smartwatch samples.

## Current Status

- Runtime on Kubernetes via StatefulSet: k3s/08-kafka-consumer.yaml
- Autoscaling via KEDA based on consumer lag
- Main input topic: heart-rate-events

## Main Features

- Immobility detection
- Average speed calculation on a sliding window
- Heart rate drift analysis
- Data saving in ClickHouse and PostgreSQL
- Persistent state store on volume (survives pod restarts)

## Java Package Structure

```text
analisi_immediata/
├── ConsumerKafka.java
├── analisi/
│   ├── AllarmeNotifier.java
│   ├── CalcoliMatematici.java
│   └── RilevatoreImmobilita.java
├── config/
│   └── Configurazione.java
├── modello/
│   ├── CampioneDaSalvare.java
│   ├── HeartRateSample.java
│   ├── Posizione.java
│   └── StatoSessione.java
└── persistenza/
	└── Database.java
```

Quick explanation of the classes:

- `ConsumerKafka.java`: main class that starts the Kafka Streams application.
- `AllarmeNotifier.java`: class responsible for immobility and potentially dangerous situations notifications.
- `CalcoliMatematici.java`: utility class for calculate distance between GPS positions.
- `RilevatoreImmobilita.java`: class that detects immobility events and saves them to the state store while they are being monitored.
- `Configurazione.java`: class that handles application configuration.
- `CampioneDaSalvare.java`: data model for samples to be saved.
- `HeartRateSample.java`: data model for heart rate samples.
- `Posizione.java`: data model for position information.
- `StatoSessione.java`: data model for session state.
- `Database.java`: class that manages database interactions.

## Prerequisites

- Java 17
- Maven 3.9+
- Docker
- Cluster k3s with bigintensive namespace

## Build Docker Image

From the project root:

```bash
cd data_processing/kafka/consumer
docker build -t davidefast/consumer-kafka:latest .
```

It will build the jar and the Docker image with the tag `davidefast/consumer-kafka:latest`.

## Access the Consumer Logs

To access the logs of the Kafka consumer running in the k3s cluster, use the following command:

```bash
kubectl logs -f statefulset/kafka-consumer-realtime -n bigintensive
```

The AllarmeNotifier class simulates notifications for immobility and potentially dangerous situations writing to the logs.

## Main Variables

Kafka:

- KAFKA_BOOTSTRAP_SERVERS (default in cluster: kafka:19092)
- KAFKA_TOPIC (default: heart-rate-events)
- KAFKA_APPLICATION_ID
- KAFKA_STATE_DIR

Processing:

- SAMPLE_INTERVAL
- MAX_CAMPIONI_BATCH
- SECONDI_FLUSH_BUFFER
- MAX_CAMPIONI_SOSPESI
- FINESTRA_VELOCITA
- SOGLIA_MOVIMENTO_M
- SECONDI_IMMOBILE

Database:

- CLICKHOUSE_URL
- CLICKHOUSE_USER
- CLICKHOUSE_PASSWORD (secret)
- POSTGRES_URL
- POSTGRES_USER
- POSTGRES_PASSWORD (secret)

## Operations and Checks

```bash
# StatefulSet Status
kubectl get statefulset kafka-consumer-realtime -n bigintensive

# Consumer Pods
kubectl get pods -n bigintensive -l app=kafka-consumer

# KEDA Scaler
kubectl get scaledobject kafka-consumer-realtime-scaler -n bigintensive

# Consumer Logs
kubectl logs -f statefulset/kafka-consumer-realtime -n bigintensive
```

## Application Dependencies

From the current pom.xml:

- kafka-clients: 3.6.0
- kafka-streams: 3.6.0
- clickhouse-jdbc: 0.4.6
- postgresql: 42.6.0
- jackson-databind: 2.17.2

> [!TIP]
> Jackson Databind is used for JSON serialization and deserialization.
