# Kafka Consumer Real-Time Analysis

Consumer Kafka in Java (Kafka Streams) per l'analisi realtime dei campioni smartwatch.

## Stato attuale

- Runtime su Kubernetes tramite StatefulSet: k3s/08-kafka-consumer.yaml
- Autoscaling tramite KEDA su consumer lag
- Topic di input principale: heart-rate-events

## Funzionalita principali

- Rilevamento immobilita
- Calcolo velocita media su finestra mobile
- Analisi deriva cardiaca
- Salvataggio dati in ClickHouse e PostgreSQL
- State store persistente su volume (sopravvive ai restart dei pod)

## Struttura package Java

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

## Prerequisiti

- Java 17
- Maven 3.9+
- Docker
- Cluster k3s con namespace bigintensive

## Build locale

```bash
cd data_processing/kafka/consumer/kafka-consumer-real-time
mvn clean package -DskipTests
```

Output JAR:

```text
target/kafka-consumer-real-time-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Build Docker

Dalla root del progetto:

```bash
cd data_processing/kafka/consumer
docker build -t davidefast/consumer-kafka:latest .
```

Script di supporto:

```bash
cd data_processing/kafka/consumer

# Build JAR
./build.sh

# Build JAR + Docker
./build.sh --docker

# Build JAR + Docker + Push
./build.sh --docker --push davidefast
```

## Deploy su k3s

Percorso consigliato: deploy orchestrato da script principale.

```bash
bash k3s/deploy-all.sh
```

Deploy solo consumer:

```bash
kubectl apply -f k3s/08-kafka-consumer.yaml
```

## Cosa contiene 08-kafka-consumer.yaml

- ConfigMap kafka-consumer-config con parametri applicativi
- StatefulSet kafka-consumer-realtime (repliche iniziali: 2)
- PVC per state store locale Kafka Streams
- ScaledObject KEDA (min 2, max 6) basato su lag topic
- Init container che aspetta bootstrap Kafka e topic disponibili
- Service headless + service metrics

## Variabili principali

Kafka:

- KAFKA_BOOTSTRAP_SERVERS (default in cluster: kafka:19092)
- KAFKA_TOPIC (default: heart-rate-events)
- KAFKA_APPLICATION_ID
- KAFKA_STATE_DIR

Elaborazione:

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

## Operativita e controlli

```bash
# Stato StatefulSet
kubectl get statefulset kafka-consumer-realtime -n bigintensive

# Pod consumer
kubectl get pods -n bigintensive -l app=kafka-consumer

# Scaler KEDA
kubectl get scaledobject kafka-consumer-realtime-scaler -n bigintensive

# Log consumer
kubectl logs -f statefulset/kafka-consumer-realtime -n bigintensive
```

## Troubleshooting rapido

Kafka non raggiungibile:

- Verifica KAFKA_BOOTSTRAP_SERVERS in ConfigMap
- Verifica broker e topic in namespace bigintensive

Consumer in crash loop:

- Controlla secret DB (CLICKHOUSE_PASSWORD, POSTGRES_PASSWORD)
- Controlla raggiungibilita ClickHouse/PostgreSQL dai pod

Nessun consumo:

- Verifica lag e stato del consumer group rilevatore-immobilita
- Verifica che il simulatore pubblichi su heart-rate-events

## Versioni dipendenze applicative

Dal pom.xml corrente:

- kafka-clients: 3.6.0
- kafka-streams: 3.6.0
- clickhouse-jdbc: 0.4.6
- postgresql: 42.6.0
- jackson-databind: 2.17.2
