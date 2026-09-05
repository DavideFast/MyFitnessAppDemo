# Kafka Consumer Real-Time Analysis

Consumatore Kafka in Java che elabora i dati di telemetria dei runner in tempo reale usando Kafka Streams.

## Funzionalità

- **Rilevamento immobilità**: Monitora i runner per rilevare periodi di immobilità
- **Calcolo velocità**: Calcola la velocità media usando i dati GPS
- **Analisi deriva cardiaca**: Monitora la deriva della frequenza cardiaca durante le sessioni
- **Salvataggio dati**: Inserisce batch di dati in ClickHouse e riepiloghi in PostgreSQL
- **State Store**: Mantiene stato persistente per sopravvivere a riavvii e rebalance

## Prerequisiti

- Java 17+
- Maven 3.9.5+
- Kafka 3.6.0+
- ClickHouse
- PostgreSQL

## Struttura del Progetto

```
kafka-consumer-real-time/
├── src/main/java/analisi_immediata/
│   ├── StreamsAllarmi.java (main application)
│   ├── HeartRateSample.java (data model)
│   └── ...
├── pom.xml (Maven configuration con maven-shade-plugin per fat JAR)
└── target/
    └── kafka-consumer-real-time-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Variabili d'Ambiente

### Kafka

- `KAFKA_BOOTSTRAP_SERVERS`: Bootstrap servers Kafka (default: `kafka:19092`)
- `KAFKA_TOPIC`: Topic di input (default: `heart-rate-events`)

### ClickHouse

- `CLICKHOUSE_URL`: JDBC URL (default: `jdbc:clickhouse://clickhouse:8123/bigintensive`)
- `CLICKHOUSE_USER`: Username (default: `default`)
- `CLICKHOUSE_PASSWORD`: Password (default: empty)

### PostgreSQL

- `POSTGRES_URL`: JDBC URL (default: `jdbc:postgresql://postgres:5432/bigintensive`)
- `POSTGRES_USER`: Username (default: `postgres`)
- `POSTGRES_PASSWORD`: Password (default: `postgres`)

## Compilazione

### Localmente

```bash
cd kafka-consumer-real-time
mvn clean package -DskipTests
```

Il JAR completo sarà in `target/kafka-consumer-real-time-1.0-SNAPSHOT-jar-with-dependencies.jar`

### Con Build Script

```bash
cd streaming/kafka/consumer

# Solo build
./build.sh

# Build + Docker
./build.sh --docker

# Build + Docker + Push to registry
./build.sh --docker --push davidefast
```

## Deployment

### Kubernetes (K3s)

1. Creare l'immagine Docker:

```bash
docker build -t davidefast/consumer-kafka:latest streaming/kafka/consumer/
```

2. Caricare l'immagine nel registry locale K3s (se non in uso DockerHub)

3. Applicare il manifesto:

```bash
kubectl apply -f k3s/08-kafka-consumer.yaml
```

**Manifesto include:**

- ConfigMap con le variabili d'ambiente
- Deployment con 2 replica per alta disponibilità
- Service per esporre le metriche
- Probes di liveness e readiness
- Affinity policy per distribuire i pod
- Tolerations per node taints

**Verificare il deployment:**

```bash
# Status deployment
kubectl get deployment kafka-consumer-realtime -n bigintensive

# Log dei pod
kubectl logs -f kafka-consumer-realtime-xxxxx -n bigintensive

# Descrivere il deployment
kubectl describe deployment kafka-consumer-realtime -n bigintensive
```

## Esecuzione

### Locale (standalone)

```bash
java -Xms256m -Xmx512m -jar target/kafka-consumer-real-time-1.0-SNAPSHOT-jar-with-dependencies.jar
```

Con variabili d'ambiente:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export CLICKHOUSE_URL=jdbc:clickhouse://localhost:8123/bigintensive
export POSTGRES_URL=jdbc:postgresql://localhost:5432/bigintensive

java -Xms256m -Xmx512m -jar target/kafka-consumer-real-time-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Architettura

```
Kafka Topic: heart-rate-events
  ↓
Kafka Streams Application (StreamsAllarmi)
  ├─ State Store: stato-sessioni (persistente)
  ├─ Processor: RilevatoreImmobilita
  │   ├─ Analizza dati GPS e frequenza cardiaca
  │   ├─ Rileva anomalie (immobilità, crisi cardiaca)
  │   └─ Calcola metriche aggregate
  │
  ├─ Output 1: Batch Insert → ClickHouse
  │   └─ Tabella: heart_rate_samples
  │
  └─ Output 2: Summary Insert → PostgreSQL
      └─ Tabella: session_summary
```

## Algoritmi Principali

### Calcolo Distanza GPS

Utilizza la formula di Haversine per calcolare la distanza tra due coordinate GPS:

```
R = 6.371.000 m (raggio terra)
dLat = lat2 - lat1
dLon = lon2 - lon1
a = sin²(dLat/2) + cos(lat1) * cos(lat2) * sin²(dLon/2)
c = 2 * atan2(√a, √(1-a))
distanza = R * c
```

### Rilevamento Immobilità

Monitora il movimento e la frequenza cardiaca per identificare periodi di immobilità:

- **Soglia movimento**: 10 metri
- **Soglia tempo immobile**: 30 secondi
- **Allarme**: Se rimane immobile oltre la soglia

### Analisi Deriva Cardiaca

```
Efficienza_puntuale = velocita_puntuale / frequenza_cardiaca_media
Deriva_cardiaca_percentuale = (Efficienza_attuele - Efficienza_iniziale) / Efficienza_iniziale * 100
```

## Monitoraggio

### Health Check

Il consumer include health check che verifica il processo Java è in esecuzione.

### Metriche (Opzionale - da implementare)

Potrebbe essere aggiunto Prometheus per metriche:

- Record elaborati per secondo
- Latenza di elaborazione
- Rate di errori nel salvataggio in DB

### Log

I log sono salvati in:

- Docker: `docker logs kafka-consumer-realtime`
- K3s: `kubectl logs <pod-name> -n bigintensive`
- File locale: `/var/log/kafka-consumer/`

## Troubleshooting

### Consumer non si connette a Kafka

```bash
# Verificare bootstrap servers
kubectl exec -it <pod-name> -n bigintensive -- /bin/sh
telnet kafka 19092
```

### Errore di connessione ClickHouse

- Verificare CLICKHOUSE_URL
- Controllare credentials
- Verificare che la tabella esista

### Errore di connessione PostgreSQL

- Verificare POSTGRES_URL
- Controllare credentials
- Verificare che le tabelle esistano

## Versioni Dipendenze

- Kafka: 3.6.0
- ClickHouse JDBC: 0.4.6
- PostgreSQL JDBC: 42.6.0
- Jackson: 2.17.2
- Java: 17

## Prossimi Passi

1. ✅ Implementare consumer Kafka Streams
2. ✅ Aggiungere nei manifesti K3s
3. ⏳ Implementare metriche Prometheus
4. ⏳ Aggiungere alerting per anomalie critiche
5. ⏳ Implementare graceful shutdown

## Supporto e Contatti

Per domande o problemi, contattare il team di sviluppo.
