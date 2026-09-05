# Kafka configuration

Questa cartella centralizza la configurazione dei topic Kafka in modo ordinato e riutilizzabile.

## Topic principali

The main Kafka topics used in the project are:

- `demo-events`
- `heart-rate-events`
- `workout-events`
- `smartwatch-status`
- `spark-analytics`
- `system-events`

Those topics are all example topics except for heart-rate-events, which is used for real-time heart rate monitoring.
They are created by k3s manifest files located in the `k3s` directory.

# Manifest structure

Kafka is built using 2 manifest files located in the `k3s` directory: one for the Kafka broker and one for the Kafka topics.

- `k3s/kafka.yaml`: defines the Kafka broker deployment and service.
- `k3s/kafka-topics.yaml`: defines the Kafka topics used in the project.
