# BigIntensive k3s Manifest Appendix

Primary runbook: [README.md](README.md)

This document is a technical appendix for the current modular Kubernetes manifests.

## Manifest map

```text
k3s/
├── 00-namespace-and-secrets.yaml
├── 01-postgresql.yaml
├── 02-kafka.yaml
├── 02b-kafka-topics.yaml
├── 03-backend.yaml
├── 04-frontend.yaml
├── 05-spark-and-jupyter.yaml
├── 06-ingress.yaml
├── 07-clickhouse.yaml
├── 08-kafka-consumer.yaml
├── 09-elt-copy-workout.yaml
├── 10-smartwatch-simulator.yaml
├── 11-generate-postgresql-samples.yaml
├── deploy-all.sh
└── validate-deploy.sh
```

## Ownership by file

- `00-namespace-and-secrets.yaml`
  - Namespace, secrets and shared application ConfigMap.
- `01-postgresql.yaml`
  - PostgreSQL service, StatefulSet and persistent volume. The schema ConfigMap is generated from `database/postgresql/schema.sql` by `deploy-all.sh`.
- `02-kafka.yaml`
  - Three-node Kafka KRaft cluster (broker + controller on each node) and Kafka UI.
- `03-backend.yaml`
  - Backend deployment/service, probes, app env wiring.
- `04-frontend.yaml`
  - Frontend deployment/service and API base URL env.
- `05-spark-and-jupyter.yaml`
  - Spark RBAC and Jupyter runtime for notebooks.
- `06-ingress.yaml`
  - Traefik ingress routes for frontend, API, Kafka UI, Jupyter.
- `07-clickhouse.yaml`
  - Three-node ClickHouse Keeper quorum.
  - Four ClickHouse servers arranged as two shards with two replicas each.
  - Replicated local tables and Distributed tables using `cityHash64(athlete_id)`.
  - The schema ConfigMap is generated from `database/clickhouse/schema.sql` by `deploy-all.sh`.
- `08-kafka-consumer.yaml`
  - Kafka Consumer Real-Time Analysis (Kafka Streams application).
  - ConfigMap with environment variables.
  - StatefulSet with 2 minimum and 6 maximum replicas, scaled by KEDA from Kafka consumer lag.
  - Service for metrics exposure.
  - Liveness and readiness probes.
- `09-elt-copy-workout.yaml`
  - Hourly CronJob that copies PostgreSQL workouts to ClickHouse staging.
  - Executes the ClickHouse transformation from `database/clickhouse/script.sql`.

## Apply order

The expected order is fixed and implemented by `deploy-all.sh`:

1. `00-namespace-and-secrets.yaml`
2. `01-postgresql.yaml`
3. `11-generate-postgresql-samples.yaml`
4. `02-kafka.yaml`
5. `02b-kafka-topics.yaml`
6. `03-backend.yaml`
7. `04-frontend.yaml`
8. `05-spark-and-jupyter.yaml`
9. `06-ingress.yaml`
10. `07-clickhouse.yaml`
11. `08-kafka-consumer.yaml` (requires Kafka, PostgreSQL, ClickHouse ready)
12. `09-elt-copy-workout.yaml` (requires PostgreSQL and ClickHouse ready)
13. `10-smartwatch-simulator.yaml`

Kafka topic replication is configured for three brokers with `min.insync.replicas=2`.
The topic bootstrap Job does not change the replication factor of topics that already
exist. On an existing cluster, migrate their assignments explicitly; for a clean
development rebuild, use `RESET_NAMESPACE=true` only when deleting existing data is
acceptable.

## Advanced operations

### Deploy all with cleanup strategy

```bash
# normal deploy
bash k3s/deploy-all.sh

# clean start (recreate namespace)
RESET_NAMESPACE=true bash k3s/deploy-all.sh
```

### Component-only deploy

```bash
# example: update only backend
sudo k3s kubectl apply -f k3s/03-backend.yaml
sudo k3s kubectl rollout restart deployment/backend -n bigintensive
```

### Runtime checks

```bash
sudo k3s kubectl get pods -n bigintensive -w
sudo k3s kubectl get svc -n bigintensive
sudo k3s kubectl get ingress -n bigintensive
sudo k3s kubectl logs -f deployment/backend -n bigintensive
```

## Change checklist

Before changing any manifest, verify:

1. Service name and deployment name stay aligned (`backend`, `frontend`).
2. Ingress backend service names match actual services.
3. Environment variable names are consistent with app code.
4. Image tags used in manifests exist in the target runtime.
5. If node labels/selectors are used, target nodes are labeled.
