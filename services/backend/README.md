# Backend

Express API for the MyFitnessAppDemo dashboard. It reads and writes workout data in PostgreSQL, reads analytical data from ClickHouse, publishes simulator events to Kafka, and coordinates selected Kubernetes resources used by the demo.

## Requirements

- Node.js 20 or newer
- PostgreSQL available with the schema from `database/postgresql/schema.sql`
- ClickHouse and Kafka when using the related features
- A Kubernetes configuration only for endpoints that control in-cluster resources

## Local start

```bash
cd services/backend
npm ci
cp .env.example .env
npm run dev
```

The API listens on `http://localhost:3001` by default. Check that it is running with:

```bash
curl http://localhost:3001/health
```

For a non-watching process, use `npm start`. The command `npm run seed:fake` loads the included fake data through the scripts folder.

## Configuration

Copy `.env.example` to `.env` and provide the services that are running locally. The main settings are:

| Variable                                            | Purpose                                  | Local default                  |
| --------------------------------------------------- | ---------------------------------------- | ------------------------------ |
| `PORT`                                              | HTTP listening port                      | `3001`                         |
| `CORS_ORIGIN`                                       | Allowed frontend origins                 | `http://localhost:5173`        |
| `POSTGRES_HOST`, `POSTGRES_PORT`                    | PostgreSQL connection address            | `localhost:5432`               |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | PostgreSQL credentials and database      | See `.env.example`             |
| `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`                | ClickHouse connection address            | Set for the target environment |
| `KAFKA_BOOTSTRAP_SERVERS`                           | Kafka broker list                        | `localhost:9094`               |
| `KUBERNETES_NAMESPACE`                              | Namespace used for Kubernetes actions    | `bigintensive`                 |
| `PYTHON_BIN`                                        | Optional absolute Python executable path | Empty                          |

The `.env` file can contain passwords and must remain untracked.

## Health endpoint

`GET /health` returns the service state, name, and timestamp. Kubernetes uses this endpoint for the readiness and liveness probes.

## Container and K3s

The `Dockerfile` builds the runtime image from Node.js 20 and includes Python plus the scripts dependencies required by backend workflows. The K3s deployment is defined in `k3s/03-backend.yaml` and runs two replicas behind the `backend` ClusterIP service on port `3001`.

The deployment imports application settings from the `bigintensive-config` ConfigMap and sensitive values from the `bigintensive-secrets` Secret. It runs with the `backend-controller` service account, whose RBAC permissions allow it to manage only the demo resources declared in the manifest.

## Useful checks

```bash
npm start
curl http://localhost:3001/health
```

In K3s:

```bash
sudo k3s kubectl get pods -n bigintensive -l app=backend
sudo k3s kubectl logs -n bigintensive deployment/backend
```
