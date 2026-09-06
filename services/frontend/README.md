# Frontend

React and Vite dashboard for MyFitnessAppDemo. It provides pages to test PostgreSQL and ClickHouse connections and to manage the smartwatch simulation.

## Requirements

- Node.js 20 or newer
- The backend API running locally or reachable from the browser

## Local start

```bash
cd services/frontend
npm ci
cp .env.example .env
npm run dev
```

Vite serves the application at `http://localhost:5173`. The configured development server listens on all interfaces, which is useful when opening the dashboard from another device on the local network.

## Configuration

The frontend uses Vite environment variables. Copy `.env.example` to `.env` and configure:

| Variable            | Purpose                                     | Default                 |
| ------------------- | ------------------------------------------- | ----------------------- |
| `VITE_API_BASE_URL` | Base URL of the backend API                 | `http://localhost:3001` |
| `VITE_EVENTS_PATH`  | Relative API path used for simulator events | `/events`               |

Variables prefixed with `VITE_` are embedded in the browser bundle. Do not place credentials or secrets in this file.

## Commands

```bash
npm run dev      # Start the Vite development server
npm run build    # Generate the production bundle in dist/
npm run preview  # Serve the production bundle locally
```

## Pages

- `/postgres`: PostgreSQL connection and data operations.
- `/clickhouse`: ClickHouse connection and analytical data operations.
- `/simulation`: Smartwatch simulator controls.

The root path redirects to `/postgres`.

## Container and K3s

The production image is a multi-stage build: Vite generates the static files and Nginx serves them. The K3s deployment in `k3s/04-frontend.yaml` runs two replicas behind the `frontend` ClusterIP service on port `5173`; its probes request `/`.

Because Vite variables are resolved during `npm run build`, set `VITE_API_BASE_URL` to the browser-reachable backend URL before building a production image. In the K3s deployment, browser access is normally provided by the ingress configuration in `k3s/06-ingress.yaml`.

## Useful checks

```bash
npm run build
npm run preview
```

In K3s:

```bash
sudo k3s kubectl get pods -n bigintensive -l app=frontend
sudo k3s kubectl logs -n bigintensive deployment/frontend
```
