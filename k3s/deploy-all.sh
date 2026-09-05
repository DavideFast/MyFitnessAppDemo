#!/bin/bash

# BigIntensive k3s Deployment Script
# This script won't build Docker images; it assumes that the backend and frontend images are already built and available in the local Docker registry.
# Applies all YAML manifests in the correct order

set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBECTL_CMD="${KUBECTL_CMD:-sudo k3s kubectl}"
NAMESPACE="${NAMESPACE:-bigintensive}"
RESET_NAMESPACE="${RESET_NAMESPACE:-false}"

echo "🚀 BigIntensive k3s Deployment"
echo "========================================"

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# Function to apply a manifest with progress indication
apply_manifest() {
    local file="$1"
    local description="$2"
    
    if [ ! -f "$file" ]; then
        echo "❌ File not found: $file"
        exit 1
    fi
    
    echo -e "${BLUE}Applying: $description${NC}"
    $KUBECTL_CMD apply -f "$file"
    echo -e "${GREEN}✓ $description${NC}\n"
}

pre_deploy_cleanup() {
    echo -e "${BLUE}Pre-deploy cleanup${NC}"

    if [ "$RESET_NAMESPACE" = "true" ]; then
        echo "RESET_NAMESPACE=true -> deleting namespace $NAMESPACE"
        $KUBECTL_CMD delete namespace "$NAMESPACE" --ignore-not-found=true
        while $KUBECTL_CMD get namespace "$NAMESPACE" >/dev/null 2>&1; do
            echo "waiting namespace delete..."
            sleep 2
        done
        echo -e "${GREEN}✓ Namespace cleanup completed${NC}\n"
        return
    fi

    if ! $KUBECTL_CMD get namespace "$NAMESPACE" >/dev/null 2>&1; then
        echo "Namespace $NAMESPACE not found, skipping cleanup"
        echo -e "${GREEN}✓ Cleanup completed${NC}\n"
        return
    fi

    # Job template is immutable; remove legacy job before re-applying 07-clickhouse.yaml
    $KUBECTL_CMD delete job clickhouse-init -n "$NAMESPACE" --ignore-not-found=true >/dev/null 2>&1 || true

    # Remove the previous single-StatefulSet topology before applying the two-shard topology.
    # ClickHouse data is disposable in this development deployment.
    $KUBECTL_CMD delete statefulset clickhouse -n "$NAMESPACE" --ignore-not-found=true >/dev/null 2>&1 || true
    for pvc in data-clickhouse-0 data-clickhouse-1 data-clickhouse-2 data-clickhouse-3; do
        $KUBECTL_CMD delete pvc "$pvc" -n "$NAMESPACE" --ignore-not-found=true >/dev/null 2>&1 || true
    done

    # Remove stale ClickHouse pods so StatefulSets can recreate them with fresh config
    $KUBECTL_CMD delete pod -n "$NAMESPACE" -l app=clickhouse --ignore-not-found=true >/dev/null 2>&1 || true
    $KUBECTL_CMD delete pod -n "$NAMESPACE" -l app=clickhouse-keeper --ignore-not-found=true >/dev/null 2>&1 || true
    $KUBECTL_CMD delete pod -n "$NAMESPACE" -l job-name=clickhouse-init --ignore-not-found=true >/dev/null 2>&1 || true

    echo -e "${GREEN}✓ Cleanup completed${NC}\n"
}

pre_deploy_cleanup

# Create the application namespace before operators create namespace-scoped resources in it.
apply_manifest "$SCRIPT_DIR/00-namespace-and-secrets.yaml" "Namespace, Secrets & ConfigMaps"

# The operator installs cluster-wide CRDs and watches SparkApplication resources.
bash "$SCRIPT_DIR/install-spark-operator.sh"
bash "$SCRIPT_DIR/install-keda.sh"

# Apply in order
$KUBECTL_CMD -n "$NAMESPACE" create configmap postgresql-schema \
    --from-file=schema.sql="$SCRIPT_DIR/../database/postgresql/schema.sql" \
    --dry-run=client -o yaml | $KUBECTL_CMD apply -f -

apply_manifest "$SCRIPT_DIR/01-postgresql.yaml" "PostgreSQL Database"
apply_manifest "$SCRIPT_DIR/11-generate-postgresql-samples.yaml" "PostgreSQL Sample Generator"
apply_manifest "$SCRIPT_DIR/02-kafka.yaml" "Kafka & Kafka UI"
apply_manifest "$SCRIPT_DIR/02b-kafka-topics.yaml" "Kafka Topic Bootstrap Job"
apply_manifest "$SCRIPT_DIR/03-backend.yaml" "Backend"
apply_manifest "$SCRIPT_DIR/04-frontend.yaml" "Frontend"
apply_manifest "$SCRIPT_DIR/05-spark-and-jupyter.yaml" "Spark & Jupyter"

# The analysis source is mounted into the submitter Job as a ConfigMap.
$KUBECTL_CMD -n "$NAMESPACE" create configmap running-population-job-script \
    --from-file=RunningPopolationAnalysis.py="$SCRIPT_DIR/../data_processing/spark/jobs/RunningPopolationAnalysis.py" \
    --from-file=config.py="$SCRIPT_DIR/../data_processing/spark/jobs/config.py" \
    --dry-run=client -o yaml | $KUBECTL_CMD apply -f -

apply_manifest "$SCRIPT_DIR/06-ingress.yaml" "Ingress Routes"

$KUBECTL_CMD -n "$NAMESPACE" create configmap clickhouse-schema \
    --from-file=schema.sql="$SCRIPT_DIR/../database/clickhouse/schema.sql" \
    --dry-run=client -o yaml | $KUBECTL_CMD apply -f -

apply_manifest "$SCRIPT_DIR/07-clickhouse.yaml" "ClickHouse & ClickHouse Keeper"
apply_manifest "$SCRIPT_DIR/08-kafka-consumer.yaml" "Kafka Consumer"
apply_manifest "$SCRIPT_DIR/09-elt-copy-workout.yaml" "PostgreSQL to ClickHouse ELT"
apply_manifest "$SCRIPT_DIR/10-smartwatch-simulator.yaml" "Smartwatch Simulator"

echo -e "${GREEN}========================================"
echo "✓ All resources deployed successfully!"
echo "========================================${NC}"

echo ""
echo "📋 Useful commands:"
echo "   Check all pods:     $KUBECTL_CMD get pods -n $NAMESPACE"
echo "   Watch pods:         $KUBECTL_CMD get pods -n $NAMESPACE -w"
echo "   Describe pod:       $KUBECTL_CMD describe pod <pod-name> -n $NAMESPACE"
echo "   View logs:          $KUBECTL_CMD logs <pod-name> -n $NAMESPACE"
echo ""
echo "🌐 Access URLs (add to /etc/hosts if needed):"
echo "   Frontend:  http://bigintensive.local or http://192.168.x.x"
echo "   Backend:   http://api.bigintensive.local"
echo "   Kafka UI:  http://kafka-ui.bigintensive.local"
echo "   Jupyter:   http://jupyter.bigintensive.local (token: changeme)"
echo ""
echo "⚙️  Options:"
echo "   RESET_NAMESPACE=true ./k3s/deploy-all.sh   # full clean start"
