#!/bin/bash
# Submit Spark Job: Running Population Analysis
# Questo script invia il job al cluster Spark tramite spark-submit

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JOB_DIR="$SCRIPT_DIR/../spark/jobs"

echo "🚀 Submitting Spark Job: Running Population Analysis"
echo "===================================================="
echo ""

# Configurazione Spark
SPARK_MASTER=${SPARK_MASTER:-"k8s://https://kubernetes.default:443"}
SPARK_NAMESPACE=${SPARK_NAMESPACE:-"bigintensive"}
SPARK_SERVICE_ACCOUNT=${SPARK_SERVICE_ACCOUNT:-"spark"}
SPARK_CONTAINER_IMAGE=${SPARK_CONTAINER_IMAGE:-"apache/spark:3.5.3"}
SPARK_DRIVER_HOST=${SPARK_DRIVER_HOST:-"$(hostname -i | awk '{print $1}')"}
SPARK_DRIVER_PORT=${SPARK_DRIVER_PORT:-"4040"}
SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-"2g"}
SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-"2g"}
SPARK_EXECUTOR_CORES=${SPARK_EXECUTOR_CORES:-"2"}
SPARK_JARS_DIR=${SPARK_JARS_DIR:-"/opt/spark/jars"}

# Database Configuration
CLICKHOUSE_JDBC_URL=${CLICKHOUSE_JDBC_URL:-"jdbc:clickhouse://clickhouse:8123/bigintensive"}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-"default"}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-""}

POSTGRES_JDBC_URL=${POSTGRES_JDBC_URL:-"jdbc:postgresql://postgres:5432/bigintensive"}
POSTGRES_USER=${POSTGRES_USER:-"postgres"}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-"postgres"}

KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-"kafka:19092"}

echo "Configuration:"
echo "  Spark Master: $SPARK_MASTER"
echo "  Spark Namespace: $SPARK_NAMESPACE"
echo "  Driver Host: $SPARK_DRIVER_HOST:$SPARK_DRIVER_PORT"
echo "  Driver Memory: $SPARK_DRIVER_MEMORY"
echo "  Executor Memory: $SPARK_EXECUTOR_MEMORY"
echo "  JDBC jars: $SPARK_JARS_DIR"
echo ""
echo "Job: $JOB_DIR/RunningPopolationAnalysis.py"
echo ""

# Check if spark-submit is available
if ! command -v spark-submit &> /dev/null; then
    echo "❌ spark-submit not found in PATH"
    echo "Make sure Spark is installed and in your PATH"
    exit 1
fi

JDBC_JARS=""
for jar in \
    "$SPARK_JARS_DIR/postgresql-42.7.2.jar" \
    "$SPARK_JARS_DIR/clickhouse-jdbc-0.6.3-all.jar"; do
    if [ -f "$jar" ]; then
        if [ -n "$JDBC_JARS" ]; then
            JDBC_JARS="$JDBC_JARS,$jar"
        else
            JDBC_JARS="$jar"
        fi
    fi
done

if [ -z "$JDBC_JARS" ]; then
    echo "❌ JDBC drivers not found in $SPARK_JARS_DIR"
    echo "Set SPARK_JARS_DIR or run this script inside the Spark image."
    exit 1
fi

# Submit the job
spark-submit \
    --master "$SPARK_MASTER" \
    --deploy-mode client \
    --driver-memory "$SPARK_DRIVER_MEMORY" \
    --executor-memory "$SPARK_EXECUTOR_MEMORY" \
    --executor-cores "$SPARK_EXECUTOR_CORES" \
    --conf "spark.kubernetes.namespace=$SPARK_NAMESPACE" \
    --conf "spark.kubernetes.authenticate.driver.serviceAccountName=$SPARK_SERVICE_ACCOUNT" \
    --conf "spark.kubernetes.container.image=$SPARK_CONTAINER_IMAGE" \
    --conf "spark.driver.host=$SPARK_DRIVER_HOST" \
    --conf "spark.driver.port=$SPARK_DRIVER_PORT" \
    --conf "spark.sql.shuffle.partitions=200" \
    --jars "$JDBC_JARS" \
    --py-files "$JOB_DIR/config.py" \
    "$JOB_DIR/RunningPopolationAnalysis.py"

echo ""
echo "✅ Job submitted successfully!"
echo ""
echo "View logs:"
echo "  - Spark Web UI: http://$SPARK_DRIVER_HOST:4040"
echo "  - Application logs: spark-submit output"
