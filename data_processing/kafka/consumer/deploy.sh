#!/bin/bash
# Deploy script for Kafka Consumer Real-Time

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONSUMER_DIR="$SCRIPT_DIR/kafka-consumer-real-time"
DOCKER_DIR="$SCRIPT_DIR"
K3S_DIR="$SCRIPT_DIR/../../../k3s"

echo "🚀 BigIntensive Kafka Consumer Deployment"
echo "=========================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Maven is available
if ! command -v mvn &> /dev/null; then
    echo -e "${RED}❌ Maven is not installed${NC}"
    echo "Please install Maven"
    exit 1
fi

# Parse arguments
DEPLOY_K3S=false
PUSH_REGISTRY=false
REGISTRY="davidefast"

while [[ $# -gt 0 ]]; do
    case $1 in
        --k3s)
            DEPLOY_K3S=true
            shift
            ;;
        --push)
            PUSH_REGISTRY=true
            REGISTRY="${2:-davidefast}"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Build JAR
echo -e "${YELLOW}📦 Building Kafka Consumer with Maven...${NC}"
cd "$CONSUMER_DIR"
mvn clean package -DskipTests -q
echo -e "${GREEN}✅ JAR built successfully${NC}"
echo ""

# Build Docker image
if [ "$DEPLOY_K3S" = true ]; then
    echo -e "${YELLOW}🐳 Building Docker image...${NC}"
    cd "$DOCKER_DIR"
    docker build -t consumer-kafka:latest .
    echo -e "${GREEN}✅ Docker image built successfully${NC}"
    echo ""
    
    if [ "$PUSH_REGISTRY" = true ]; then
        echo -e "${YELLOW}📤 Pushing to registry: $REGISTRY${NC}"
        docker tag consumer-kafka:latest $REGISTRY/consumer-kafka:latest
        docker push $REGISTRY/consumer-kafka:latest
        echo -e "${GREEN}✅ Image pushed to $REGISTRY/consumer-kafka:latest${NC}"
        echo ""
    fi
fi

# Deploy to K3s
if [ "$DEPLOY_K3S" = true ]; then
    echo -e "${YELLOW}☸️  Deploying to K3s...${NC}"
    
    # Check if kubectl is available
    if ! command -v kubectl &> /dev/null && ! command -v k3s &> /dev/null; then
        echo -e "${RED}❌ kubectl or k3s not found${NC}"
        exit 1
    fi
    
    # Use k3s kubectl if available
    KUBECTL_CMD="kubectl"
    if command -v k3s &> /dev/null; then
        KUBECTL_CMD="k3s kubectl"
    fi
    
    $KUBECTL_CMD apply -f "$K3S_DIR/08-kafka-consumer.yaml"
    echo -e "${GREEN}✅ Kafka Consumer deployed to K3s${NC}"
    echo ""
    
    echo "Check deployment status with:"
    echo "  $KUBECTL_CMD get deployment kafka-consumer-realtime -n bigintensive"
    echo ""
    echo "View logs with:"
    echo "  $KUBECTL_CMD logs -f deployment/kafka-consumer-realtime -n bigintensive"
fi

# Summary
echo ""
echo -e "${GREEN}✅ Deployment complete!${NC}"
echo ""
echo "Configuration:"
echo "  - Topic: heart-rate-events"
echo "  - Kafka Bootstrap: kafka:19092"
echo "  - ClickHouse: clickhouse:8123/bigintensive"
echo "  - PostgreSQL: postgres:5432/bigintensive"
echo ""

# Next steps
if [ "$DEPLOY_K3S" = false ]; then
    echo "Next steps:"
    echo "1. K3s: ./deploy.sh --k3s"
    echo "2. Build and push image: ./deploy.sh --k3s --push davidefast"
fi
