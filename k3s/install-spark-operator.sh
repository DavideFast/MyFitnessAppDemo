#!/bin/bash

set -eu

HELM_BIN="${HELM_BIN:-helm}"
KUBECTL_CMD="${KUBECTL_CMD:-sudo k3s kubectl}"
KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-spark-operator}"
OPERATOR_RELEASE="${OPERATOR_RELEASE:-spark-operator}"
OPERATOR_VERSION="${OPERATOR_VERSION:-2.5.2}"

HELM_CMD=("$HELM_BIN")
if [[ ! -r "$KUBECONFIG" ]]; then
    HELM_CMD=(sudo env "KUBECONFIG=$KUBECONFIG" "$HELM_BIN")
fi

if ! command -v "$HELM_BIN" >/dev/null 2>&1; then
    echo "Helm is required to install the Spark Operator."
    echo "Install Helm on the K3s server and retry."
    exit 1
fi

echo "Installing Spark Operator ${OPERATOR_VERSION}..."
export KUBECONFIG
"${HELM_CMD[@]}" repo add --force-update spark-operator https://kubeflow.github.io/spark-operator
"${HELM_CMD[@]}" upgrade --install "$OPERATOR_RELEASE" spark-operator/spark-operator \
    --namespace "$OPERATOR_NAMESPACE" \
    --create-namespace \
    --version "$OPERATOR_VERSION" \
    --set 'spark.jobNamespaces[0]=bigintensive' \
    --wait \
    --timeout 5m

$KUBECTL_CMD get crd sparkapplications.sparkoperator.k8s.io >/dev/null
echo "Spark Operator is ready."
echo "It will run SparkApplication resources only when they are created."