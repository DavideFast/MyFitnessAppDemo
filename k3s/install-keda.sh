#!/bin/bash

set -eu

HELM_BIN="${HELM_BIN:-helm}"
KUBECTL_CMD="${KUBECTL_CMD:-sudo k3s kubectl}"
KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
KEDA_NAMESPACE="${KEDA_NAMESPACE:-keda}"
KEDA_RELEASE="${KEDA_RELEASE:-keda}"

HELM_CMD=("$HELM_BIN")
if [[ ! -r "$KUBECONFIG" ]]; then
    HELM_CMD=(sudo env "KUBECONFIG=$KUBECONFIG" "$HELM_BIN")
fi

if ! command -v "$HELM_BIN" >/dev/null 2>&1; then
    echo "Helm is required to install KEDA."
    echo "Install Helm on the K3s server and retry."
    exit 1
fi

echo "Installing KEDA..."
export KUBECONFIG
"${HELM_CMD[@]}" repo add --force-update kedacore https://kedacore.github.io/charts
"${HELM_CMD[@]}" upgrade --install "$KEDA_RELEASE" kedacore/keda \
    --namespace "$KEDA_NAMESPACE" \
    --create-namespace \
    --wait \
    --timeout 5m

$KUBECTL_CMD get crd scaledobjects.keda.sh >/dev/null
echo "KEDA is ready."
