#!/bin/bash

set -eu

HELM_BIN="${HELM_BIN:-helm}"
KUBECTL_CMD="${KUBECTL_CMD:-sudo k3s kubectl}"
KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
ARGO_NAMESPACE="${ARGO_NAMESPACE:-argo}"
ARGO_RELEASE="${ARGO_RELEASE:-argo-workflows}"
ARGO_VERSION="${ARGO_VERSION:-0.45.21}"

HELM_CMD=("$HELM_BIN")
if [[ ! -r "$KUBECONFIG" ]]; then
    HELM_CMD=(sudo env "KUBECONFIG=$KUBECONFIG" "$HELM_BIN")
fi

if ! command -v "$HELM_BIN" >/dev/null 2>&1; then
    echo "Helm is required to install Argo Workflows."
    echo "Install Helm on the K3s server and retry."
    exit 1
fi

echo "Installing Argo Workflows chart ${ARGO_VERSION}..."
export KUBECONFIG
"${HELM_CMD[@]}" repo add --force-update argo https://argoproj.github.io/argo-helm
"${HELM_CMD[@]}" upgrade --install "$ARGO_RELEASE" argo/argo-workflows \
    --namespace "$ARGO_NAMESPACE" \
    --create-namespace \
    --version "$ARGO_VERSION" \
    --set server.enabled=true \
    --set controller.workflowNamespaces[0]=bigintensive \
    --wait \
    --timeout 8m

$KUBECTL_CMD get crd workflows.argoproj.io >/dev/null
$KUBECTL_CMD get crd cronworkflows.argoproj.io >/dev/null

echo "Argo Workflows is ready."
