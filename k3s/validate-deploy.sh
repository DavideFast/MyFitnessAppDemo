#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${NAMESPACE:-bigintensive}"
KUBECTL_CMD="${KUBECTL_CMD:-sudo k3s kubectl}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-180s}"
DEPLOY_FIRST="false"

usage() {
  echo "Usage: bash k3s/validate-deploy.sh [--deploy-first]"
}

if [[ "${1:-}" == "--deploy-first" ]]; then
  DEPLOY_FIRST="true"
elif [[ -n "${1:-}" ]]; then
  usage
  exit 2
fi

PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0

pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  echo "[PASS] $1"
}

warn() {
  WARN_COUNT=$((WARN_COUNT + 1))
  echo "[WARN] $1"
}

fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "[FAIL] $1"
}

check_namespace() {
  if $KUBECTL_CMD get namespace "$NAMESPACE" >/dev/null 2>&1; then
    pass "Namespace '$NAMESPACE' exists"
  else
    fail "Namespace '$NAMESPACE' not found"
  fi
}

check_rollout() {
  local deployment="$1"
  if ! $KUBECTL_CMD get deployment "$deployment" -n "$NAMESPACE" >/dev/null 2>&1; then
    fail "Deployment '$deployment' not found"
    return
  fi

  if $KUBECTL_CMD rollout status "deployment/$deployment" -n "$NAMESPACE" --timeout="$ROLLOUT_TIMEOUT" >/dev/null 2>&1; then
    pass "Deployment '$deployment' rollout completed"
  else
    fail "Deployment '$deployment' rollout failed or timed out"
  fi
}

check_service() {
  local service="$1"
  if $KUBECTL_CMD get svc "$service" -n "$NAMESPACE" >/dev/null 2>&1; then
    pass "Service '$service' exists"
  else
    fail "Service '$service' not found"
  fi
}

check_endpoint_ready() {
  local service="$1"
  if ! $KUBECTL_CMD get endpoints "$service" -n "$NAMESPACE" >/dev/null 2>&1; then
    fail "Endpoints for service '$service' not found"
    return
  fi

  local addresses
  addresses="$($KUBECTL_CMD get endpoints "$service" -n "$NAMESPACE" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null || true)"

  if [[ -n "${addresses// }" ]]; then
    pass "Service '$service' has ready endpoints"
  else
    fail "Service '$service' has no ready endpoints"
  fi
}

check_no_bad_pods() {
  local bad
  bad="$($KUBECTL_CMD get pods -n "$NAMESPACE" --no-headers 2>/dev/null | awk '$3 ~ /CrashLoopBackOff|ImagePullBackOff|ErrImagePull|CreateContainerConfigError|CreateContainerError|Error/ {print $1 " (" $3 ")"}')"

  if [[ -z "$bad" ]]; then
    pass "No pods in failing states"
  else
    fail "Found pods in failing states: $bad"
  fi
}

check_ingress() {
  if $KUBECTL_CMD get ingress bigintensive-web -n "$NAMESPACE" >/dev/null 2>&1; then
    pass "Ingress 'bigintensive-web' exists"
  else
    fail "Ingress 'bigintensive-web' not found"
  fi
}

print_summary() {
  echo
  echo "=== BigIntensive Deploy Validation Summary ==="
  echo "PASS: $PASS_COUNT"
  echo "WARN: $WARN_COUNT"
  echo "FAIL: $FAIL_COUNT"

  if [[ "$FAIL_COUNT" -gt 0 ]]; then
    echo "Validation failed"
    exit 1
  fi

  echo "Validation successful"
  exit 0
}

run_deploy_if_requested() {
  if [[ "$DEPLOY_FIRST" != "true" ]]; then
    return
  fi

  echo "Running deploy-all before validation"
  bash "$SCRIPT_DIR/deploy-all.sh"
  pass "Deploy completed successfully"
}

echo "Running deployment validation for namespace '$NAMESPACE'"

run_deploy_if_requested

check_namespace
check_rollout backend
check_rollout frontend
check_service backend
check_service frontend
check_service kafka
check_service kafka-ui
check_service postgres
check_endpoint_ready backend
check_endpoint_ready frontend
check_no_bad_pods
check_ingress

print_summary
