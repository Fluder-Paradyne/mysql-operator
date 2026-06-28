#!/usr/bin/env bash
# Create (or reuse) a local kind cluster and run MySQL operator e2e tests.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="${KIND_CLUSTER_NAME:-mysql-operator-e2e}"
KIND_IMAGE="${KIND_IMAGE:-kindest/node:v1.31.0}"

if ! command -v kind >/dev/null 2>&1; then
  echo "kind is required (https://kind.sigs.k8s.io/)" >&2
  exit 1
fi
if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required" >&2
  exit 1
fi

if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  echo "Creating kind cluster ${CLUSTER_NAME}..."
  kind create cluster --name "${CLUSTER_NAME}" --image "${KIND_IMAGE}"
else
  echo "Reusing kind cluster ${CLUSTER_NAME}"
  kind export kubeconfig --name "${CLUSTER_NAME}" >/dev/null
fi

export KUBECONFIG="${KUBECONFIG:-${HOME}/.kube/config}"
kubectl config use-context "kind-${CLUSTER_NAME}" >/dev/null

echo "Installing CRD..."
kubectl apply -f "${ROOT}/config/crd/mysql.asrk.dev_mysqls.yaml"

echo "Running e2e tests..."
cd "${ROOT}"
go test ./test/e2e/ -tags=e2e -count=1 -timeout=25m -v
