#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
K8S_VERSION="${ENVTEST_K8S_VERSION:-1.31.0}"
BIN_DIR="${ROOT}/bin"
mkdir -p "${BIN_DIR}"

# Prefer an already-installed setup-envtest (GOPATH/bin), avoid broken local copies.
if [[ -x "${GOPATH:-$(go env GOPATH)}/bin/setup-envtest" ]]; then
  SETUP_ENVTEST="${GOPATH:-$(go env GOPATH)}/bin/setup-envtest"
elif command -v setup-envtest >/dev/null 2>&1; then
  SETUP_ENVTEST="$(command -v setup-envtest)"
else
  go install sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.19
  SETUP_ENVTEST="$(go env GOPATH)/bin/setup-envtest"
fi

# Drop a broken project-local binary if present (seen on some macOS toolchains).
if [[ -x "${BIN_DIR}/setup-envtest" ]] && ! "${BIN_DIR}/setup-envtest" --help >/dev/null 2>&1; then
  rm -f "${BIN_DIR}/setup-envtest"
fi

ASSETS="$("${SETUP_ENVTEST}" use "${K8S_VERSION}" --bin-dir "${BIN_DIR}" -p path)"
echo "${ASSETS}"
