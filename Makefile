IMG ?= mysql-operator:latest
ENVTEST_K8S_VERSION ?= 1.31.0
KIND_CLUSTER_NAME ?= mysql-operator-e2e

.PHONY: build run docker-build install deploy undeploy sample tidy \
	test test-unit test-integration test-e2e test-e2e-kind envtest

build:
	go build -o bin/manager cmd/main.go

run: build
	./bin/manager

tidy:
	go mod tidy

docker-build:
	docker build -t $(IMG) .

install:
	kubectl apply -f config/crd/mysql.asrk.dev_mysqls.yaml
	kubectl apply -f config/crd/mysql.asrk.dev_mysqlbackups.yaml

deploy: install
	kubectl apply -f config/manager/namespace.yaml
	kubectl apply -f config/rbac/service_account.yaml
	kubectl apply -f config/rbac/role.yaml
	kubectl apply -f config/rbac/role_binding.yaml
	kubectl apply -f config/manager/manager.yaml

undeploy:
	-kubectl delete -f config/samples/mysql_v1alpha1_mysql.yaml --ignore-not-found
	-kubectl delete -f config/manager/manager.yaml --ignore-not-found
	-kubectl delete -f config/rbac/role_binding.yaml --ignore-not-found
	-kubectl delete -f config/rbac/role.yaml --ignore-not-found
	-kubectl delete -f config/rbac/service_account.yaml --ignore-not-found
	-kubectl delete -f config/manager/namespace.yaml --ignore-not-found
	-kubectl delete -f config/crd/mysql.asrk.dev_mysqlbackups.yaml --ignore-not-found
	-kubectl delete -f config/crd/mysql.asrk.dev_mysqls.yaml --ignore-not-found

sample:
	kubectl apply -f config/samples/mysql_v1alpha1_mysql.yaml

# Fast tests only (no envtest binaries, no cluster).
test-unit:
	go test ./api/... -count=1

# Controller integration tests using envtest (API server + etcd, no real pods).
envtest:
	@echo "KUBEBUILDER_ASSETS=$$(./hack/setup-envtest.sh)"

test-integration: envtest
	KUBEBUILDER_ASSETS="$$(./hack/setup-envtest.sh)" go test ./internal/controller/ -count=1 -timeout=5m -v

# Real-cluster e2e: standalone + HA replication + automatic failover.
# Requires kubeconfig with a working cluster and the CRD installed (`make install`).
test-e2e: install
	go test ./test/e2e/ -tags=e2e -count=1 -timeout=25m -v

# Failover-only e2e (faster feedback while iterating on promotion logic).
test-e2e-failover: install
	go test ./test/e2e/ -tags=e2e -count=1 -timeout=15m -run 'TestMySQLAutomaticFailover' -v

# Spins up kind (if needed) and runs e2e — fully local, no pre-existing cluster required
# beyond Docker + kind + kubectl.
test-e2e-kind:
	KIND_CLUSTER_NAME=$(KIND_CLUSTER_NAME) ./hack/kind-e2e.sh

# Default: unit + envtest integration (no Docker/kind required after first envtest download).
test: test-unit test-integration
