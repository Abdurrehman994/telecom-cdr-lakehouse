#!/usr/bin/env bash
# Bring the data platform up on a local Kubernetes cluster (kind).
#
#   1. a kind cluster
#   2. the Strimzi operator, and Kafka with its topics (k8s/kafka.yaml)
#   3. the platform's images, built and loaded into the cluster
#   4. the Helm chart (k8s/chart): storage, catalog, contracts, sinks, maintenance, workload
#
# Needs docker, kind, kubectl and helm. Run from fraud-detection-system/:
#   bash k8s/up.sh
# The Compose stack and the cluster together need more memory than a laptop's Docker
# usually has; stop the Compose stack first (docker compose -f docker-compose.system.yml stop).
set -euo pipefail
export MSYS_NO_PATHCONV=1

CLUSTER=data-platform
NAMESPACE=data-platform
STRIMZI_VERSION=1.3.0
cd "$(dirname "$0")/.."

echo "== cluster"
if ! kind get clusters | grep -qx "$CLUSTER"; then
    kind create cluster --config k8s/kind-cluster.yaml
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

echo "== Strimzi $STRIMZI_VERSION and Kafka"
# the release manifest installs into "myproject"; point it at our namespace
curl -fsSL "https://github.com/strimzi/strimzi-kafka-operator/releases/download/$STRIMZI_VERSION/strimzi-cluster-operator-$STRIMZI_VERSION.yaml" \
    | sed "s/namespace: myproject/namespace: $NAMESPACE/" \
    | kubectl apply -n "$NAMESPACE" --server-side --force-conflicts -f - >/dev/null
kubectl -n "$NAMESPACE" rollout status deployment/strimzi-cluster-operator --timeout=300s
kubectl -n "$NAMESPACE" apply -f k8s/kafka.yaml

echo "== images"
docker compose -f docker-compose.system.yml build lakehouse-writer cdr-ingestion ml-service
docker tag fraud-detection-system-lakehouse-writer:latest data-platform/lakehouse:dev
docker tag fraud-detection-system-cdr-ingestion:latest data-platform/cdr-ingestion:dev
docker tag fraud-detection-system-ml-service:latest data-platform/ml-service:dev
# only the images built here; the cluster pulls public images from their registries
# (kind load also cannot import multi-platform images from Docker Desktop's image store)
for image in data-platform/lakehouse:dev data-platform/cdr-ingestion:dev data-platform/ml-service:dev; do
    kind load docker-image --name "$CLUSTER" "$image"
done

echo "== Kafka ready"
kubectl -n "$NAMESPACE" wait kafka/platform --for=condition=Ready --timeout=900s

echo "== platform"
kubectl -n "$NAMESPACE" create configmap alerts-schema --from-file=postgres/init \
    --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install platform k8s/chart -n "$NAMESPACE" --wait --timeout 15m

kubectl -n "$NAMESPACE" get pods
cat <<EOF

The platform is up. To use it from this machine:
  kubectl -n $NAMESPACE port-forward svc/cdr-ingestion 8080:8080    # send CDRs (ingest-auto.py)
  kubectl -n $NAMESPACE port-forward svc/lakekeeper 8181:8181       # catalog UI at /ui
Remove the cluster with: kind delete cluster --name $CLUSTER
EOF
