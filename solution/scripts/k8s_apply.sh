#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-sprint9}"

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "${NAMESPACE}" apply -f k8s/00-configmap.yaml
kubectl -n "${NAMESPACE}" apply -f k8s/01-secrets.yaml
kubectl -n "${NAMESPACE}" apply -f k8s/10-dds-deployment.yaml
kubectl -n "${NAMESPACE}" apply -f k8s/20-cdm-deployment.yaml

echo "Applied. Check pods:"
echo "  kubectl -n ${NAMESPACE} get pods"

