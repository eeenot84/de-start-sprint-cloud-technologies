.PHONY: build-dds build-cdm build push-dds push-cdm push k8s-apply k8s-delete

REGISTRY ?= cr.yandex/crpj432mugle66u5vj6k
REPO ?= test
TAG ?= latest

DDS_IMAGE := $(REGISTRY)/$(REPO)/dds-service:$(TAG)
CDM_IMAGE := $(REGISTRY)/$(REPO)/cdm-service:$(TAG)

NAMESPACE ?= sprint9

build-dds:
	docker build -t $(DDS_IMAGE) -f service_dds/Dockerfile service_dds

build-cdm:
	docker build -t $(CDM_IMAGE) -f service_cdm/Dockerfile service_cdm

build: build-dds build-cdm

push-dds:
	# Build and push for YC nodes (amd64). On Apple Silicon host default is arm64.
	docker buildx build --platform linux/amd64 -t $(DDS_IMAGE) -f service_dds/Dockerfile --push service_dds

push-cdm:
	# Build and push for YC nodes (amd64). On Apple Silicon host default is arm64.
	docker buildx build --platform linux/amd64 -t $(CDM_IMAGE) -f service_cdm/Dockerfile --push service_cdm

push: push-dds push-cdm

k8s-apply:
	kubectl -n $(NAMESPACE) apply -f k8s/00-configmap.yaml
	kubectl -n $(NAMESPACE) apply -f k8s/01-secrets.yaml
	kubectl -n $(NAMESPACE) apply -f k8s/10-dds-deployment.yaml
	kubectl -n $(NAMESPACE) apply -f k8s/20-cdm-deployment.yaml

k8s-delete:
	kubectl -n $(NAMESPACE) delete -f k8s/20-cdm-deployment.yaml --ignore-not-found
	kubectl -n $(NAMESPACE) delete -f k8s/10-dds-deployment.yaml --ignore-not-found
	kubectl -n $(NAMESPACE) delete -f k8s/01-secrets.yaml --ignore-not-found
	kubectl -n $(NAMESPACE) delete -f k8s/00-configmap.yaml --ignore-not-found

