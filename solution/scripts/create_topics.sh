#!/usr/bin/env bash
set -euo pipefail

# NOTE:
# Topic creation depends on your Kafka setup and permissions.
# In Yandex Managed Kafka you can create topics via UI/CLI/API (or kafka-admin tools if allowed).
#
# Use these topic names (must match your env/config):
# - STG -> DDS input:  ${KAFKA_STG_SERVICE_ORDERS_TOPIC:-stg-service-orders}
# - DDS -> CDM input:  ${KAFKA_DDS_SERVICE_ORDERS_TOPIC:-dds-service-orders}
#
# This script is a placeholder so you don't forget to create the topics.

echo "Create topics in your Kafka cluster:"
echo "  - ${KAFKA_STG_SERVICE_ORDERS_TOPIC:-stg-service-orders}"
echo "  - ${KAFKA_DDS_SERVICE_ORDERS_TOPIC:-dds-service-orders}"
echo
echo "Then deploy services."

