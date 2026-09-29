#!/bin/bash
set -euo pipefail

BROKERS="${REDPANDA_BROKERS:-redpanda:9092}"
ADMIN_API="${REDPANDA_ADMIN_API:-redpanda:9644}"

echo "Waiting for Redpanda broker at ${BROKERS}..."
until rpk cluster info --brokers "${BROKERS}" >/dev/null 2>&1; do
  echo "  not ready yet, retrying in 2s..."
  sleep 2
done
echo "Redpanda broker is ready."

# These two cluster defaults silently pull in Enterprise-gated features
# (continuous core/partition rebalancing). Force them to their Community-safe
# values so the cluster never actually consumes the built-in trial license.
echo "Ensuring cluster stays on Community-only features..."
rpk cluster config set core_balancing_continuous false -X "admin.hosts=${ADMIN_API}" --no-confirm >/dev/null
rpk cluster config set partition_autobalancing_mode node_add -X "admin.hosts=${ADMIN_API}" --no-confirm >/dev/null

# Topics must be created explicitly (via seed.sh or `make redpanda-topic-create`)
# instead of being implicitly created on first produce/consume.
echo "Disabling topic auto-creation..."
rpk cluster config set auto_create_topics_enabled false -X "admin.hosts=${ADMIN_API}" --no-confirm >/dev/null

echo "Cluster config applied."
