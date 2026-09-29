#!/bin/bash
set -euo pipefail

TOPIC="${1:?Usage: produce-accounts-events.sh <topic> [count]}"
COUNT="${2:-100}"
BROKERS="${REDPANDA_BROKERS:-redpanda:9092}"
WINDOW_US=$((10 * 60 * 1000000))
NOW_US=$(date +%s%6N)

random_status() {
  if (( RANDOM % 2 == 0 )); then echo "ENABLED"; else echo "DISABLED"; fi
}

random_created_at() {
  local offset_us=$(( (RANDOM * RANDOM) % WINDOW_US ))
  echo $((NOW_US - offset_us))
}

echo "Producing ${COUNT} account event(s) to topic '${TOPIC}' (brokers: ${BROKERS})..."

for ((i = 1; i <= COUNT; i++)); do
  printf '{"account": {"id": "%s", "owner": "%s", "created_at": %s, "status": "%s"}}\n' \
    "$(cat /proc/sys/kernel/random/uuid)" \
    "$(cat /proc/sys/kernel/random/uuid)" \
    "$(random_created_at)" \
    "$(random_status)"
done | rpk topic produce "${TOPIC}" --brokers "${BROKERS}" -f '%v\n'

echo "Done. Published ${COUNT} event(s) to '${TOPIC}'."
