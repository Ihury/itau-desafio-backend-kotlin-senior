#!/bin/bash
set -euo pipefail

BROKERS="${REDPANDA_BROKERS:-redpanda:9092}"
TOPIC_NAME="${BALANCE_EVENTS_TOPIC:-transacoes-financeiras-processadas}"
DLT_TOPIC_NAME="${BALANCE_EVENTS_DLT_TOPIC:-${TOPIC_NAME}.DLT}"

echo "Waiting for Redpanda broker at ${BROKERS}..."
until rpk cluster info --brokers "${BROKERS}" >/dev/null 2>&1; do
  echo "  not ready yet, retrying in 2s..."
  sleep 2
done
echo "Redpanda broker is ready."

# Cria o topico se ainda nao existir (idempotente). Argumentos extras vao para `rpk topic create`.
ensure_topic() {
  local name="$1"
  shift
  if rpk topic describe "${name}" --brokers "${BROKERS}" >/dev/null 2>&1; then
    echo "Topic '${name}' already exists, skipping creation."
  else
    echo "Creating topic '${name}'..."
    rpk topic create "${name}" --brokers "${BROKERS}" --replicas 1 "$@"
  fi
}

# Entrada: 12 particoes; nenhuma suposicao de ordem e feita pelo servico (Constitution II).
ensure_topic "${TOPIC_NAME}" --partitions 12
# Dead Letter Topic: 3 particoes e retencao de 14 dias (1209600000 ms) para investigacao e reprocessamento manual.
ensure_topic "${DLT_TOPIC_NAME}" --partitions 3 -c retention.ms=1209600000

echo "Seed complete. Topics '${TOPIC_NAME}' and '${DLT_TOPIC_NAME}' are ready (no messages published)."
