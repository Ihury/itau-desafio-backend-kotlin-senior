#!/bin/bash
# Espera os jobs de seed (`dynamodb-seed`, `redpanda-seed`) terminarem com sucesso, com ou sem eles ja terem terminado.
#
# `docker compose wait <seed>` falha ("no containers for project") quando o seed ja encerrou antes da chamada, e
# `docker compose up -d --wait` trata o seed encerrado (exit 0) como erro. Este script consulta o estado e o codigo de saida do
# proprio container, entao e seguro chamar depois de `docker compose up -d dynamodb dynamodb-seed redpanda redpanda-seed`.
#
# Uso: infra/wait-seeds.sh [timeout_em_segundos]   (default: 120)
set -euo pipefail

TIMEOUT="${1:-120}"
SERVICES=(dynamodb-seed redpanda-seed)
DEADLINE=$(( $(date +%s) + TIMEOUT ))

for service in "${SERVICES[@]}"; do
  container_id="$(docker compose ps -a -q "${service}")"
  if [ -z "${container_id}" ]; then
    echo "No container for '${service}': run 'docker compose up -d dynamodb dynamodb-seed redpanda redpanda-seed' first." >&2
    exit 1
  fi

  while [ "$(docker inspect -f '{{.State.Status}}' "${container_id}")" != "exited" ]; do
    if [ "$(date +%s)" -ge "${DEADLINE}" ]; then
      echo "Timed out after ${TIMEOUT}s waiting for '${service}' to finish." >&2
      docker logs --tail 20 "${container_id}" >&2 || true
      exit 1
    fi
    sleep 1
  done

  exit_code="$(docker inspect -f '{{.State.ExitCode}}' "${container_id}")"
  if [ "${exit_code}" != "0" ]; then
    echo "'${service}' exited with code ${exit_code}." >&2
    docker logs --tail 20 "${container_id}" >&2 || true
    exit 1
  fi
  echo "${service} finished successfully."
done
