#!/bin/bash
set -euo pipefail

ENDPOINT_URL="${DYNAMODB_ENDPOINT_URL:-http://dynamodb:8000}"
TABLE_NAME="${GREETING_TABLE_NAME:-GreetingMessages}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
SEED_FILE="/dynamodb-seed/greeting-messages.json"

echo "Waiting for DynamoDB Local at ${ENDPOINT_URL}..."
until aws dynamodb list-tables --endpoint-url "${ENDPOINT_URL}" --region "${REGION}" >/dev/null 2>&1; do
  echo "  not ready yet, retrying in 2s..."
  sleep 2
done
echo "DynamoDB Local is ready."

if aws dynamodb describe-table --table-name "${TABLE_NAME}" --endpoint-url "${ENDPOINT_URL}" --region "${REGION}" >/dev/null 2>&1; then
  echo "Table '${TABLE_NAME}' already exists, skipping creation."
else
  echo "Creating table '${TABLE_NAME}'..."
  aws dynamodb create-table \
    --table-name "${TABLE_NAME}" \
    --attribute-definitions AttributeName=id,AttributeType=S \
    --key-schema AttributeName=id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --endpoint-url "${ENDPOINT_URL}" \
    --region "${REGION}" >/dev/null
  aws dynamodb wait table-exists \
    --table-name "${TABLE_NAME}" \
    --endpoint-url "${ENDPOINT_URL}" \
    --region "${REGION}"
  echo "Table '${TABLE_NAME}' created."
fi

echo "Seeding greeting messages from ${SEED_FILE}..."
aws dynamodb batch-write-item \
  --request-items "file://${SEED_FILE}" \
  --endpoint-url "${ENDPOINT_URL}" \
  --region "${REGION}" >/dev/null

COUNT=$(aws dynamodb scan \
  --table-name "${TABLE_NAME}" \
  --endpoint-url "${ENDPOINT_URL}" \
  --region "${REGION}" \
  --select COUNT \
  --query 'Count' \
  --output text)

echo "Seed complete. '${TABLE_NAME}' now has ${COUNT} item(s)."
