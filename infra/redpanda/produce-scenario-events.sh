#!/bin/bash
# Publica um cenario DETERMINISTICO (contas, transacoes e instantes fixos) no topico de entrada e imprime o resultado esperado de cada
# consulta (specs/001-consulta-saldo/quickstart.md, secoes 5 e 6). Pode ser repetido: eventos reentregues viram `duplicate`/`obsolete`.
set -euo pipefail

TOPIC="${1:?Usage: produce-scenario-events.sh <topic>}"
BROKERS="${REDPANDA_BROKERS:-redpanda:9092}"

OWNER=315e3cfe-f4af-4cd2-b298-a449e614349a
T0=1751749453433123 # 2025-07-05T18:04:13.433123-03:00
CREATED_AT=1634874339000000

# Contas do cenario (prefixo proprio: nao colidem com o exemplo do seed nem com as contas aleatorias do gerador).
DISORDER=00000000-0000-4000-8001-000000000001
TIE=00000000-0000-4000-8001-000000000002
DECLINED=00000000-0000-4000-8001-000000000003
PRECISION=00000000-0000-4000-8001-000000000004
DISABLED=00000000-0000-4000-8001-000000000005
REENABLED=00000000-0000-4000-8001-000000000006
OLD_ACCOUNT=00000000-0000-4000-8001-000000000007
VALID=00000000-0000-4000-8001-0000000000f0
DEFECT=00000000-0000-4000-8001-0000000000f1

ts() { echo $(( T0 + $1 * 1000000 )); } # ts N = T0 + N segundos
tx() { printf '00000000-0000-4000-8002-%012d' "$1"; }

# ev <txId> <timestampMicros> <accountId> <accountStatus> <balance> [txStatus] [accountCreatedAt]
ev() {
  printf '{"transaction":{"id":"%s","type":"CREDIT","amount":10.00,"currency":"BRL","status":"%s","timestamp":%s},"account":{"id":"%s","owner":"%s","created_at":%s,"status":"%s","balance":{"amount":%s,"currency":"BRL"}}}\n' \
    "$1" "${6:-APPROVED}" "$2" "$3" "$OWNER" "${7:-$CREATED_AT}" "$4" "$5"
}

DEFECT_EVENT=$(ev "$(tx 40)" "$(ts 1)" "$DEFECT" ENABLED 5.00)

echo "Publishing the deterministic scenario to topic '${TOPIC}' (brokers: ${BROKERS})..."
{
  # 1. Desordem + duplicata: vence o instante mais novo (300.00), mesmo tendo chegado primeiro.
  ev "$(tx 3)" "$(ts 3)" "$DISORDER" ENABLED 300.00
  ev "$(tx 1)" "$(ts 1)" "$DISORDER" ENABLED 100.00
  ev "$(tx 2)" "$(ts 2)" "$DISORDER" ENABLED 200.00
  ev "$(tx 3)" "$(ts 3)" "$DISORDER" ENABLED 300.00

  # 2. Empate de timestamp: vence o maior transaction.id (20.00), qualquer que seja a ordem de chegada.
  ev "$(tx 11)" "$(ts 5)" "$TIE" ENABLED 20.00
  ev "$(tx 10)" "$(ts 5)" "$TIE" ENABLED 10.00

  # 3. DECLINED participa da precedencia (0.10) e precisao total preservada (38 digitos, escala 18).
  ev "$(tx 20)" "$(ts 1)" "$DECLINED" ENABLED 50.00
  ev "$(tx 21)" "$(ts 2)" "$DECLINED" ENABLED 0.10 DECLINED
  ev "$(tx 22)" "$(ts 1)" "$PRECISION" ENABLED 12345678901234567890.123456789012345678

  # 4. Ciclo DISABLED: a conta desabilitada responde 409 (evento antigo nao a reabilita); reabilitada por evento mais novo, volta a 200.
  ev "$(tx 30)" "$(ts 1)" "$DISABLED" ENABLED 50.00
  ev "$(tx 31)" "$(ts 2)" "$DISABLED" DISABLED 50.00
  ev "$(tx 29)" "$(ts 0)" "$DISABLED" ENABLED 40.00
  ev "$(tx 32)" "$(ts 1)" "$REENABLED" ENABLED 50.00
  ev "$(tx 33)" "$(ts 2)" "$REENABLED" DISABLED 50.00
  ev "$(tx 34)" "$(ts 3)" "$REENABLED" ENABLED 70.00

  # 5. Conta criada antes de 2000 (1998-07-01) e valida.
  ev "$(tx 60)" "$(ts 1)" "$OLD_ACCOUNT" ENABLED 15.00 APPROVED 899251200000000

  # 6. Nove mensagens defeituosas (DLT) intercaladas com uma valida.
  printf '{not json\n'                                                                    # malformed_payload
  printf '\xc3\x28\n'                                                                     # malformed_payload (veneno binario: UTF-8 invalido)
  echo "$DEFECT_EVENT" | sed 's/"owner":"[^"]*",//'                                        # missing_field
  echo "$DEFECT_EVENT" | sed "s/$DEFECT/1-1-1-1-1/"                                        # invalid_identifier
  ev "$(tx 41)" "$(ts 1)" "$VALID" ENABLED 5.00                                            # valida: processada
  echo "$DEFECT_EVENT" | sed 's/"currency":"BRL"/"currency":"brl"/'                        # invalid_currency
  echo "$DEFECT_EVENT" | sed 's/"amount":10.00/"amount":"10.00"/'                          # invalid_value
  echo "$DEFECT_EVENT" | sed "s/\"timestamp\":$(ts 1)/\"timestamp\":1751749453433/"        # invalid_timestamp (milissegundos)
  echo "$DEFECT_EVENT" | sed 's/"type":"CREDIT"/"type":"TRANSFER"/'                        # unknown_domain_value
  echo "$DEFECT_EVENT" | sed 's/"status":"ENABLED"/"status":"SUSPENDED"/'                  # unknown_domain_value
} | rpk topic produce "${TOPIC}" --brokers "${BROKERS}" -f '%v\n'

cat <<EXPECTED

Done. 17 valid + 9 defective messages published. After ~5 s the expected results are:

  make balance-get ACCOUNT=${DISORDER}    200  balance 300.00 BRL, updated_at 2025-07-05T18:04:16.433123-03:00 (1 processed, 2 obsolete, 1 duplicate)
  make balance-get ACCOUNT=${TIE}    200  balance 20.00 BRL,  updated_at 2025-07-05T18:04:18.433123-03:00 (larger transaction.id wins the tie)
  make balance-get ACCOUNT=${DECLINED}    200  balance 0.10 BRL,   updated_at 2025-07-05T18:04:15.433123-03:00 (DECLINED takes part in precedence)
  make balance-get ACCOUNT=${PRECISION}    200  balance 12345678901234567890.123456789012345678 BRL (no rounding, no scientific notation)
  make balance-get ACCOUNT=${DISABLED}    409  problem+json 'conta-desabilitada', no balance and no owner (the older ENABLED event does not re-enable it)
  make balance-get ACCOUNT=${REENABLED}    200  balance 70.00 BRL,  updated_at 2025-07-05T18:04:16.433123-03:00 (re-enabled by a newer event)
  make balance-get ACCOUNT=${OLD_ACCOUNT}    200  balance 15.00 BRL (account created in 1998 is valid)
  make balance-get ACCOUNT=${VALID}    200  balance 5.00 BRL
  make balance-get ACCOUNT=${DEFECT}    404  (the defective messages never touch the snapshot)

  Dead-letter topic (${TOPIC}.DLT): 9 new messages with the original value preserved and x-rejection-reason
  malformed_payload=2, missing_field=1, invalid_identifier=1, invalid_currency=1, invalid_value=1, invalid_timestamp=1,
  unknown_domain_value=2. Inspect with: make kafka-consume TOPIC=${TOPIC}.DLT
EXPECTED
