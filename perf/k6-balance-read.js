// Teste de carga da consulta de saldo (SC-001): GET /balances/{accountId}.
//
// Meta: a 500 req/s, p50 < 50 ms e p99 < 300 ms. O limiar vale so para a fase "steady"; o aquecimento
// (warmup) existe para a JVM compilar os caminhos quentes antes da medicao e nao entra nos limiares.
//
// Uso recomendado: `make load-test` (gera contas, coleta os ids e executa este script via Docker).
// Variaveis (-e): BASE_URL, RATE, DURATION, WARMUP_RATE, WARMUP_DURATION, ACCOUNTS (ids separados por virgula).
//
// Ressalva: com DynamoDB Local e gerador de carga na mesma maquina, o resultado NAO representa a AWS.
import http from 'k6/http';
import { check, fail } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:8080';
const RATE = parseInt(__ENV.RATE || '500', 10);
const DURATION = __ENV.DURATION || '60s';
const WARMUP_RATE = parseInt(__ENV.WARMUP_RATE || '100', 10);
const WARMUP_DURATION = __ENV.WARMUP_DURATION || '10s';
const SAMPLE_ACCOUNT = '5b19c8b6-0cc4-4c72-a989-0c2ee15fa975'; // item do seed (exemplo do cliente)
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export const options = {
  scenarios: {
    warmup: {
      executor: 'constant-arrival-rate',
      rate: WARMUP_RATE,
      timeUnit: '1s',
      duration: WARMUP_DURATION,
      preAllocatedVUs: 20,
      maxVUs: 200,
      exec: 'read',
    },
    steady: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      startTime: WARMUP_DURATION,
      preAllocatedVUs: 100,
      maxVUs: 1000,
      exec: 'read',
    },
  },
  thresholds: {
    'http_req_duration{scenario:steady}': ['p(50)<50', 'p(99)<300'],
    'http_req_failed{scenario:steady}': ['rate<0.001'],
    'checks{scenario:steady}': ['rate>0.999'],
    // a taxa pedida precisa ter sido de fato oferecida; iteracao descartada indica gerador ou servico saturado
    'dropped_iterations{scenario:steady}': ['count==0'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(50)', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  const requested = (__ENV.ACCOUNTS || '')
    .split(',')
    .map((id) => id.trim().toLowerCase())
    .filter((id) => UUID.test(id));
  const candidates = Array.from(new Set([SAMPLE_ACCOUNT, ...requested]));

  // Mantem so as contas que respondem 200: o teste mede a leitura de saldos existentes, nao 404/409.
  const usable = candidates.filter((id) => http.get(`${BASE_URL}/balances/${id}`).status === 200);
  if (usable.length === 0) {
    fail(`nenhuma conta respondeu 200 em ${BASE_URL}; suba a stack com "make up" antes do teste de carga`);
  }
  console.log(`contas usadas na carga: ${usable.length} de ${candidates.length} candidatas`);
  return { accounts: usable };
}

export function read(data) {
  const id = data.accounts[Math.floor(Math.random() * data.accounts.length)];
  const res = http.get(`${BASE_URL}/balances/${id}`, {
    tags: { name: 'GET /balances/{accountId}' },
  });
  // Sem 200 nao ha corpo confiavel para conferir (erro de rede tem status 0): o segundo check so avalia o corpo do 200.
  return check(res, {
    'status 200': (r) => r.status === 200,
    'corpo e o saldo da conta pedida': (r) => r.status === 200 && r.json('id') === id,
  });
}
