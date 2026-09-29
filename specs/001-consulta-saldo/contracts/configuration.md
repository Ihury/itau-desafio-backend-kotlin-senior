# Contrato de configuração (12-factor)

Toda configuração vem de variáveis de ambiente com defaults para o ambiente local (docker compose).
Segredos NÃO existem no código; fora do ambiente local as credenciais AWS vêm da *default credentials provider chain*.

| Variável | Default | Descrição |
|----------|---------|-----------|
| `SERVER_PORT` | `8080` | Porta da API |
| `MANAGEMENT_SERVER_PORT` | `8082` | Porta do Actuator (health/prometheus), separada da API (publicada no host somente no compose local) |
| `DYNAMODB_ENDPOINT` | `http://localhost:8000` | Vazio/ausente em produção (usa o endpoint AWS). Se definido, ativa credenciais estáticas locais |
| `DYNAMODB_REGION` | `us-east-1` | Região |
| `BALANCE_TABLE_NAME` | `AccountBalances` | Tabela do snapshot |
| `DYNAMODB_READ_CONSISTENT` | `true` | `ConsistentRead` na consulta (trade-off custo x atualidade, R-05) |
| `DYNAMODB_READ_ATTEMPT_TIMEOUT` / `DYNAMODB_READ_CALL_TIMEOUT` | `PT0.6S` / `PT1.5S` | Timeouts do cliente de leitura (API) |
| `DYNAMODB_READ_MAX_ATTEMPTS` | `2` | Tentativas do retry do SDK na leitura (1 retry) |
| `DYNAMODB_WRITE_ATTEMPT_TIMEOUT` / `DYNAMODB_WRITE_CALL_TIMEOUT` | `PT2S` / `PT2S` | Timeouts do cliente de escrita (consumer); sem retry do SDK |
| `DYNAMODB_CONNECT_TIMEOUT` / `DYNAMODB_ACQUIRE_TIMEOUT` | `PT0.3S` / `PT0.3S` | Conexão e aquisição no pool (falha rápida = bulkhead) |
| `DYNAMODB_READ_MAX_CONNECTIONS` / `DYNAMODB_WRITE_MAX_CONNECTIONS` | `100` / `50` | Tamanho dos pools (isolados por cliente) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Brokers |
| `KAFKA_CONSUMER_GROUP_ID` | `consulta-saldo` | Grupo de consumo |
| `BALANCE_EVENTS_TOPIC` | `transacoes-financeiras-processadas` | Tópico de entrada |
| `BALANCE_EVENTS_DLT_TOPIC` | `${BALANCE_EVENTS_TOPIC}.DLT` | Tópico de isolamento |
| `KAFKA_LISTENER_CONCURRENCY` | `4` | Threads de consumo por instância (total entre instâncias <= partições) |
| `KAFKA_MAX_POLL_RECORDS` / `KAFKA_MAX_POLL_INTERVAL_MS` | `100` / `300000` | Invariante verificada por teste: `max.poll.records x DYNAMODB_WRITE_CALL_TIMEOUT < max.poll.interval.ms` |
| `KAFKA_BACKOFF_INITIAL_MS` / `KAFKA_BACKOFF_MAX_MS` | `500` / `30000` | Backoff exponencial do consumer (multiplicador 2,0) |
| `KAFKA_BACKOFF_JITTER_MS` | `250` | Jitter (escala com o multiplicador; Spring Framework 7) |
| `BALANCE_DLT_SEND_TIMEOUT` | `PT5S` | `waitForSendResultTimeout` da publicação síncrona no DLT |
| `KAFKA_DLT_MAX_BLOCK_MS` | `3000` | `max.block.ms` do produtor do DLT (`acks=all` e idempotência são fixos) |
| `BALANCE_FUTURE_TOLERANCE` | `PT5M` | Tolerância de timestamp futuro (FR-012); relógio usado só para isso |
| `BALANCE_MIN_EVENT_TIMESTAMP` | `2000-01-01T00:00:00Z` | Limite inferior de `transaction.timestamp` (detecta unidade s/ms) |
| `BALANCE_MIN_ACCOUNT_CREATED_AT` | `1900-01-01T00:00:00Z` | Limite inferior de `account.created_at` (não participa da precedência) |
| `BALANCE_DISPLAY_ZONE` | `America/Sao_Paulo` | Fuso do offset em `updated_at` |
| `BALANCE_CB_WINDOW` | `PT10S` | Janela deslizante (por tempo) do circuit breaker de leitura |
| `BALANCE_CB_MIN_CALLS` / `BALANCE_CB_FAILURE_RATE` | `20` / `50` | Chamadas mínimas / % de falha para abrir |
| `BALANCE_CB_SLOW_CALL` / `BALANCE_CB_SLOW_RATE` | `PT0.5S` / `80` | Chamada lenta / % de lentas para abrir |
| `BALANCE_CB_OPEN_WAIT` | `PT10S` | Espera em OPEN; também o valor de `Retry-After` (10) |
| `BALANCE_CB_HALF_OPEN_CALLS` | `5` | Chamadas de teste em HALF_OPEN |
| `LOGGING_STRUCTURED_FORMAT_CONSOLE` | `logstash` | Logs JSON nativos do Spring Boot |
| `JAVA_TOOL_OPTIONS` | `-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError` | Heap relativa à memória do container (Dockerfile) |
