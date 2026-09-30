.DEFAULT_GOAL := help

IMAGE := consulta-saldo
COMPOSE := docker compose
HTTP_DIR := http
COMPOSE_PROJECT := $(notdir $(CURDIR))
PARTITIONS ?= 1
COUNT ?= 100
SCENARIO_TOPIC := $(or $(TOPIC),transacoes-financeiras-processadas)
K6_IMAGE := grafana/k6:2.3.0
PERF_DIR := perf
LOAD_ACCOUNTS ?= 200
LOAD_RATE ?= 500
LOAD_DURATION ?= 60s

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-34s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the application image
	docker build --target runtime -t $(IMAGE) .

.PHONY: test
test: ## Run tests + coverage gate (min 90%) inside a container
	docker build --target test --progress=plain -t $(IMAGE)-test .

.PHONY: run
run: ## Start the application (foreground)
	$(COMPOSE) up --build

.PHONY: up
up: ## Start the application in the background
	$(COMPOSE) up --build -d

.PHONY: logs
logs: ## Tail the application logs (when started with make up)
	$(COMPOSE) logs -f

.PHONY: stop
stop: ## Stop and remove containers started by docker compose
	$(COMPOSE) down

.PHONY: http
http: ## Call all .http files against the running app (no local deps, runs via Docker)
	docker run --rm \
		--add-host=host.docker.internal:host-gateway \
		-v "$(CURDIR)/$(HTTP_DIR)":/http -w /http \
		node:20-alpine sh -c "npx --yes httpyac send *.http --all -e docker"

.PHONY: balance-get
balance-get: ## Query the balance of an account (usage: make balance-get ACCOUNT=<uuid>)
	@if [ -z "$(ACCOUNT)" ]; then \
		echo "ACCOUNT is required, e.g. make balance-get ACCOUNT=5b19c8b6-0cc4-4c72-a989-0c2ee15fa975"; \
		exit 1; \
	fi
	@if ! echo "$(ACCOUNT)" | grep -Eq '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$$'; then \
		echo "ACCOUNT must be a UUID in the 8-4-4-4-12 format, got: $(ACCOUNT)"; \
		exit 1; \
	fi
	curl -si http://localhost:8080/balances/$(ACCOUNT)

.PHONY: db-up
db-up: ## Start DynamoDB Local + web console and (re)seed the AccountBalances table
	$(COMPOSE) up dynamodb dynamodb-seed dynamodb-admin -d

.PHONY: db-seed
db-seed: ## Re-run the seed job (table creation is idempotent, the sample account is overwritten)
	$(COMPOSE) up dynamodb-seed

.PHONY: db-scan
db-scan: ## List the account balances currently stored in DynamoDB
	$(COMPOSE) run --rm --entrypoint aws dynamodb-seed \
		dynamodb scan --table-name AccountBalances --endpoint-url http://dynamodb:8000 --region us-east-1

.PHONY: db-down
db-down: ## Stop DynamoDB Local + web console
	$(COMPOSE) stop dynamodb dynamodb-seed dynamodb-admin

.PHONY: kafka-up
kafka-up: ## Start Redpanda + Console and (re)seed the input and DLT topics
	$(COMPOSE) up redpanda redpanda-seed redpanda-console -d

.PHONY: kafka-seed
kafka-seed: ## Re-run the seed job (topic creation is idempotent, no messages are published)
	$(COMPOSE) up redpanda-seed

.PHONY: kafka-topic-create
kafka-topic-create: ## Create a Kafka topic on Redpanda (usage: make kafka-topic-create NAME=my-topic [PARTITIONS=3])
	@if [ -z "$(NAME)" ]; then \
		echo "NAME is required, e.g. make kafka-topic-create NAME=my-topic PARTITIONS=3"; \
		exit 1; \
	fi
	$(COMPOSE) run --rm --entrypoint rpk redpanda-seed \
		topic create $(NAME) --brokers redpanda:9092 --partitions $(PARTITIONS) --replicas 1

.PHONY: kafka-produce-accounts-events
kafka-produce-accounts-events: ## Produce random account-event JSON messages to a Kafka topic (usage: make kafka-produce-accounts-events TOPIC=my-topic [COUNT=100])
	@if [ -z "$(TOPIC)" ]; then \
		echo "TOPIC is required, e.g. make kafka-produce-accounts-events TOPIC=my-topic COUNT=50"; \
		exit 1; \
	fi
	$(COMPOSE) run --rm --entrypoint /bin/bash redpanda-seed \
		/redpanda-seed/produce-accounts-events.sh $(TOPIC) $(COUNT)

.PHONY: kafka-produce-transactions-events
kafka-produce-transactions-events: ## Produce random transaction+account event JSON messages to a Kafka topic (usage: make kafka-produce-transactions-events TOPIC=my-topic [COUNT=100])
	@if [ -z "$(TOPIC)" ]; then \
		echo "TOPIC is required, e.g. make kafka-produce-transactions-events TOPIC=my-topic COUNT=50"; \
		exit 1; \
	fi
	$(COMPOSE) run --rm --entrypoint /bin/bash redpanda-seed \
		/redpanda-seed/produce-transactions-events.sh $(TOPIC) $(COUNT)

.PHONY: kafka-produce-scenario
kafka-produce-scenario: ## Publish the deterministic scenario (disorder, duplicate, tie, DISABLED, invalid, poison) and print the expected results (optional TOPIC=)
	$(COMPOSE) run --rm --entrypoint /bin/bash redpanda-seed \
		/redpanda-seed/produce-scenario-events.sh $(SCENARIO_TOPIC)

.PHONY: kafka-consume
kafka-consume: ## Print all messages on a Kafka topic (usage: make kafka-consume TOPIC=my-topic)
	@if [ -z "$(TOPIC)" ]; then \
		echo "TOPIC is required, e.g. make kafka-consume TOPIC=my-topic"; \
		exit 1; \
	fi
	$(COMPOSE) run --rm --entrypoint /bin/bash redpanda-seed -c \
		"timeout 5 rpk topic consume $(TOPIC) --brokers redpanda:9092 --format '%v\n' || true"

.PHONY: kafka-down
kafka-down: ## Stop Redpanda + Console
	$(COMPOSE) stop redpanda redpanda-seed redpanda-console

.PHONY: wait-seeds
wait-seeds: ## Wait for the DynamoDB and Redpanda seed jobs to finish (works whether they already exited or not)
	./infra/wait-seeds.sh

.PHONY: integration-test
integration-test: db-up kafka-up wait-seeds ## Run the functional integration tests against live DynamoDB + Redpanda, excluding the perf tag (always re-executed)
	./gradlew cleanIntegrationTest integrationTest

.PHONY: perf-test
perf-test: db-up kafka-up wait-seeds ## Run the perf-tagged integration tests (SC-006: p99 with invalid messages <= 1.10x baseline); latency-sensitive, run on a quiet machine
	./gradlew cleanPerfTest perfTest

.PHONY: load-test
load-test: ## Load test GET /balances/{id} with k6 (SC-001: p50<50ms, p99<300ms at 500 req/s). Needs make up. Vars: LOAD_RATE, LOAD_DURATION, LOAD_ACCOUNTS
	@echo "Publishing $(LOAD_ACCOUNTS) transaction events (one new account each) and waiting for ingestion..."
	$(COMPOSE) run --rm -T --entrypoint /bin/bash redpanda-seed \
		/redpanda-seed/produce-transactions-events.sh $(SCENARIO_TOPIC) $(LOAD_ACCOUNTS)
	@sleep 10
	@ids=$$($(COMPOSE) run --rm -T --entrypoint aws dynamodb-seed \
		dynamodb scan --table-name AccountBalances --endpoint-url http://dynamodb:8000 --region us-east-1 \
		--max-items $(LOAD_ACCOUNTS) --query 'Items[].pk.S' --output text 2>/dev/null \
		| tr '\t' '\n' | sed -n 's/^ACCOUNT#//p' | paste -sd, -); \
	echo "Accounts collected from DynamoDB: $$(echo "$$ids" | tr ',' '\n' | grep -c .)"; \
	docker run --rm \
		--add-host=host.docker.internal:host-gateway \
		-v "$(CURDIR)/$(PERF_DIR)":/perf -w /perf \
		-e RATE=$(LOAD_RATE) -e DURATION=$(LOAD_DURATION) -e ACCOUNTS="$$ids" \
		$(K6_IMAGE) run k6-balance-read.js

.PHONY: chaos-dynamodb-pause
chaos-dynamodb-pause: ## Chaos: freeze DynamoDB Local (queries answer 503, ingestion applies backpressure). Undo with chaos-dynamodb-unpause
	$(COMPOSE) pause dynamodb

.PHONY: chaos-dynamodb-unpause
chaos-dynamodb-unpause: ## Chaos: resume DynamoDB Local (the consumer recovers on its own)
	$(COMPOSE) unpause dynamodb

.PHONY: clean-containers
clean-containers: ## Remove every container for this project, running or stopped, including orphans
	$(COMPOSE) down --remove-orphans --volumes
	@docker ps -aq --filter "label=com.docker.compose.project=$(COMPOSE_PROJECT)" | xargs -r docker rm -f

.PHONY: clean
clean: ## Remove built images
	docker rmi -f $(IMAGE) $(IMAGE)-test 2>/dev/null || true
