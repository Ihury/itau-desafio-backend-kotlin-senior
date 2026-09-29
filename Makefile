.DEFAULT_GOAL := help

IMAGE := itau-hello-world
COMPOSE := docker compose
HTTP_DIR := http
COMPOSE_PROJECT := $(notdir $(CURDIR))
PARTITIONS ?= 1
COUNT ?= 100

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

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
		node:20-alpine sh -c "npx --yes httpyac send hello.http --all -e docker"

.PHONY: db-up
db-up: ## Start DynamoDB Local + web console and (re)seed the GreetingMessages table
	$(COMPOSE) up dynamodb dynamodb-seed dynamodb-admin -d

.PHONY: db-seed
db-seed: ## Re-run the seed job (table creation is idempotent, items are overwritten)
	$(COMPOSE) up dynamodb-seed

.PHONY: db-scan
db-scan: ## List greeting messages currently stored in DynamoDB
	$(COMPOSE) run --rm --entrypoint aws dynamodb-seed \
		dynamodb scan --table-name GreetingMessages --endpoint-url http://dynamodb:8000 --region us-east-1

.PHONY: db-down
db-down: ## Stop DynamoDB Local + web console
	$(COMPOSE) stop dynamodb dynamodb-seed dynamodb-admin

.PHONY: kafka-up
kafka-up: ## Start Redpanda + Console and (re)seed the greeting-templates topic
	$(COMPOSE) up redpanda redpanda-seed redpanda-console -d

.PHONY: kafka-seed
kafka-seed: ## Re-run the seed job (topic creation is idempotent, messages are re-published)
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

.PHONY: integration-test
integration-test: db-up kafka-up ## Run all integration tests against live DynamoDB + Redpanda
	$(COMPOSE) wait dynamodb-seed redpanda-seed
	./gradlew integrationTest

.PHONY: clean-containers
clean-containers: ## Remove every container for this project, running or stopped, including orphans
	$(COMPOSE) down --remove-orphans --volumes
	@docker ps -aq --filter "label=com.docker.compose.project=$(COMPOSE_PROJECT)" | xargs -r docker rm -f

.PHONY: clean
clean: ## Remove built images
	docker rmi -f $(IMAGE) $(IMAGE)-test 2>/dev/null || true
