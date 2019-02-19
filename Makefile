RUN?=""
KILLCACHE=-count=1
CLI_PORT?=7006
UP_IP?=0.0.0.0

unit:
	go test $(KILLCACHE) -v -race -short -cover ./... -run $(RUN)

test:
	REDIS_SINGLE=localhost:7006 REDIS_CLUSTER=localhost:7000 go test $(KILLCACHE) -v -race -cover ./... -run $(RUN)

up:
	REDIS_CLUSTER_IP=$(UP_IP) docker-compose up -d
	docker-compose ps

down:
	docker-compose down
	docker-compose ps

bash:
	docker-compose exec redis-cluster /bin/bash

cli:
	docker-compose exec redis-cluster /redis/src/redis-cli -p $(CLI_PORT)

monitor:
	docker-compose exec redis-cluster /redis/src/redis-cli -p $(CLI_PORT) monitor
