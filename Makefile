RUN?=""
KILLCACHE=-count=1
CLI_PORT?=7006

unit:
	go test $(KILLCACHE) -v -race -short ./... -run $(RUN)

test:
	REDIS_SINGLE=localhost:7006 REDIS_CLUSTER=localhost:7000 go test $(KILLCACHE) -v -race -cover ./... -run $(RUN) 

bash:
	docker-compose exec redis-cluster /bin/bash

cli:
	docker-compose exec redis-cluster /redis/src/redis-cli -p $(CLI_PORT)

monitor:
	docker-compose exec redis-cluster /redis/src/redis-cli -p $(CLI_PORT) monitor
