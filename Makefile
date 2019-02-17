RUN?=""
KILLCACHE=-count=1
CLI_PORT?=7006

unit:
	go test $(KILLCACHE) -v -short ./... -run $(RUN)

test:
	REDIS_SINGLE=localhost:7006 REDIS_CLUSTER=localhost:7000 go test $(KILLCACHE) -v ./... -run $(RUN)

bash:
	docker-compose exec redis-cluster /bin/bash

cli:
	docker-compose exec redis-cluster /redis/src/redis-cli -p $(CLI_PORT)
