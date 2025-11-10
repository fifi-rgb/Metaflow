.PHONY: help build build-dev up up-dev down logs clean test shell

help:
	@echo "MetaFlow Docker Commands"
	@echo "========================"
	@echo "build          - Build production image"
	@echo "build-dev      - Build development image"
	@echo "up             - Start production containers"
	@echo "up-dev         - Start development containers"
	@echo "down           - Stop all containers"
	@echo "logs           - View container logs"
	@echo "clean          - Remove all containers and volumes"
	@echo "test           - Run tests in container"
	@echo "shell          - Open bash shell in container"

build:
	docker-compose build metaflow

build-dev:
	docker-compose -f docker-compose.dev.yml build metaflow-dev

up:
	docker-compose up -d
	@echo "Services started. Access points:"
	@echo "  - Spark UI: http://localhost:4040"
	@echo "  - Kafka UI: http://localhost:8090"
	@echo "  - MinIO: http://localhost:9001"
	@echo "  - Grafana: http://localhost:3000"
	@echo "  - Adminer: http://localhost:8081"

up-dev:
	docker-compose -f docker-compose.dev.yml up -d
	@echo "Development environment started."
	@echo "  - Jupyter Lab: http://localhost:8888"
	@echo "  - Spark UI: http://localhost:4040"

down:
	docker-compose down
	docker-compose -f docker-compose.dev.yml down

logs:
	docker-compose logs -f

clean:
	docker-compose down -v
	docker-compose -f docker-compose.dev.yml down -v
	docker system prune -f

test:
	docker-compose exec metaflow pytest tests/ -v

shell:
	docker-compose exec metaflow bash

shell-dev:
	docker-compose -f docker-compose.dev.yml exec metaflow-dev bash