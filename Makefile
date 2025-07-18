# Cachier Makefile
# Development tasks and shortcuts

.PHONY: help test test-all test-local test-all-local test-external \
        test-mongo-local test-mongo-docker test-mongo-inmemory test-mongo-also-local \
        test-redis-local test-sql-local \
        services-start services-stop services-logs \
        mongo-start mongo-stop mongo-logs lint type-check format clean \
        install install-dev install-all

# Default target
help:
	@echo "Cachier Development Commands:"
	@echo ""
	@echo "Testing:"
	@echo "  make test               - Run all tests"
	@echo "  make test-local CORES='...' - Run tests for specified cores with Docker"
	@echo "  make test-all-local     - Run all backend tests with Docker"
	@echo "  make test-external      - Run all external backend tests (mongo, redis, sql)"
	@echo "  make test-mongo-local   - Run MongoDB tests with Docker"
	@echo "  make test-redis-local   - Run Redis tests with Docker"
	@echo "  make test-sql-local     - Run SQL tests with Docker"
	@echo "  make test-mongo-also-local - Run MongoDB + memory, pickle, maxage tests with Docker"
	@echo "  make test-mongo-inmemory - Run MongoDB tests with in-memory backend"
	@echo "  make services-start     - Start all test containers"
	@echo "  make services-stop      - Stop all test containers"
	@echo "  make mongo-start        - Start MongoDB container"
	@echo "  make mongo-stop         - Stop MongoDB container"
	@echo "  make mongo-logs         - View MongoDB container logs"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint               - Run ruff linter"
	@echo "  make type-check         - Run mypy type checker"
	@echo "  make format             - Format code with ruff"
	@echo ""
	@echo "Installation:"
	@echo "  make install            - Install package"
	@echo "  make install-dev        - Install with development dependencies"
	@echo "  make install-all        - Install with all optional dependencies"
	@echo ""
	@echo "Other:"
	@echo "  make clean              - Clean build artifacts"

# Installation targets
install:
	pip install -e .

install-dev:
	pip install -e .
	pip install -r tests/requirements.txt

install-all:
	pip install -e .[all]
	pip install -r tests/requirements.txt
	pip install -r tests/mongodb_requirements.txt
	pip install -r tests/redis_requirements.txt
	pip install -r tests/sql_requirements.txt

# Testing targets
test:
	pytest

test-all: test

# MongoDB testing targets
test-mongo-inmemory:
	@echo "Running MongoDB tests with in-memory backend..."
	pytest -m mongo --cov=cachier --cov-report=term

test-mongo-docker:
	@echo "Running MongoDB tests against Docker MongoDB..."
	./scripts/test-mongo-local.sh

test-mongo-local: test-mongo-docker

test-mongo-also-local:
	@echo "Running MongoDB tests with local core tests..."
	./scripts/test-mongo-local.sh --mode also-local

# New unified testing targets
test-local:
	@echo "Running tests for cores: $(CORES)"
	./scripts/test-local.sh $(CORES)

test-all-local:
	@echo "Running all backend tests with Docker..."
	./scripts/test-local.sh all

test-external:
	@echo "Running all external backend tests..."
	./scripts/test-local.sh external

test-redis-local:
	@echo "Running Redis tests with Docker..."
	./scripts/test-local.sh redis

test-sql-local:
	@echo "Running SQL tests with Docker..."
	./scripts/test-local.sh sql

# Service management
services-start:
	@echo "Starting all test services..."
	docker-compose -f scripts/docker-compose.all-cores.yml up -d

services-stop:
	@echo "Stopping all test services..."
	docker-compose -f scripts/docker-compose.all-cores.yml down

services-logs:
	docker-compose -f scripts/docker-compose.all-cores.yml logs -f

# MongoDB container management
mongo-start:
	@echo "Starting MongoDB container..."
	@docker ps -q -f name=cachier-test-mongo | grep -q . && \
		(echo "MongoDB container already running" && exit 0) || \
		(docker run -d -p 27017:27017 --name cachier-test-mongo mongo:latest && \
		echo "Waiting for MongoDB to start..." && sleep 5)

mongo-stop:
	@echo "Stopping MongoDB container..."
	@docker ps -q -f name=cachier-test-mongo | grep -q . && \
		(docker stop cachier-test-mongo && docker rm cachier-test-mongo) || \
		echo "MongoDB container not running"

mongo-logs:
	@docker logs cachier-test-mongo

# Code quality targets
lint:
	ruff check .

type-check:
	mypy src/cachier/

format:
	ruff format .

# Clean targets
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
