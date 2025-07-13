# Local Testing Guide for Cachier

This guide explains how to run cachier tests locally with Docker containers for external backends.

## Quick Start

```bash
# Test a single backend
./scripts/test-local.sh mongo

# Test multiple backends
./scripts/test-local.sh redis sql

# Test all backends
./scripts/test-local.sh all

# Test all external backends (mongo, redis, sql)
./scripts/test-local.sh external

# Test with options
./scripts/test-local.sh mongo redis -v -k
```

## Available Cores

- **mongo** - MongoDB backend tests
- **redis** - Redis backend tests  
- **sql** - SQL (PostgreSQL) backend tests
- **memory** - Memory backend tests (no Docker needed)
- **pickle** - Pickle backend tests (no Docker needed)

### Core Groups

- **all** - All backends (mongo, redis, sql, memory, pickle)
- **external** - All external backends requiring Docker (mongo, redis, sql)
- **local** - All local backends (memory, pickle)

## Command Line Options

- `-v, --verbose` - Show verbose pytest output
- `-k, --keep-running` - Keep Docker containers running after tests
- `-h, --html-coverage` - Generate HTML coverage report
- `--help` - Show help message

## Examples

### Basic Usage

```bash
# Run MongoDB tests
./scripts/test-local.sh mongo

# Run Redis and SQL tests
./scripts/test-local.sh redis sql

# Run all tests
./scripts/test-local.sh all
```

### Using Make

```bash
# Run specific backends
make test-local CORES="mongo redis"

# Run all tests
make test-all-local

# Run external backends only
make test-external

# Run individual backends
make test-mongo-local
make test-redis-local
make test-sql-local
```

### Advanced Usage

```bash
# Keep containers running for debugging
./scripts/test-local.sh mongo redis -k

# Verbose output with HTML coverage
./scripts/test-local.sh all -v -h

# Using environment variable
CACHIER_TEST_CORES="mongo redis" ./scripts/test-local.sh
```

### Docker Compose

```bash
# Start all services
make services-start

# Run tests manually
CACHIER_TEST_HOST=localhost CACHIER_TEST_PORT=27017 CACHIER_TEST_VS_DOCKERIZED_MONGO=true \
CACHIER_TEST_REDIS_HOST=localhost CACHIER_TEST_REDIS_PORT=6379 CACHIER_TEST_VS_DOCKERIZED_REDIS=true \
SQLALCHEMY_DATABASE_URL="postgresql://testuser:testpass@localhost:5432/testdb" \
pytest -m "mongo or redis or sql"

# Stop all services
make services-stop

# View logs
make services-logs
```

## Docker Containers

The script manages the following containers:

| Backend | Container Name | Port | Image |
|---------|---------------|------|-------|
| MongoDB | cachier-test-mongo | 27017 | mongo:latest |
| Redis | cachier-test-redis | 6379 | redis:7-alpine |
| PostgreSQL | cachier-test-postgres | 5432 | postgres:15 |

## Environment Variables

The script automatically sets the required environment variables:

### MongoDB
- `CACHIER_TEST_HOST=localhost`
- `CACHIER_TEST_PORT=27017`
- `CACHIER_TEST_VS_DOCKERIZED_MONGO=true`

### Redis
- `CACHIER_TEST_REDIS_HOST=localhost`
- `CACHIER_TEST_REDIS_PORT=6379`
- `CACHIER_TEST_REDIS_DB=0`
- `CACHIER_TEST_VS_DOCKERIZED_REDIS=true`

### SQL/PostgreSQL
- `SQLALCHEMY_DATABASE_URL=postgresql://testuser:testpass@localhost:5432/testdb`

## Prerequisites

1. **Docker** - Required for external backends (mongo, redis, sql)
2. **Python dependencies** - Install test requirements:
   ```bash
   pip install -r tests/requirements.txt
   pip install -r tests/mongodb_requirements.txt  # For MongoDB tests
   pip install -r tests/redis_requirements.txt    # For Redis tests
   pip install -r tests/sql_requirements.txt      # For SQL tests
   ```

## Troubleshooting

### Docker not found
- Install Docker Desktop from https://www.docker.com/products/docker-desktop
- Ensure Docker daemon is running

### Port conflicts
- The script will fail if required ports are already in use
- Stop conflicting services or use `docker ps` to check running containers

### Tests failing
- Check container logs: `docker logs cachier-test-<backend>`
- Ensure all dependencies are installed
- Try running with `-v` for verbose output

### Cleanup issues
- If containers aren't cleaned up properly:
  ```bash
  make services-stop
  # or manually
  docker stop cachier-test-mongo cachier-test-redis cachier-test-postgres
  docker rm cachier-test-mongo cachier-test-redis cachier-test-postgres
  ```

## Integration with test-mongo-local.sh

The previous `test-mongo-local.sh` script still works and is now a specialized version of the general `test-local.sh`:

```bash
# These are equivalent:
./scripts/test-mongo-local.sh
./scripts/test-local.sh mongo

# These are equivalent:
./scripts/test-mongo-local.sh --mode also-local
./scripts/test-local.sh mongo memory pickle
```

## Best Practices

1. **Before committing**: Run `./scripts/test-local.sh external` to test all external backends
2. **For quick iteration**: Use memory and pickle tests (no Docker required)
3. **For debugging**: Use `-k` to keep containers running and inspect them
4. **For CI parity**: Test with the same backends that CI uses

## Future Enhancements

- Add MySQL/MariaDB support
- Add Elasticsearch support
- Add performance benchmarking mode
- Add parallel test execution for multiple backends