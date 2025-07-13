# PR Title: Enable local MongoDB testing with Docker for developers

## Description

This PR addresses the gap between local development (using `pymongo_inmemory`) and CI testing (using real MongoDB via Docker) by providing developers with easy-to-use tools to run MongoDB tests locally against a real MongoDB instance.

## What's New

### 1. **Test Script** (`scripts/test-mongo-local.sh`)
A comprehensive bash script that:
- Automatically manages Docker MongoDB container lifecycle
- Configures required environment variables
- Runs tests with coverage reporting
- Provides graceful cleanup

**Options:**
- `--mode also-local`: Include memory, pickle, and maxage tests alongside MongoDB tests
- `--keep-running`: Keep MongoDB container running after tests (useful for debugging)
- `--verbose`: Enable verbose pytest output
- `--coverage-html`: Generate HTML coverage report

### 2. **Docker Compose** (`scripts/docker-compose.mongodb.yml`)
Alternative approach for developers who prefer docker-compose:
- MongoDB service with health checks
- Proper network configuration
- Usage instructions included

### 3. **Makefile Integration**
New make targets for convenience:
- `make test-mongo-local`: Run MongoDB tests with Docker
- `make test-mongo-also-local`: Run MongoDB + memory, pickle, maxage tests
- `make test-mongo-inmemory`: Run with in-memory MongoDB (existing)
- `make mongo-start/stop/logs`: Container management commands

### 4. **Documentation Updates**
- **README.rst**: Updated MongoDB testing section with all three options
- **CLAUDE.md**: Added MongoDB Local Testing section with examples
- **Draft documentation**: Comprehensive guide in `backlog/drafts/`

## Usage Examples

```bash
# MongoDB tests only (default)
./scripts/test-mongo-local.sh

# MongoDB + local core tests
./scripts/test-mongo-local.sh --mode also-local

# Keep container running for debugging
./scripts/test-mongo-local.sh --keep-running --verbose

# Using make
make test-mongo-local
make test-mongo-also-local
```

## Benefits

1. **CI Parity**: Developers can now test against the same MongoDB setup as CI
2. **Easy to Use**: Single command to run tests with automatic setup/teardown
3. **Flexible**: Multiple options (script, make, docker-compose) to suit different workflows
4. **Debugging Support**: `--keep-running` option helps debug test failures
5. **No Manual Setup**: No need to manually install MongoDB or manage containers

## Testing

- Script tested on macOS with Docker Desktop
- Handles missing Docker gracefully with helpful error messages
- Cleans up containers even on test failure
- Respects existing containers and port conflicts

## Future Enhancements

This pattern can be extended to other backends (Redis, SQL) for consistent local testing across all cachier backends.

---

Closes #[issue-number] (if applicable)