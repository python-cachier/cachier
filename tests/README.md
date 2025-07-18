# Cachier Test Suite Documentation

This document provides comprehensive guidelines for writing and running tests for the Cachier package.

## Table of Contents

1. [Test Suite Overview](#test-suite-overview)
2. [Test Structure](#test-structure)
3. [Running Tests](#running-tests)
4. [Writing Tests](#writing-tests)
5. [Test Isolation](#test-isolation)
6. [Backend-Specific Testing](#backend-specific-testing)
7. [Parallel Testing](#parallel-testing)
8. [CI/CD Integration](#cicd-integration)
9. [Troubleshooting](#troubleshooting)

## Test Suite Overview

The Cachier test suite is designed to comprehensively test all caching backends while maintaining proper isolation between tests. The suite uses pytest with custom markers for backend-specific tests.

### Supported Backends

- **Memory**: In-memory caching (no external dependencies)
- **Pickle**: File-based caching using pickle (default backend)
- **MongoDB**: Database caching using MongoDB
- **Redis**: In-memory data store caching
- **SQL**: SQL database caching via SQLAlchemy (PostgreSQL, SQLite, MySQL)

### Test Categories

1. **Core Functionality**: Basic caching operations (get, set, clear)
2. **Stale Handling**: Testing `stale_after` parameter
3. **Concurrency**: Thread-safety and multi-process tests
4. **Error Handling**: Exception scenarios and recovery
5. **Performance**: Speed and efficiency tests
6. **Integration**: Cross-backend compatibility

## Test Structure

```
tests/
├── conftest.py                    # Shared fixtures and configuration
├── requirements.txt               # Base test dependencies
├── mongodb_requirements.txt       # MongoDB-specific dependencies
├── redis_requirements.txt         # Redis-specific dependencies
├── sql_requirements.txt           # SQL-specific dependencies
│
├── test_*.py                      # Test modules
├── test_mongo_core.py            # MongoDB-specific tests
├── test_redis_core.py            # Redis-specific tests
├── test_sql_core.py              # SQL-specific tests
├── test_memory_core.py           # Memory backend tests
├── test_pickle_core.py           # Pickle backend tests
├── test_general.py               # Cross-backend tests
└── ...
```

### Test Markers

Tests are marked with backend-specific markers:

```python
@pytest.mark.mongo     # MongoDB tests
@pytest.mark.redis     # Redis tests
@pytest.mark.sql       # SQL tests
@pytest.mark.memory    # Memory backend tests
@pytest.mark.pickle    # Pickle backend tests
@pytest.mark.maxage    # Tests involving stale_after functionality
```

## Running Tests

### Quick Start

```bash
# Run all tests
pytest

# Run tests for specific backend
pytest -m mongo
pytest -m redis
pytest -m sql

# Run tests for multiple backends
pytest -m "mongo or redis"

# Exclude specific backends
pytest -m "not mongo"

# Run with verbose output
pytest -v
```

### Using the Test Script

The recommended way to run tests with proper backend setup:

```bash
# Test single backend
./scripts/test-local.sh mongo

# Test multiple backends
./scripts/test-local.sh mongo redis sql

# Test all backends
./scripts/test-local.sh all

# Run tests in parallel
./scripts/test-local.sh all -p

# Keep containers running for debugging
./scripts/test-local.sh mongo redis -k
```

### Parallel Testing

Tests can be run in parallel using pytest-xdist:

```bash
# Run with automatic worker detection
./scripts/test-local.sh all -p

# Specify number of workers
./scripts/test-local.sh all -p -w 4

# Or directly with pytest
pytest -n auto
pytest -n 4
```

## Writing Tests

### Basic Test Structure

```python
import pytest
from cachier import cachier

def test_basic_caching():
    """Test basic caching functionality."""
    # Define a cached function local to this test
    @cachier()
    def expensive_computation(x):
        return x ** 2
    
    # First call - should compute
    result1 = expensive_computation(5)
    assert result1 == 25
    
    # Second call - should return from cache
    result2 = expensive_computation(5)
    assert result2 == 25
    
    # Clear cache for cleanup
    expensive_computation.clear_cache()
```

### Backend-Specific Tests

```python
@pytest.mark.mongo
def test_mongo_specific_feature():
    """Test MongoDB-specific functionality."""
    from tests.test_mongo_core import _test_mongetter
    
    @cachier(mongetter=_test_mongetter)
    def mongo_cached_func(x):
        return x * 2
    
    # Test implementation
    assert mongo_cached_func(5) == 10
```

## Test Isolation

### Critical Rule: Function Isolation

**Never share cachier-decorated functions between test functions.** Each test must have its own decorated function to ensure proper isolation.

#### Why This Matters

Cachier identifies cached functions by their full module path and function name. When tests share decorated functions:
- Cache entries can conflict between tests
- Parallel test execution may fail unpredictably
- Test results become non-deterministic

#### Good Practice

```python
def test_feature_one():
    @cachier()
    def compute_one(x):  # Unique to this test
        return x * 2
    
    assert compute_one(5) == 10

def test_feature_two():
    @cachier()
    def compute_two(x):  # Different function for different test
        return x * 2
    
    assert compute_two(5) == 10
```

#### Bad Practice

```python
# DON'T DO THIS!
@cachier()
def shared_compute(x):  # Shared between tests
    return x * 2

def test_feature_one():
    assert shared_compute(5) == 10  # May conflict with test_feature_two

def test_feature_two():
    assert shared_compute(5) == 10  # May conflict with test_feature_one
```

### Isolation Mechanisms

1. **Pickle Backend**: Uses `isolated_cache_directory` fixture that creates unique directories per pytest-xdist worker
2. **External Backends**: Rely on function namespacing (module + function name)
3. **Clear Cache**: Always clear cache at test end for cleanup

### Best Practices for Isolation

1. Define cached functions inside test functions
2. Use unique, descriptive function names
3. Clear cache after each test
4. Avoid module-level cached functions in tests
5. Use fixtures for common setup/teardown

## Backend-Specific Testing

### MongoDB Tests

```python
@pytest.mark.mongo
def test_mongo_feature():
    """Test with MongoDB backend."""
    @cachier(mongetter=_test_mongetter, wait_for_calc_timeout=2)
    def mongo_func(x):
        return x
    
    # MongoDB-specific assertions
    assert mongo_func.get_cache_mongetter() is not None
```

### Redis Tests

```python
@pytest.mark.redis
def test_redis_feature():
    """Test with Redis backend."""
    @cachier(backend='redis', redis_client=_test_redis_client)
    def redis_func(x):
        return x
    
    # Redis-specific testing
    assert redis_func(5) == 5
```

### SQL Tests

```python
@pytest.mark.sql
def test_sql_feature():
    """Test with SQL backend."""
    @cachier(backend='sql', sql_engine=test_engine)
    def sql_func(x):
        return x
    
    # SQL-specific testing
    assert sql_func(5) == 5
```

### Memory Tests

```python
@pytest.mark.memory
def test_memory_feature():
    """Test with memory backend."""
    @cachier(backend='memory')
    def memory_func(x):
        return x
    
    # Memory-specific testing
    assert memory_func(5) == 5
```

## Parallel Testing

### How It Works

1. pytest-xdist creates multiple worker processes
2. Each worker gets a subset of tests
3. Cachier's function identification ensures natural isolation
4. Pickle backend uses worker-specific cache directories

### Running Parallel Tests

```bash
# Automatic worker detection
./scripts/test-local.sh all -p

# Specify workers
./scripts/test-local.sh all -p -w 4

# Direct pytest command
pytest -n auto
```

### Parallel Testing Considerations

1. **Resource Usage**: More workers = more CPU/memory usage
2. **External Services**: Ensure Docker has sufficient resources
3. **Test Output**: May be interleaved; use `-v` for clarity
4. **Debugging**: Harder with parallel execution; use `-n 1` for debugging

## CI/CD Integration

### GitHub Actions

The CI pipeline tests all backends:

```yaml
# Local backends run in parallel
pytest -m "memory or pickle" -n auto

# External backends run sequentially for stability
pytest -m mongo
pytest -m redis
pytest -m sql
```

### Environment Variables

- `CACHIER_TEST_VS_DOCKERIZED_MONGO`: Use real MongoDB in CI
- `CACHIER_TEST_REDIS_HOST`: Redis connection details
- `SQLALCHEMY_DATABASE_URL`: SQL database connection

## Troubleshooting

### Common Issues

1. **Import Errors**: Install backend-specific requirements
   ```bash
   pip install -r tests/redis_requirements.txt
   ```

2. **Docker Not Running**: Start Docker Desktop or daemon
   ```bash
   docker ps  # Check if Docker is running
   ```

3. **Port Conflicts**: Stop conflicting services
   ```bash
   docker stop cachier-test-mongo cachier-test-redis cachier-test-postgres
   ```

4. **Flaky Tests**: Usually due to timing issues
   - Increase timeouts
   - Add proper waits
   - Check for race conditions

5. **Cache Conflicts**: Ensure function isolation
   - Don't share decorated functions
   - Clear cache after tests
   - Use unique function names

### Debugging Tips

1. **Run Single Test**: 
   ```bash
   pytest -k test_name -v
   ```

2. **Disable Parallel**: 
   ```bash
   pytest -n 1
   ```

3. **Check Logs**:
   ```bash
   docker logs cachier-test-mongo
   ```

4. **Interactive Debugging**:
   ```python
   import pdb; pdb.set_trace()
   ```

### Performance Considerations

1. **Test Speed**: Memory/pickle tests are fastest
2. **External Backends**: Add overhead for Docker/network
3. **Parallel Execution**: Speeds up test suite significantly
4. **Cache Size**: Large caches slow down tests

## Best Practices Summary

1. **Always** define cached functions inside test functions
2. **Never** share cached functions between tests
3. **Clear** cache after each test
4. **Use** appropriate markers for backend-specific tests
5. **Run** full test suite before submitting PRs
6. **Test** with parallel execution to catch race conditions
7. **Document** any special test requirements
8. **Follow** existing test patterns in the codebase

## Adding New Tests

When adding new tests:

1. Follow existing naming conventions
2. Add appropriate backend markers
3. Ensure function isolation
4. Include docstrings explaining test purpose
5. Test both success and failure cases
6. Consider edge cases and error conditions
7. Run with all backends if applicable
8. Update this documentation if needed

## Questions or Issues?

- Check existing tests for examples
- Review the main README.rst
- Open an issue on GitHub
- Contact maintainers listed in README.rst