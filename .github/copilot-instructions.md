# Copilot Instructions for Cachier

Welcome to the Cachier codebase! Please follow these guidelines to ensure code suggestions, reviews, and contributions are robust, maintainable, and compatible with our multi-backend architecture.

## 1. Decorator and API Usage

- The main decorator is `@cachier`. It supports parameters such as `stale_after`, `backend`, `mongetter`, `cache_dir`, `pickle_reload`, `separate_files`, `wait_for_calc_timeout`, `allow_none`, and `hash_func`.
- Arguments to cached functions must be hashable; for unhashable arguments, provide a custom hash function via the `hash_func` parameter.
- The default backend is pickle-based, storing cache files in `~/.cachier/` unless otherwise specified. MongoDB and memory backends are also supported.
- Cachier is thread-safe and supports per-function cache clearing via the `clear_cache()` method on decorated functions.
- Global configuration is possible via `set_default_params`, `set_global_params`, and `enable_caching`/`disable_caching`.

## 2. Optional Dependencies and Backends

- Cachier supports multiple backends: `pickle`, `memory`, `mongo`, and `sql`.
- Not all dependencies are required for all backends. Code and tests for optional backends (e.g., MongoDB, SQL/SQLAlchemy) **must gracefully handle missing dependencies** and should not break import or test collection for other backends.
- Only raise errors or warnings for missing dependencies when the relevant backend is actually used (not at import time).

## 3. Testing Matrix and Markers

- Tests are located in the `tests/` directory and should be run with `pytest`.
- Tests are marked with `@pytest.mark.<backend>` (e.g., `@pytest.mark.sql`, `@pytest.mark.mongo`, `@pytest.mark.local`).
- The CI matrix runs different backends on different OSes. Do **not** assume all tests run on all platforms.
- MongoDB-related tests require either a mocked or live MongoDB instance.
- When adding new backends that require external services (e.g., databases), update the CI matrix and use Dockerized services as in the current MongoDB and PostgreSQL setup. Exclude backends from OSes where they are not supported.

## 4. Coverage, Linting, and Typing

- Code must pass `mypy`, `ruff`, and `pytest`.
- Use per-file or per-line ignores for known, justified issues (e.g., SQLAlchemy model base class typing, intentional use of `pickle`).
- All new code must include full type annotations and docstrings matching the style of the existing codebase.
- All docstrings should follow numpy docstring conventions.

## 5. Error Handling and Warnings

- Do **not** emit warnings at import time for missing optional dependencies. Only raise errors or warnings when the relevant backend is actually used.

## 6. Backward Compatibility

- Maintain backward compatibility for public APIs unless a breaking change is explicitly approved.
- Cachier supports Python 3.9+.

## 7. Documentation and Examples

- When adding a new backend or feature, provide:
  - Example usage in the README
  - At least one test for each public method
  - Documentation of any new configuration options
- For documentation, follow numpy docstring conventions and validate changes to `README.rst` with `python setup.py checkdocs`.

## 8. General Style

- Prefer concise, readable, and well-documented Python code.
- Follow the existing code style and conventions for imports, docstrings, and type annotations.
- Prefer explicit, readable code over cleverness.

## 9. The Code Base

### General structure

The repository contains a Python package called Cachier that provides persistent function caching with several backends:

cachier/
├── src/cachier/ # Main library code
│ ├── __init__.py
│ ├── core.py # Decorator logic, backend selection
│ ├── cores/ # Backend implementations
│ │ ├── pickle.py
│ │ ├── memory.py
│ │ ├── mongo.py
│ │ ├── sql.py
│ │ ├── redis.py
│ │ └── base.py
│ ├── config.py # Global/default config
│ ├── \_types.py # Type definitions
│ ├── _version.py
│ └── __main__.py
├── tests/ # Pytest-based tests, backend-marked
│ ├── test_\*.py
│ └── \*\_requirements.txt # Backend-specific test requirements
├── examples/ # Usage examples
├── README.rst # Main documentation
└── ...

### Key functionality

- core.py exposes the cachier decorator. It chooses a backend (pickle, mongo, memory, SQL, or Redis) and wraps the target function:

```python
backend = _update_with_defaults(backend, "backend")
mongetter = _update_with_defaults(mongetter, "mongetter")
if callable(mongetter):
    backend = "mongo"

if backend == "pickle":
    core = _PickleCore(...)
elif backend == "mongo":
    core = _MongoCore(...)
elif backend == "memory":
    core = _MemoryCore(...)
elif backend == "sql":
    core = _SQLCore(...)
elif backend == "redis":
    core = _RedisCore(
        hash_func=hash_func,
        redis_client=redis_client,
        wait_for_calc_timeout=wait_for_calc_timeout,
    )
else:
    raise ValueError("specified an invalid core: %s" % backend)
```

- Global defaults and cache-entry structures are defined in config.py:

```python
@dataclass
class Params:
    caching_enabled: bool = True
    hash_func: HashFunc = _default_hash_func
    backend: Backend = "pickle"
    mongetter: Optional[Mongetter] = None
    stale_after: timedelta = timedelta.max
    next_time: bool = False
    cache_dir: Union[str, os.PathLike] = field(default_factory=LazyCacheDir)
    pickle_reload: bool = True
    separate_files: bool = False
    wait_for_calc_timeout: int = 0
    allow_none: bool = False
```

- The project supports multiple backends; each resides under src/cachier/cores/ (e.g., redis.py, mongo.py, etc.). The Redis example demonstrates how to use one backend:

```python
import time
from datetime import timedelta

try:
    import redis

    from cachier import cachier
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Install with: pip install redis cachier")
    exit(1)


def setup_redis_client():
    """Set up a Redis client for caching."""
    try:
        # Connect to Redis (adjust host/port as needed)
        client = redis.Redis(
            host="localhost",
            port=6379,
            db=0,
            decode_responses=False,  # Important: keep as bytes for pickle
        )
        # Test connection
        client.ping()
        print("✓ Connected to Redis successfully")
        return client
    except redis.ConnectionError:
        print("✗ Could not connect to Redis")
        print("Make sure Redis is running on localhost:6379")
        print("Or install and start Redis with: docker run -p 6379:6379 redis")
        return None


def expensive_calculation(n):
    """Simulate an expensive calculation."""
    print(f"  Computing expensive_calculation({n})...")
    time.sleep(2)  # Simulate work
    return n * n + 42


def demo_basic_caching():
    """Demonstrate basic Redis caching."""
    print("\n=== Basic Redis Caching ===")

    @cachier(backend="redis", redis_client=setup_redis_client())
    def cached_calculation(n):
        return expensive_calculation(n)

    # First call - should be slow
    start = time.time()
    result1 = cached_calculation(5)
    time1 = time.time() - start
    print(f"First call: {result1} (took {time1:.2f}s)")

    # Second call - should be fast (cached)
    start = time.time()
    result2 = cached_calculation(5)
    time2 = time.time() - start
    print(f"Second call: {result2} (took {time2:.2f}s)")

    assert result1 == result2
    assert time2 < time1
    print("✓ Caching working correctly!")


def demo_stale_after():
    """Demonstrate stale_after functionality with Redis."""
    print("\n=== Stale After Demo ===")

    @cachier(
        backend="redis",
        redis_client=setup_redis_client(),
        stale_after=timedelta(seconds=3),
    )
    def time_sensitive_calculation(n):
        return expensive_calculation(n)

    # First call
    result1 = time_sensitive_calculation(10)
    print(f"First call: {result1}")

    # Second call within 3 seconds - should use cache
    result2 = time_sensitive_calculation(10)
    print(f"Second call (within 3s): {result2}")
    assert result1 == result2

    # Wait for cache to become stale
    print("Waiting 4 seconds for cache to become stale...")
    time.sleep(4)

    # Third call after 4 seconds - should recalculate
    result3 = time_sensitive_calculation(10)
    print(f"Third call (after 4s): {result3}")
    assert result3 != result1
    print("✓ Stale after working correctly!")


def demo_callable_client():
    """Demonstrate using a callable Redis client."""
    print("\n=== Callable Client Demo ===")

    def get_redis_client():
        """Get a Redis client."""
        return redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)

    @cachier(backend="redis", redis_client=get_redis_client)
    def cached_with_callable(n):
        return expensive_calculation(n)

    result1 = cached_with_callable(15)
    result2 = cached_with_callable(15)
    assert result1 == result2
    print(f"Callable client result: {result1}")
    print("✓ Callable client working correctly!")


def demo_cache_management():
    """Demonstrate cache management functions."""
    print("\n=== Cache Management Demo ===")

    @cachier(backend="redis", redis_client=setup_redis_client())
    def managed_calculation(n):
        return expensive_calculation(n)

    # Cache some values
    managed_calculation(20)
    managed_calculation(21)

    # Clear the cache
    managed_calculation.clear_cache()
    print("✓ Cache cleared successfully!")

    # Verify cache is empty
    start = time.time()
    result = managed_calculation(20)  # Should be slow again
    time_taken = time.time() - start
    print(f"After clearing cache: {result} (took {time_taken:.2f}s)")


def main():
    """Run all Redis core demonstrations."""
    print("Cachier Redis Core Demo")
    print("=" * 50)

    # Check if Redis is available
    client = setup_redis_client()
    if client is None:
        return

    try:
        demo_basic_caching()
        demo_stale_after()
        demo_callable_client()
        demo_cache_management()

        print("\n" + "=" * 50)
        print("✓ All Redis core demonstrations completed successfully!")
        print("\nKey benefits of Redis core:")
        print("- High-performance in-memory caching")
        print("- Cross-process and cross-machine caching")
        print("- Optional persistence with Redis configuration")
        print("- Built-in expiration and eviction policies")

    except Exception as e:
        print(f"\n✗ Demo failed with error: {e}")
    finally:
        # Clean up
        if client:
            client.close()


if __name__ == "__main__":
    main()
```

______________________________________________________________________

## 10. Contributing Guidelines

### Issue Reporting

- Before opening a new issue, search existing issues to avoid duplicates
- Use issue templates when available
- Provide clear reproduction steps for bugs
- Include environment details (OS, Python version, backend used)
- For feature requests, explain the use case and expected behavior

### Pull Request Process

1. **Fork and Branch**: Fork the repository and create a feature branch from `master`
2. **Make Changes**: Follow the code style guidelines and add tests for new features
3. **Run Tests**: Ensure all tests pass locally before submitting
   - Run `pytest` for all tests
   - Run `pytest -m <backend>` for backend-specific tests
   - Run `ruff check .` for linting
   - Run `mypy src/cachier/` for type checking
4. **Commit**: Use clear, descriptive commit messages
5. **Submit PR**: Reference related issues and provide a clear description of changes
6. **Code Review**: Address review feedback and re-run tests as needed
7. **Merge**: Maintainers will merge once approved

### Development Workflow

- **Branch Naming**: Use descriptive names like `feature/add-redis-backend`, `fix/mongo-connection-issue`, `docs/update-readme`
- **Commit Messages**: Use clear, present-tense messages (e.g., "Add Redis backend support", not "Added Redis backend support")
- **Testing**: Always add tests for new features or bug fixes
- **Documentation**: Update README.rst and docstrings for new features

### Release Process

- Releases are managed by maintainers
- Version numbers follow semantic versioning (MAJOR.MINOR.PATCH)
- Release notes are generated from PR titles and descriptions
- PyPI releases are automated via GitHub Actions

______________________________________________________________________

Thank you for contributing to Cachier! These guidelines help ensure a robust, maintainable, and user-friendly package for everyone.
