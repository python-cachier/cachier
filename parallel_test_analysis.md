# Analysis: How Parallel Tests for MongoDB, Redis, and SQL Backends Avoid Conflicts

## Key Findings

The parallel tests for MongoDB, Redis, and SQL backends successfully avoid conflicts through several mechanisms:

### 1. **Unique Function Names Per Cache Key**

Cache keys in Cachier are generated using a combination of:
- **Module name**: `func.__module__`
- **Function name**: `func.__name__`
- **Function arguments**: Hashed via the `hash_func`

From `src/cachier/cores/base.py`:
```python
def _get_func_str(func: Callable) -> str:
    return f".{func.__module__}.{func.__name__}"
```

This means:
- Each backend prefixes cache entries with the full module path and function name
- Redis: `{prefix}:{func_str}:{key}` (e.g., `cachier:.tests.test_redis_core._test_redis_caching:hash123`)
- MongoDB: Documents with `{"func": func_str, "key": key}`
- SQL: Rows with `function_id = func_str` and `key = key`

### 2. **Function Name Isolation Within Test Files**

Looking at the test files:
- Functions within each test function are **locally scoped**
- Even if multiple tests use `def f(x)` or `def _test_func()`, they are different function objects
- Each function gets a unique module path because they're defined inside different test functions

Examples:
```python
# In test_sql_core.py
def test_sql_core_basic():
    @cachier(backend="sql", sql_engine=SQL_CONN_STR)
    def f(x, y):  # This f is local to test_sql_core_basic
        return random() + x + y

def test_sql_core_keywords():
    @cachier(backend="sql", sql_engine=SQL_CONN_STR)
    def f(x, y):  # This f is different from the one above
        return random() + x + y
```

### 3. **Clear Cache Operations**

Most tests start with `func.clear_cache()` which removes all entries for that specific function:
- MongoDB: `delete_many(filter={"func": self._func_str})`
- Redis: Deletes all keys matching pattern `{prefix}:{func_str}:*`
- SQL: `delete(CacheTable).where(CacheTable.function_id == self._func_str)`

### 4. **Backend-Specific Isolation**

#### MongoDB:
- Uses a collection name that includes platform and Python version: `cachier_test_{platform}_{python_version}`
- Each function's entries are filtered by `func` field

#### Redis:
- Uses key prefixes that include the full function path
- Pattern-based operations only affect keys for specific functions

#### SQL:
- Uses `function_id` column to separate entries by function
- Composite operations use both `function_id` and `key`

### 5. **Test Fixtures for Additional Isolation**

From `tests/conftest.py`:
```python
@pytest.fixture(autouse=True)
def isolated_cache_directory(tmp_path, monkeypatch, request, worker_id):
    """Ensure each test gets an isolated cache directory."""
    if "pickle" in request.node.keywords:
        # Create unique cache directory per worker
        if worker_id == "master":
            cache_dir = tmp_path / "cachier_cache"
        else:
            cache_dir = tmp_path / f"cachier_cache_{worker_id}"
```

### 6. **No Shared Function Names Across Test Files**

Analysis shows:
- Test functions have unique names across files (no duplicate `test_*` function names)
- Cached functions are either:
  - Defined locally within test functions (most common)
  - Given unique names when defined at module level (e.g., `_test_redis_caching`, `_test_mongo_caching`)

### 7. **Argument-Based Key Differentiation**

Even if two tests used the same function name (which they don't), different arguments would create different cache keys:
- Tests use different argument values (e.g., `(1, 2)`, `(34, 82.3)`, etc.)
- The hash function ensures different arguments → different keys

## Conclusion

The parallel tests avoid conflicts through:
1. **Function name namespacing** - Full module path included in cache keys
2. **Local function scope** - Functions defined inside test functions are unique objects
3. **Clear cache operations** - Tests clean up their own function's cache
4. **Backend-specific key prefixing** - Each backend uses function-specific prefixes/filters
5. **Test isolation fixtures** - Separate cache directories for pickle backend
6. **No naming collisions** - Test authors have been careful to use unique function names

This design allows tests to run in parallel without interfering with each other, as each test operates on its own namespace within the cache backends.