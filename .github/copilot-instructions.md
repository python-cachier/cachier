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

______________________________________________________________________

Thank you for contributing to Cachier! These guidelines help ensure a robust, maintainable, and user-friendly package for everyone.
