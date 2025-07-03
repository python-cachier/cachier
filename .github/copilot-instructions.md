# GitHub Copilot Custom Instructions for Cachier

- Cachier is a Python package providing persistent, stale-free memoization decorators for Python functions, supporting local (pickle), cross-machine (MongoDB), and in-memory caching backends.
- Always refer to the main decorator as `@cachier`, and note that it can be configured via parameters such as `stale_after`, `backend`, `mongetter`, `cache_dir`, `pickle_reload`, `separate_files`, `wait_for_calc_timeout`, and `allow_none`.
- Arguments to cached functions must be hashable; custom hash functions can be provided via the `hash_func` parameter for unhashable arguments.
- The default backend is pickle-based, storing cache files in `~/.cachier/` unless otherwise specified. MongoDB and memory backends are also supported.
- Cachier is thread-safe and supports per-function cache clearing via the `clear_cache()` method on decorated functions.
- Global configuration is possible via `set_default_params` and `enable_caching`/`disable_caching` functions.
- When reviewing code, ensure new features or bugfixes maintain compatibility with Python 3.9+, preserve thread safety, and follow the numpy docstring conventions for documentation.
- Tests are located in the `tests/` directory and should be run with `pytest`. MongoDB-related tests require either a mocked or live MongoDB instance.
- When discussing or generating code, prefer concise, readable, and well-documented Python code, and follow the established conventions in the codebase and README.
- For documentation, follow numpy docstring conventions and validate changes to `README.rst` with `python setup.py checkdocs`.
