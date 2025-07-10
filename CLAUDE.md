# CLAUDE.md

## 📦 Project Overview

**Cachier** is a Python library providing persistent, stale-free, local and cross-machine caching for Python functions via a decorator API. It supports multiple backends (pickle, memory, MongoDB, SQL, Redis), is thread-safe, and is designed for extensibility and robust cross-platform support.

- **Repository:** [python-cachier/cachier](https://github.com/python-cachier/cachier)
- **Primary Language:** Python 3.9+
- **Key Dependencies:** `portalocker`, `watchdog` (optional: `pymongo`, `sqlalchemy`, `redis`)
- **Test Framework:** `pytest`
- **Linting:** `ruff`
- **Type Checking:** `mypy`
- **CI:** GitHub Actions (matrix for backends/OS)
- **Issue Tracking:** GitHub Issues

______________________________________________________________________

## 🗂️ Repository Structure

```
cachier/
├── src/cachier/           # Main library code
│   ├── __init__.py
│   ├── core.py            # Decorator logic, backend selection
│   ├── cores/             # Backend implementations
│   │   ├── pickle.py
│   │   ├── memory.py
│   │   ├── mongo.py
│   │   ├── sql.py
│   │   ├── redis.py
│   │   └── base.py
│   ├── config.py          # Global/default config
│   ├── _types.py          # Type definitions
│   ├── _version.py
│   └── __main__.py
├── tests/                 # Pytest-based tests, backend-marked
│   ├── test_*.py
│   └── *_requirements.txt # Backend-specific test requirements
├── examples/              # Usage examples
├── README.rst             # Main documentation
├── pyproject.toml         # Build, lint, type, test config
├── .pre-commit-config.yaml
├── .github/               # CI, issue templates, workflows
└── ... (see full tree above)
```

______________________________________________________________________

## 🚦 Quick Start

1. **Install core dependencies:**

   ```bash
   pip install .[all]
   ```

   - For backend-specific dev: see `tests/*_requirements.txt`.

2. **Run tests:**

   ```bash
   pytest
   ```

3. **Lint and type-check:**

   ```bash
   ruff check .
   mypy src/cachier/
   ```

4. **Try an example:**

   ```bash
   python examples/redis_example.py
   ```

______________________________________________________________________

## 🧑‍💻 Development Guidelines

### 1. **Code Style & Quality**

- **Python 3.9+** only.
- **Type annotations** required for all new code.
- **Docstrings:** Use numpy style, multi-line, no single-line docstrings.
- **Lint:** Run `ruff` before PRs. Use per-line/file ignores only for justified cases.
- **Type check:** Run `mypy` before PRs.
- **Testing:** All public methods must have at least one test. Use `pytest.mark.<backend>` for backend-specific tests.
- **No warnings/errors for missing optional dependencies at import time.** Only raise when backend is used.

### 2. **Backends**

- **Default:** Pickle (local file cache, `~/.cachier/`)
- **Others:** Memory, MongoDB, SQL, Redis
- **Adding a backend:** Implement in `src/cachier/cores/`, subclass `BaseCore`, add tests, update docs, and CI matrix if needed.
- **Optional dependencies:** Code/tests must gracefully skip if backend deps are missing.

### 3. **Decorator Usage**

- Main API: `@cachier`
- Key params: `stale_after`, `backend`, `mongetter`, `cache_dir`, `pickle_reload`, `separate_files`, `wait_for_calc_timeout`, `allow_none`, `hash_func`
- Arguments to cached functions must be hashable. For unhashable, provide `hash_func`.

### 4. **Testing**

- **Run all tests:** `pytest`
- **Backend-specific:** Use markers, e.g. `pytest -m mongo`
- **Requirements:** See `tests/*_requirements.txt` for backend test deps.
- **CI:** Matrix covers OS/backend combinations. Mongo/SQL require Dockerized services.

### 5. **Documentation**

- **README.rst** is the canonical user/developer doc.
- **New features/backends:** Update README, add usage example, document config options.
- **Doc validation:** `python setup.py checkdocs`

### 6. **Error Handling**

- **No import-time warnings for missing optional deps.**
- **Raise errors/warnings only when backend is used.**
- **Graceful fallback/skip for missing backend deps in tests.**

### 7. **Backward Compatibility**

- **Public API must remain backward compatible** unless breaking change is approved.
- **Support for Python 3.9+ only.**

### 8. **Global Configuration**

- Use `set_default_params`, `set_global_params`, `enable_caching`, `disable_caching` for global config.

______________________________________________________________________

## 🛠️ Common Bash & MCP Commands

- **Install all dev dependencies:**
  ```bash
  pip install .[all]
  pip install -r tests/requirements.txt
  ```
- **Run all tests:** `pytest`
- **Run backend-specific tests:** `pytest -m <backend>`
- **Lint:** `ruff check .`
- **Type check:** `mypy src/cachier/`
- **Format:** `ruff format .`
- **Pre-commit:** `pre-commit run --all-files`
- **Build package:** `python -m build`
- **Check docs:** `python setup.py checkdocs`
- **Run example:** `python examples/redis_example.py`
- **Update requirements:** Edit `tests/*_requirements.txt` as needed.

______________________________________________________________________

## 🧩 Claude Code Integration

### a. **File Navigation & Context**

- **Core logic:** `src/cachier/core.py`
- **Backends:** `src/cachier/cores/`
- **Config:** `src/cachier/config.py`
- **Types:** `src/cachier/_types.py`
- **Tests:** `tests/`
- **Examples:** `examples/`
- **Docs:** `README.rst`

### b. **Best Practices for Claude**

- **Always check for backend-specific requirements** before running backend tests or code.
- **When adding a backend:** Update all relevant places (core, tests, docs, CI).
- **When editing core logic:** Ensure all backends are still supported and tested.
- **When updating the decorator API:** Update docstrings, README, and tests.
- **When adding config options:** Update `config.py`, docstrings, README, and add tests.
- **When changing global config:** Ensure backward compatibility and update docs.

### c. **Claude-Specific Tips**

- **Use MCP for git operations** (commits, pushes, PRs) instead of CLI.
- **When in doubt, prefer explicit, readable code over cleverness.**
- **Never use non-ASCII characters or the em dash.**
- **If stuck, suggest opening a new chat with latest context.**
- **If adding new dependencies, use context7 MCP to get latest versions.**
- **Always check GitHub Issues before starting new features/PRs.**
- **Create a relevant issue for every new PR.**
- **Use per-file or per-line ignores for mypy/ruff only when justified.**
- **All new code must have full type annotations and numpy-style docstrings.**

______________________________________________________________________

## 🧪 Testing Matrix & Markers

- **Markers:** `@pytest.mark.<backend>` (e.g., `@pytest.mark.sql`)
- **Mongo/SQL:** Require Dockerized services for CI.
- **Tests must not break if optional backend deps are missing.**
- **CI matrix:** See `.github/workflows/` for details.

______________________________________________________________________

## 📝 Documentation & Examples

- **README.rst:** Main user/developer doc. Update for new features/backends.
- **Examples:** Add usage examples for new features/backends in `examples/`.
- **Docstrings:** Numpy style, multi-line, no single-line docstrings.

______________________________________________________________________

## 🛡️ Security & Performance

- **No secrets in code or tests.**
- **Do not emit warnings/errors for missing optional deps at import time.**
- **Thread safety:** All backends must be thread-safe.
- **Performance:** Avoid unnecessary serialization/deserialization.

______________________________________________________________________

## 🏷️ Branching & Workflow

- **Workflow:** Issue → Feature branch → GitHub PR
- **Branch naming:** `feature/<desc>`, `bugfix/<desc>`, etc.
- **PRs:** Reference relevant issue, link to tests/docs as needed.
- **Commits:** Use MCP tools, not direct git CLI.

______________________________________________________________________

## 🧭 Quick Reference

| Task                       | Command/Location            |
| -------------------------- | --------------------------- |
| Run all tests              | `pytest`                    |
| Run backend-specific tests | `pytest -m <backend>`       |
| Lint                       | `ruff check .`              |
| Type check                 | `mypy src/cachier/`         |
| Format code                | `ruff format .`             |
| Build package              | `python -m build`           |
| Check docs                 | `python setup.py checkdocs` |
| Update requirements        | `tests/*_requirements.txt`  |
| Main decorator             | `src/cachier/core.py`       |
| Backends                   | `src/cachier/cores/`        |
| Global config              | `src/cachier/config.py`     |
| Tests                      | `tests/`                    |
| Examples                   | `examples/`                 |
| Documentation              | `README.rst`                |

______________________________________________________________________

## 🧠 Claude Code: Special Instructions

- **This file is commited to the repository and so should never include any secrets.**
- **Always read this file and the README.rst before making changes.**
- **When adding new features/backends, update all relevant docs, tests, and CI.**
- **If a test fails due to missing optional dependency, skip gracefully.**
- **Never emit warnings/errors for missing optional deps at import time.**
- **All code must be Python 3.9+ compatible.**
- **All new code must have full type annotations and numpy-style docstrings.**
- **If you are unsure about a pattern, check the README and this file first.**
- **If you are stuck, suggest opening a new chat with the latest context.**

______________________________________________________________________

## 🏁 Final Notes

- **This file is the canonical quick reference for Claude Code and human contributors.**
- **Update this file whenever project conventions, workflows, or best practices change.**
- **Keep this file concise, actionable, and up-to-date.**
- **For detailed documentation, see README.rst and the codebase.**
- **This file is commited to the repository and so should never include any secrets.**
