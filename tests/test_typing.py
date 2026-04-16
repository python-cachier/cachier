"""Tests that the @cachier decorator preserves function type signatures.

These tests invoke mypy programmatically and assert that decorated functions retain their parameter types and return
types as seen by static analysis.

"""

import textwrap

import pytest

mypy_api = pytest.importorskip("mypy.api", reason="mypy is required for typing tests")


def _run_mypy(code: str) -> tuple[list[str], list[str]]:
    """Run mypy on a code snippet and return (notes, errors).

    Parameters
    ----------
    code : str
        Python source code to type-check.

    Returns
    -------
    tuple[list[str], list[str]]
        A tuple of (note lines, error lines) from mypy output.

    """
    result = mypy_api.run(
        [
            "-c",
            textwrap.dedent(code),
            "--no-error-summary",
            "--hide-error-context",
            "--ignore-missing-imports",
        ]
    )
    stdout = result[0]
    notes = []
    errors = []
    for line in stdout.splitlines():
        if ": note:" in line:
            notes.append(line)
        elif ": error:" in line:
            errors.append(line)
    return notes, errors


_POSITIVE_SNIPPET = """
    import asyncio
    from datetime import timedelta
    from typing import Optional

    from cachier import cachier

    # sync_return_type: decorated function's return type preserved
    @cachier()
    def sync_return(x: int) -> str:
        return str(x)

    reveal_type(sync_return(5))
    reveal_type(sync_return)

    # sync_params: multi-arg signature preserved
    @cachier()
    def sync_params(x: int, y: str) -> list[str]:
        return [y] * x

    reveal_type(sync_params)

    # async_return: awaited return type preserved
    @cachier()
    async def async_fetch(url: str) -> bytes:
        return b"data"

    async def _async_caller() -> None:
        result = await async_fetch("http://example.com")
        reveal_type(result)

    reveal_type(async_fetch)

    # optional_params: Optional preserved
    @cachier()
    def optional_greet(name: str, greeting: Optional[str] = None) -> str:
        return f"{greeting or 'Hello'}, {name}"

    reveal_type(optional_greet)

    # generic_return: parametrized dict preserved
    @cachier()
    def make_mapping(keys: list[str], value: int) -> dict[str, int]:
        return {k: value for k in keys}

    reveal_type(make_mapping(["a"], 1))

    # none_return: None return preserved
    @cachier()
    def side_effect(x: int) -> None:
        return None

    reveal_type(side_effect(1))

    # decorator_args: typing works with explicit backend
    @cachier(backend="memory")
    def compute(x: float) -> float:
        return x * 2.0

    reveal_type(compute(1.0))

    # decorator_args: typing works with stale_after
    @cachier(stale_after=timedelta(hours=1))
    def lookup(key: str) -> list[int]:
        return [1, 2, 3]

    reveal_type(lookup("x"))

    # attributes: cache-management API visible
    @cachier()
    def attr_fn(x: int) -> int:
        return x

    attr_fn.clear_cache()
    attr_fn.precache_value(1, value_to_cache=42)
    _m = attr_fn.metrics
"""


@pytest.fixture(scope="module")
def positive_mypy_output() -> tuple[list[str], list[str]]:
    """Run mypy once on a combined snippet of all positive typing assertions.

    Consolidating the positive cases into a single mypy invocation avoids per-test mypy startup cost. Negative tests
    (which assert that errors are reported) stay isolated so each one can verify the error originates from its own
    snippet.

    """
    return _run_mypy(_POSITIVE_SNIPPET)


class TestSyncTyping:
    """Verify that synchronous decorated functions preserve types."""

    def test_return_type_preserved(self, positive_mypy_output) -> None:
        """Mypy should infer the original return type through @cachier."""
        notes, errors = positive_mypy_output
        assert not errors
        assert any('"str"' in n for n in notes)

    def test_param_types_preserved(self, positive_mypy_output) -> None:
        """Mypy should see the original parameter types through @cachier."""
        notes, errors = positive_mypy_output
        assert not errors
        assert any("[x: int, y: str]" in n and "list[str]" in n for n in notes)

    def test_wrong_arg_type_is_error(self) -> None:
        """Mypy should reject calls with wrong argument types."""
        _notes, errors = _run_mypy("""
            from cachier import cachier

            @cachier()
            def add(a: int, b: int) -> int:
                return a + b

            add("not", "ints")
        """)
        assert errors

    def test_return_type_mismatch_is_error(self) -> None:
        """Mypy should catch assigning the result to an incompatible type."""
        _notes, errors = _run_mypy("""
            from cachier import cachier

            @cachier()
            def get_name() -> str:
                return "hello"

            x: int = get_name()
        """)
        assert errors


class TestAsyncTyping:
    """Verify that async decorated functions preserve types."""

    def test_async_return_type_preserved(self, positive_mypy_output) -> None:
        """Mypy should infer the awaited return type for async functions."""
        notes, errors = positive_mypy_output
        assert not errors
        assert any('"bytes"' in n for n in notes)

    def test_async_signature_preserved(self, positive_mypy_output) -> None:
        """Mypy should see the async function as a coroutine."""
        notes, errors = positive_mypy_output
        assert not errors
        assert any("Coroutine" in n for n in notes)

    def test_async_wrong_arg_type_is_error(self) -> None:
        """Mypy should reject calls with wrong argument types for async."""
        _notes, errors = _run_mypy("""
            from cachier import cachier

            @cachier()
            async def fetch(url: str) -> bytes:
                return b"data"

            async def main() -> None:
                await fetch(123)
        """)
        assert errors


class TestComplexSignatures:
    """Verify preservation of more complex type signatures."""

    def test_optional_params(self, positive_mypy_output) -> None:
        """Mypy should preserve Optional parameter types."""
        notes, errors = positive_mypy_output
        assert not errors
        assert any("name: str, greeting: str | None" in n for n in notes)

    def test_generic_return_type(self, positive_mypy_output) -> None:
        """Mypy should preserve generic return types like dict."""
        notes, errors = positive_mypy_output
        assert not errors
        assert any("dict[str, int]" in n for n in notes)

    def test_none_return_type(self, positive_mypy_output) -> None:
        """Mypy should preserve None return type."""
        notes, errors = positive_mypy_output
        assert not errors
        assert any('"None"' in n for n in notes)


class TestDecoratorAttributes:
    """Verify that cache-management attributes are visible to type checkers."""

    def test_attributes_do_not_error(self, positive_mypy_output) -> None:
        """Using ``.clear_cache()``, ``.precache_value()``, ``.metrics`` should not raise type errors."""
        _notes, errors = positive_mypy_output
        assert not errors

    def test_undefined_attribute_is_error(self) -> None:
        """Mypy should reject access to attributes that do not exist."""
        _notes, errors = _run_mypy("""
            from cachier import cachier

            @cachier()
            def f(x: int) -> int:
                return x

            f.not_a_real_method()
        """)
        assert errors


class TestDecoratorWithArgs:
    """Verify typing works with various decorator arguments."""

    def test_with_backend_arg(self, positive_mypy_output) -> None:
        """Type preservation should work with explicit backend selection."""
        notes, errors = positive_mypy_output
        assert not errors
        assert any('"float"' in n for n in notes)

    def test_with_stale_after_arg(self, positive_mypy_output) -> None:
        """Type preservation should work with stale_after parameter."""
        notes, errors = positive_mypy_output
        assert not errors
        assert any("list[int]" in n for n in notes)
