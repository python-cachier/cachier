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


class TestSyncTyping:
    """Verify that synchronous decorated functions preserve types."""

    def test_return_type_preserved(self) -> None:
        """Mypy should infer the original return type through @cachier."""
        notes, errors = _run_mypy("""
            from cachier import cachier

            @cachier()
            def my_func(x: int) -> str:
                return str(x)

            reveal_type(my_func(5))
        """)
        assert not errors
        assert any('"str"' in n for n in notes)

    def test_param_types_preserved(self) -> None:
        """Mypy should see the original parameter types through @cachier."""
        notes, errors = _run_mypy("""
            from cachier import cachier

            @cachier()
            def my_func(x: int, y: str) -> list[str]:
                return [y] * x

            reveal_type(my_func)
        """)
        assert not errors
        assert any("int" in n and "str" in n for n in notes)

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

    def test_async_return_type_preserved(self) -> None:
        """Mypy should infer the awaited return type for async functions."""
        notes, errors = _run_mypy("""
            import asyncio
            from cachier import cachier

            @cachier()
            async def fetch(url: str) -> bytes:
                return b"data"

            async def main() -> None:
                result = await fetch("http://example.com")
                reveal_type(result)

            asyncio.run(main())
        """)
        assert not errors
        assert any('"bytes"' in n for n in notes)

    def test_async_signature_preserved(self) -> None:
        """Mypy should see the async function as a coroutine."""
        notes, errors = _run_mypy("""
            from cachier import cachier

            @cachier()
            async def fetch(url: str) -> bytes:
                return b"data"

            reveal_type(fetch)
        """)
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

    def test_optional_params(self) -> None:
        """Mypy should preserve Optional parameter types."""
        notes, errors = _run_mypy("""
            from typing import Optional
            from cachier import cachier

            @cachier()
            def greet(name: str, greeting: Optional[str] = None) -> str:
                return f"{greeting or 'Hello'}, {name}"

            reveal_type(greet)
        """)
        assert not errors
        assert any("str" in n for n in notes)

    def test_generic_return_type(self) -> None:
        """Mypy should preserve generic return types like dict."""
        notes, errors = _run_mypy("""
            from cachier import cachier

            @cachier()
            def make_mapping(keys: list[str], value: int) -> dict[str, int]:
                return {k: value for k in keys}

            reveal_type(make_mapping(["a"], 1))
        """)
        assert not errors
        assert any("dict[str, int]" in n for n in notes)

    def test_none_return_type(self) -> None:
        """Mypy should preserve None return type."""
        notes, errors = _run_mypy("""
            from cachier import cachier

            @cachier()
            def side_effect(x: int) -> None:
                pass

            reveal_type(side_effect(1))
        """)
        assert not errors
        assert any('"None"' in n for n in notes)


class TestDecoratorWithArgs:
    """Verify typing works with various decorator arguments."""

    def test_with_backend_arg(self) -> None:
        """Type preservation should work with explicit backend selection."""
        notes, errors = _run_mypy("""
            from cachier import cachier

            @cachier(backend="memory")
            def compute(x: float) -> float:
                return x * 2.0

            reveal_type(compute(1.0))
        """)
        assert not errors
        assert any('"float"' in n for n in notes)

    def test_with_stale_after_arg(self) -> None:
        """Type preservation should work with stale_after parameter."""
        notes, errors = _run_mypy("""
            from datetime import timedelta
            from cachier import cachier

            @cachier(stale_after=timedelta(hours=1))
            def lookup(key: str) -> list[int]:
                return [1, 2, 3]

            reveal_type(lookup("x"))
        """)
        assert not errors
        assert any("list[int]" in n for n in notes)
