"""Regression tests for preserving call arguments when caching is bypassed."""

from collections.abc import Iterator

import pytest

from cachier import cachier, disable_caching, enable_caching

pytestmark = pytest.mark.memory


@pytest.fixture(params=["per_call", "global"])
def bypass_kwargs(request: pytest.FixtureRequest) -> Iterator[dict[str, bool]]:
    """Exercise both public ways to bypass cached results.

    Restore global caching after each case so other tests remain independent.

    """
    if request.param == "global":
        disable_caching()
    try:
        yield {} if request.param == "global" else {"cachier__skip_cache": True}
    finally:
        enable_caching()


def test_bypass_positional_only(bypass_kwargs: dict[str, bool]) -> None:
    @cachier(backend="memory")
    def add(left: int, /, right: int = 2) -> int:
        """Add two values.

        The first value must be supplied positionally.

        """
        return left + right

    assert add(1, **bypass_kwargs) == 3
    assert add(1, right=4, **bypass_kwargs) == 5


def test_bypass_variadic_arguments(bypass_kwargs: dict[str, bool]) -> None:
    @cachier(backend="memory")
    def collect(*args: int, **kwargs: int) -> tuple[tuple[int, ...], dict[str, int]]:
        """Return the received positional and keyword arguments.

        No cache-key-only arguments should reach this function.

        """
        return args, kwargs

    assert collect(1, 2, item=3, **bypass_kwargs) == ((1, 2), {"item": 3})


@pytest.mark.asyncio
async def test_async_bypass_positional_only(bypass_kwargs: dict[str, bool]) -> None:
    @cachier(backend="memory")
    async def add(left: int, /, right: int = 2) -> int:
        """Add two values asynchronously.

        The first value must be supplied positionally.

        """
        return left + right

    assert await add(1, **bypass_kwargs) == 3
    assert await add(1, right=4, **bypass_kwargs) == 5


@pytest.mark.asyncio
async def test_async_bypass_variadic_arguments(bypass_kwargs: dict[str, bool]) -> None:
    @cachier(backend="memory")
    async def collect(*args: int, **kwargs: int) -> tuple[tuple[int, ...], dict[str, int]]:
        """Return the received positional and keyword arguments asynchronously.

        No cache-key-only arguments should reach this function.

        """
        return args, kwargs

    assert await collect(1, 2, item=3, **bypass_kwargs) == ((1, 2), {"item": 3})


def test_bypass_bound_method(bypass_kwargs: dict[str, bool]) -> None:
    class Calculator:
        @cachier(backend="memory", allow_non_static_methods=True)
        def add(self, left: int, /, right: int = 2) -> int:
            """Add values with a positional-only argument.

            Keep the instance binding when the cache is bypassed.

            """
            return left + right

    assert Calculator().add(1, right=4, **bypass_kwargs) == 5


@pytest.mark.asyncio
async def test_async_bypass_bound_method(bypass_kwargs: dict[str, bool]) -> None:
    class Calculator:
        @cachier(backend="memory", allow_non_static_methods=True)
        async def add(self, left: int, /, right: int = 2) -> int:
            """Add values asynchronously with a positional-only argument.

            Keep the instance binding when the cache is bypassed.

            """
            return left + right

    assert await Calculator().add(1, right=4, **bypass_kwargs) == 5
