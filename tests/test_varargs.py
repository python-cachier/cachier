"""Test for variadic arguments (*args) handling in cachier."""

from datetime import timedelta

import pytest

from cachier import cachier


@pytest.mark.pickle
@pytest.mark.parametrize("backend", ["pickle", "memory"])
class TestVarargsDifferentCacheKeys:
    """Test *args get unique cache keys for different arguments."""

    @pytest.fixture(autouse=True)
    def setup(self, backend):
        """Set up the test function for each test."""
        self.call_count = 0
        self.backend = backend

        @cachier(backend=backend, stale_after=timedelta(seconds=500))
        def get_data(*args):
            """Test function that accepts variadic arguments."""
            self.call_count += 1
            return f"Result for args: {args}, call #{self.call_count}"

        self.get_data = get_data
        get_data.clear_cache()
        self.call_count = 0
        yield
        get_data.clear_cache()

    def test_different_args_produce_different_results(self):
        """Test that different arguments produce different cache entries."""
        result1 = self.get_data("print", "domains")
        assert self.call_count == 1
        assert "('print', 'domains')" in result1

        result2 = self.get_data("print", "users", "allfields")
        assert self.call_count == 2, (
            "Function should be called again with different args"
        )
        assert "('print', 'users', 'allfields')" in result2
        assert result1 != result2, (
            "Different args should produce different results"
        )

    def test_same_args_use_cache(self):
        """Test that calling with same arguments uses cache."""
        # First call
        result1 = self.get_data("print", "domains")
        assert self.call_count == 1

        # Second call with same args should use cache
        previous_call_count = self.call_count
        result3 = self.get_data("print", "domains")
        assert self.call_count == previous_call_count, (
            "Function should not be called again (cache hit)"
        )
        assert result3 == result1

    def test_another_set_of_args_cache_hit(self):
        """Test cache hit for another set of arguments."""
        # First call
        result2 = self.get_data("print", "users", "allfields")
        assert self.call_count == 1

        # Second call with same args should use cache
        previous_call_count = self.call_count
        result4 = self.get_data("print", "users", "allfields")
        assert self.call_count == previous_call_count, (
            "Function should not be called again (cache hit)"
        )
        assert result4 == result2


@pytest.mark.pickle
@pytest.mark.parametrize("backend", ["pickle", "memory"])
class TestVarargsEmpty:
    """Test that functions with *args work with no arguments."""

    @pytest.fixture(autouse=True)
    def setup(self, backend):
        """Set up the test function for each test."""
        self.call_count = 0

        @cachier(backend=backend, stale_after=timedelta(seconds=500))
        def get_data(*args):
            """Test function that accepts variadic arguments."""
            self.call_count += 1
            return f"Result for args: {args}, call #{self.call_count}"

        self.get_data = get_data
        get_data.clear_cache()
        self.call_count = 0
        yield
        get_data.clear_cache()

    def test_call_with_no_arguments(self):
        """Test calling with no arguments."""
        result1 = self.get_data()
        assert self.call_count == 1
        assert "()" in result1

    def test_no_args_cache_hit(self):
        """Test cache hit when calling with no arguments again."""
        # First call
        self.get_data()
        assert self.call_count == 1

        # Second call should use cache
        previous_call_count = self.call_count
        _ = self.get_data()
        assert self.call_count == previous_call_count


@pytest.mark.pickle
@pytest.mark.parametrize("backend", ["pickle", "memory"])
class TestVarargsWithRegularArgs:
    """Test regular and variadic arguments work correctly."""

    @pytest.fixture(autouse=True)
    def setup(self, backend):
        """Set up the test function for each test."""
        self.call_count = 0

        @cachier(backend=backend, stale_after=timedelta(seconds=500))
        def get_data(command, *args):
            """Test function with regular and variadic arguments."""
            self.call_count += 1
            return f"Command: {command}, args: {args}, call #{self.call_count}"

        self.get_data = get_data
        get_data.clear_cache()
        self.call_count = 0
        yield
        get_data.clear_cache()

    def test_different_varargs_produce_different_results(self):
        """Test that different varargs produce different results."""
        result1 = self.get_data("print", "domains")
        assert self.call_count == 1

        result2 = self.get_data("print", "users", "allfields")
        assert self.call_count == 2
        assert result1 != result2

    def test_different_regular_arg_produces_different_result(self):
        """Test that different regular arg produces different result."""
        result1 = self.get_data("print", "domains")
        result3 = self.get_data("list")
        assert self.call_count == 2
        assert result3 != result1

    def test_same_args_produce_cache_hit(self):
        """Test that same arguments produce cache hit."""
        # First call
        result1 = self.get_data("print", "domains")
        assert self.call_count == 1

        # Same args should use cache
        previous_call_count = self.call_count
        result4 = self.get_data("print", "domains")
        assert self.call_count == previous_call_count
        assert result4 == result1


@pytest.mark.pickle
@pytest.mark.parametrize("backend", ["pickle", "memory"])
class TestVarkwargsDifferentCacheKeys:
    """Test **kwargs get unique cache keys for different arguments."""

    @pytest.fixture(autouse=True)
    def setup(self, backend):
        """Set up the test function for each test."""
        self.call_count = 0

        @cachier(backend=backend, stale_after=timedelta(seconds=500))
        def get_data(**kwargs):
            """Test function that accepts keyword variadic arguments."""
            self.call_count += 1
            return f"Result for kwargs: {kwargs}, call #{self.call_count}"

        self.get_data = get_data
        get_data.clear_cache()
        self.call_count = 0
        yield
        get_data.clear_cache()

    def test_different_kwargs_produce_different_results(self):
        """Test that different kwargs produce different results."""
        result1 = self.get_data(type="domains", action="print")
        assert self.call_count == 1

        result2 = self.get_data(
            type="users", action="print", fields="allfields"
        )
        assert self.call_count == 2
        assert result1 != result2

    def test_same_kwargs_produce_cache_hit(self):
        """Test that same kwargs produce cache hit."""
        # First call
        result1 = self.get_data(type="domains", action="print")
        assert self.call_count == 1

        # Same kwargs should use cache
        previous_call_count = self.call_count
        result3 = self.get_data(type="domains", action="print")
        assert self.call_count == previous_call_count
        assert result3 == result1


@pytest.mark.pickle
@pytest.mark.parametrize("backend", ["pickle", "memory"])
class TestVarargsAndVarkwargs:
    """Test that functions with both *args and **kwargs work correctly."""

    @pytest.fixture(autouse=True)
    def setup(self, backend):
        """Set up the test function for each test."""
        self.call_count = 0

        @cachier(backend=backend, stale_after=timedelta(seconds=500))
        def get_data(*args, **kwargs):
            """Test function with both variadic arguments."""
            self.call_count += 1
            return f"args: {args}, kwargs: {kwargs}, call #{self.call_count}"

        self.get_data = get_data
        get_data.clear_cache()
        self.call_count = 0
        yield
        get_data.clear_cache()

    def test_args_only_call(self):
        """Test call with only args."""
        result1 = self.get_data("print", "domains")
        assert self.call_count == 1

        # Cache hit
        result4 = self.get_data("print", "domains")
        assert self.call_count == 1
        assert result4 == result1

    def test_args_and_kwargs_call(self):
        """Test call with both args and kwargs."""
        result2 = self.get_data("print", "users", action="list")
        assert self.call_count == 1

        # Cache hit
        result_check = self.get_data("print", "users", action="list")
        assert self.call_count == 1
        assert result_check == result2

    def test_kwargs_only_call(self):
        """Test call with only kwargs."""
        result3 = self.get_data(action="list", resource="domains")
        assert self.call_count == 1

        # Cache hit
        result_check = self.get_data(action="list", resource="domains")
        assert self.call_count == 1
        assert result_check == result3

    def test_different_combinations_produce_different_results(self):
        """Test that different combinations produce different results."""
        result1 = self.get_data("print", "domains")
        result2 = self.get_data("print", "users", action="list")
        result3 = self.get_data(action="list", resource="domains")

        assert result3 != result1
        assert result3 != result2
        assert result1 != result2


@pytest.mark.pickle
@pytest.mark.parametrize("backend", ["pickle", "memory"])
class TestKeywordOnlyParameters:
    """Test that functions with keyword-only parameters work correctly."""

    @pytest.fixture(autouse=True)
    def setup(self, backend):
        """Set up the test function for each test."""
        self.call_count = 0

        @cachier(backend=backend, stale_after=timedelta(seconds=500))
        def get_data(*args, kw_only):
            """Test function with keyword-only parameter."""
            self.call_count += 1
            return f"args={args}, kw_only={kw_only}, call #{self.call_count}"

        self.get_data = get_data
        get_data.clear_cache()
        self.call_count = 0
        yield
        get_data.clear_cache()

    def test_different_kw_only_values_produce_different_results(self):
        """Test that different keyword-only values produce dif results."""
        result1 = self.get_data("a", "b", kw_only="value1")
        assert self.call_count == 1

        result2 = self.get_data("a", "b", kw_only="value2")
        assert self.call_count == 2
        assert result1 != result2

    def test_same_kw_only_value_produces_cache_hit(self):
        """Test that same keyword-only value produces cache hit."""
        # First call
        result1 = self.get_data("a", "b", kw_only="value1")
        assert self.call_count == 1

        # Same kw_only should use cache
        result3 = self.get_data("a", "b", kw_only="value1")
        assert self.call_count == 1
        assert result3 == result1


@pytest.mark.pickle
@pytest.mark.parametrize("backend", ["pickle", "memory"])
class TestKeywordOnlyWithDefault:
    """Test keyword-only parameters with defaults work correctly."""

    @pytest.fixture(autouse=True)
    def setup(self, backend):
        """Set up the test function for each test."""
        self.call_count = 0

        @cachier(backend=backend, stale_after=timedelta(seconds=500))
        def get_data(*args, kw_only="default"):
            """Test function with keyword-only parameter with default."""
            self.call_count += 1
            return f"args={args}, kw_only={kw_only}, call #{self.call_count}"

        self.get_data = get_data
        get_data.clear_cache()
        self.call_count = 0
        yield
        get_data.clear_cache()

    def test_with_default_value(self):
        """Test calling with default value."""
        result1 = self.get_data("a", "b")
        assert self.call_count == 1
        assert "kw_only=default" in result1

    def test_with_explicit_value(self):
        """Test calling with explicit value."""
        result1 = self.get_data("a", "b")
        result2 = self.get_data("a", "b", kw_only="explicit")
        assert self.call_count == 2
        assert result1 != result2

    def test_cache_hit_with_default(self):
        """Test cache hit when using default value."""
        # First call with default
        result1 = self.get_data("a", "b")
        assert self.call_count == 1

        # Second call with default should use cache
        result3 = self.get_data("a", "b")
        assert self.call_count == 1
        assert result3 == result1

    def test_cache_hit_with_explicit(self):
        """Test cache hit when using explicit value."""
        # First call with explicit
        result2 = self.get_data("a", "b", kw_only="explicit")
        assert self.call_count == 1

        # Second call with explicit should use cache
        result4 = self.get_data("a", "b", kw_only="explicit")
        assert self.call_count == 1
        assert result4 == result2


@pytest.mark.pickle
@pytest.mark.parametrize("backend", ["pickle", "memory"])
class TestMixedVarargsKeywordOnly:
    """Test *args and keyword-only parameters work correctly."""

    @pytest.fixture(autouse=True)
    def setup(self, backend):
        """Set up the test function for each test."""
        self.call_count = 0

        @cachier(backend=backend, stale_after=timedelta(seconds=500))
        def get_data(regular, *args, kw_only, kw_with_default="default"):
            """Test function with mixed parameter types."""
            self.call_count += 1
            return (
                f"regular={regular}, args={args}, kw_only={kw_only}, "
                f"kw_with_default={kw_with_default}, call #{self.call_count}"
            )

        self.get_data = get_data
        get_data.clear_cache()
        self.call_count = 0
        yield
        get_data.clear_cache()

    def test_different_kw_only_produces_different_results(self):
        """Test that different kw_only values produce different results."""
        result1 = self.get_data("r1", "a", "b", kw_only="k1")
        assert self.call_count == 1

        result2 = self.get_data("r1", "a", "b", kw_only="k2")
        assert self.call_count == 2
        assert result1 != result2

    def test_different_kw_with_default_produces_different_results(self):
        """Test that different kw_with_default values produce dif results."""
        result1 = self.get_data("r1", "a", "b", kw_only="k1")
        result3 = self.get_data(
            "r1", "a", "b", kw_only="k1", kw_with_default="custom"
        )
        assert self.call_count == 2
        assert result3 != result1

    def test_same_args_produce_cache_hit(self):
        """Test that same arguments produce cache hit."""
        # First call
        result1 = self.get_data("r1", "a", "b", kw_only="k1")
        assert self.call_count == 1

        # Same args should use cache
        result4 = self.get_data("r1", "a", "b", kw_only="k1")
        assert self.call_count == 1
        assert result4 == result1
