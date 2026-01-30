"""Tests for metrics exporters."""

import pytest

from cachier import cachier
from cachier.exporters import MetricsExporter, PrometheusExporter


@pytest.mark.memory
def test_prometheus_exporter_registration():
    """Test registering a function with PrometheusExporter."""

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()

    exporter = PrometheusExporter(port=9091)

    # Should succeed with metrics-enabled function
    exporter.register_function(test_func)
    assert test_func in exporter._registered_functions.values()

    test_func.clear_cache()


@pytest.mark.memory
def test_prometheus_exporter_requires_metrics():
    """Test that PrometheusExporter requires metrics to be enabled."""

    @cachier(backend="memory")  # metrics disabled by default
    def test_func(x):
        return x * 2

    exporter = PrometheusExporter(port=9092)

    # Should raise error for function without metrics
    with pytest.raises(ValueError, match="does not have metrics enabled"):
        exporter.register_function(test_func)

    test_func.clear_cache()


@pytest.mark.memory
def test_prometheus_exporter_text_format():
    """Test that PrometheusExporter generates valid text format."""

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()

    exporter = PrometheusExporter(port=9093, use_prometheus_client=False)
    exporter.register_function(test_func)

    # Generate some metrics
    test_func(5)
    test_func(5)

    # Generate text format
    metrics_text = exporter._generate_text_metrics()

    # Check for Prometheus format elements
    assert "cachier_cache_hits_total" in metrics_text
    assert "cachier_cache_misses_total" in metrics_text
    assert "cachier_cache_hit_rate" in metrics_text
    assert "# HELP" in metrics_text
    assert "# TYPE" in metrics_text

    test_func.clear_cache()


@pytest.mark.memory
def test_prometheus_exporter_multiple_functions():
    """Test PrometheusExporter with multiple functions."""

    @cachier(backend="memory", enable_metrics=True)
    def func1(x):
        return x * 2

    @cachier(backend="memory", enable_metrics=True)
    def func2(x):
        return x * 3

    func1.clear_cache()
    func2.clear_cache()

    exporter = PrometheusExporter(port=9094, use_prometheus_client=False)
    exporter.register_function(func1)
    exporter.register_function(func2)

    # Generate some metrics
    func1(5)
    func2(10)

    metrics_text = exporter._generate_text_metrics()

    # Both functions should be in the output
    assert "func1" in metrics_text
    assert "func2" in metrics_text

    func1.clear_cache()
    func2.clear_cache()


def test_metrics_exporter_interface():
    """Test PrometheusExporter implements MetricsExporter interface."""
    exporter = PrometheusExporter(port=9095)

    # Check that it has the required methods
    assert hasattr(exporter, "register_function")
    assert hasattr(exporter, "export_metrics")
    assert hasattr(exporter, "start")
    assert hasattr(exporter, "stop")

    # Check that it's an instance of the base class
    assert isinstance(exporter, MetricsExporter)


@pytest.mark.memory
def test_prometheus_exporter_with_prometheus_client_fallback():
    """Test PrometheusExporter with use_prometheus_client=True falls back gracefully."""

    # When prometheus_client is not available, it should fall back to text mode
    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()

    # Create exporter with use_prometheus_client=True (will use text mode as fallback)
    exporter = PrometheusExporter(port=9095, use_prometheus_client=True)
    exporter.register_function(test_func)

    # Generate some metrics
    test_func(5)
    test_func(5)

    # Verify function is registered
    assert test_func in exporter._registered_functions.values()

    # Verify text metrics can be generated (fallback mode)
    metrics_text = exporter._generate_text_metrics()
    assert "cachier_cache_hits_total" in metrics_text

    test_func.clear_cache()


@pytest.mark.memory
def test_prometheus_exporter_collector_metrics():
    """Test that custom collector generates correct metrics."""
    from cachier import cachier
    from cachier.exporters import PrometheusExporter

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()

    # Use text mode to verify metrics are accessible
    exporter = PrometheusExporter(port=9096, use_prometheus_client=False)
    exporter.register_function(test_func)

    # Generate metrics
    test_func(5)
    test_func(5)  # hit
    test_func(10)  # miss

    # Get stats to verify
    stats = test_func.metrics.get_stats()
    assert stats.hits == 1
    assert stats.misses == 2

    test_func.clear_cache()
