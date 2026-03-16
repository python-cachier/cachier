"""Tests for metrics exporters."""

import re

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


@pytest.mark.memory
def test_prometheus_exporter_double_instantiation():
    """Test that two PrometheusExporter instances both work independently."""

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()
    test_func(5)

    exporter1 = PrometheusExporter(port=9097, use_prometheus_client=False)
    exporter1.register_function(test_func)

    exporter2 = PrometheusExporter(port=9098, use_prometheus_client=False)
    exporter2.register_function(test_func)

    # Both should generate valid metrics
    text1 = exporter1._generate_text_metrics()
    text2 = exporter2._generate_text_metrics()

    assert "cachier_cache_hits_total" in text1
    assert "cachier_cache_hits_total" in text2

    test_func.clear_cache()


@pytest.mark.memory
def test_prometheus_text_metrics_consistency():
    """Test that hits + misses == total_calls in generated text at one point in time."""

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()

    exporter = PrometheusExporter(port=9099, use_prometheus_client=False)
    exporter.register_function(test_func)

    test_func(5)  # miss
    test_func(5)  # hit
    test_func(10)  # miss

    # Get stats and text at same time
    stats = test_func.metrics.get_stats()
    metrics_text = exporter._generate_text_metrics()

    # Verify consistency: parse hits and misses from text
    func_name = f"{test_func.__module__}.{test_func.__name__}"
    hits_match = re.search(
        rf'cachier_cache_hits_total\{{function="{re.escape(func_name)}"\}} (\d+)',
        metrics_text,
    )
    misses_match = re.search(
        rf'cachier_cache_misses_total\{{function="{re.escape(func_name)}"\}} (\d+)',
        metrics_text,
    )

    assert hits_match
    assert misses_match
    hits = int(hits_match.group(1))
    misses = int(misses_match.group(1))
    assert hits + misses == stats.total_calls

    test_func.clear_cache()


@pytest.mark.memory
def test_prometheus_export_metrics_noop():
    """Test that export_metrics is a no-op (backward-compat method)."""
    exporter = PrometheusExporter(port=9100, use_prometheus_client=False)
    # Should not raise
    exporter.export_metrics("some_func", None)


@pytest.mark.memory
def test_prometheus_text_metrics_skips_none_metrics():
    """Test that _generate_text_metrics skips functions whose metrics attr is None."""

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()
    test_func(5)

    exporter = PrometheusExporter(port=9101, use_prometheus_client=False)
    exporter.register_function(test_func)

    # Inject a fake entry whose metrics resolve to None
    class _NoMetrics:
        __module__ = "test"
        __name__ = "no_metrics"
        metrics = None

        def __call__(self, *a, **kw):
            pass

    exporter._registered_functions["test.no_metrics"] = _NoMetrics()

    # Should not raise; the None-metrics entry is silently skipped
    text = exporter._generate_text_metrics()
    assert "cachier_cache_hits_total" in text
    assert "no_metrics" not in text

    test_func.clear_cache()


@pytest.mark.memory
def test_prometheus_start_stop_simple_server():
    """Test starting and stopping the simple HTTP server."""
    exporter = PrometheusExporter(port=19090, use_prometheus_client=False)
    exporter.start()
    assert exporter._server is not None
    exporter.stop()
    assert exporter._server is None


@pytest.mark.memory
def test_prometheus_start_stop_prometheus_server():
    """Test starting and stopping the prometheus_client-backed HTTP server."""
    prometheus_client = pytest.importorskip("prometheus_client")  # noqa: F841
    exporter = PrometheusExporter(port=19091, use_prometheus_client=True)
    assert exporter._registry is not None
    exporter.start()
    assert exporter._server is not None
    exporter.stop()
    assert exporter._server is None


@pytest.mark.memory
def test_prometheus_collector_collect():
    """Test that the CachierCollector.collect() yields metrics correctly."""
    pytest.importorskip("prometheus_client")
    from prometheus_client import generate_latest

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()
    test_func(5)
    test_func(5)

    exporter = PrometheusExporter(port=19092, use_prometheus_client=True)
    exporter.register_function(test_func)

    assert exporter._registry is not None
    output = generate_latest(exporter._registry).decode()
    assert "cachier_cache_hits_total" in output
    assert "cachier_cache_misses_total" in output

    test_func.clear_cache()


@pytest.mark.memory
def test_prometheus_client_not_available(monkeypatch):
    """Test PrometheusExporter falls back gracefully when prometheus_client is patched out."""
    monkeypatch.setattr("cachier.exporters.prometheus.PROMETHEUS_CLIENT_AVAILABLE", False)
    monkeypatch.setattr("cachier.exporters.prometheus.prometheus_client", None)

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()
    test_func(5)

    exporter = PrometheusExporter(port=19093, use_prometheus_client=True)
    assert exporter._prom_client is None
    exporter.register_function(test_func)
    text = exporter._generate_text_metrics()
    assert "cachier_cache_hits_total" in text

    test_func.clear_cache()


@pytest.mark.memory
def test_prometheus_stop_when_not_started():
    """Test that stop() is a no-op when the server was never started."""
    exporter = PrometheusExporter(port=19094, use_prometheus_client=False)
    exporter.stop()  # Should not raise


@pytest.mark.memory
def test_prometheus_simple_server_404():
    """Test that simple HTTP server returns 404 for non-metrics paths."""
    import http.client

    exporter = PrometheusExporter(port=19095, use_prometheus_client=False)
    exporter.start()
    try:
        conn = http.client.HTTPConnection("127.0.0.1", 19095)
        conn.request("GET", "/notfound")
        response = conn.getresponse()
        assert response.status == 404
        conn.close()
    finally:
        exporter.stop()


@pytest.mark.memory
def test_prometheus_prometheus_server_404():
    """Test that prometheus_client-backed server returns 404 for non-metrics paths."""
    import http.client

    pytest.importorskip("prometheus_client")

    exporter = PrometheusExporter(port=19096, use_prometheus_client=True)
    exporter.start()
    try:
        conn = http.client.HTTPConnection("127.0.0.1", 19096)
        conn.request("GET", "/notfound")
        response = conn.getresponse()
        assert response.status == 404
        conn.close()
    finally:
        exporter.stop()


@pytest.mark.memory
def test_prometheus_collector_collect_empty():
    """Test CachierCollector.collect() when no functions have metrics."""
    pytest.importorskip("prometheus_client")
    from prometheus_client import generate_latest

    exporter = PrometheusExporter(port=19097, use_prometheus_client=True)
    assert exporter._registry is not None
    # No functions registered — collect() should run without error and yield metric families
    output = generate_latest(exporter._registry).decode()
    # Output may be empty or contain only headers; no crash is the key assertion
    assert isinstance(output, str)


@pytest.mark.memory
def test_prometheus_simple_server_metrics_endpoint():
    """Test that simple HTTP server returns metrics on /metrics."""
    import urllib.request

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()
    test_func(5)

    exporter = PrometheusExporter(port=19098, use_prometheus_client=False)
    exporter.register_function(test_func)
    exporter.start()
    try:
        response = urllib.request.urlopen("http://127.0.0.1:19098/metrics")
        body = response.read().decode()
        assert "cachier_cache_hits_total" in body
    finally:
        exporter.stop()
        test_func.clear_cache()


@pytest.mark.memory
def test_prometheus_prometheus_server_metrics_endpoint():
    """Test that prometheus_client-backed server returns metrics on /metrics."""
    import urllib.request

    pytest.importorskip("prometheus_client")

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()
    test_func(5)

    exporter = PrometheusExporter(port=19099, use_prometheus_client=True)
    exporter.register_function(test_func)
    exporter.start()
    try:
        response = urllib.request.urlopen("http://127.0.0.1:19099/metrics")
        body = response.read().decode()
        assert "cachier_cache_hits_total" in body
    finally:
        exporter.stop()
        test_func.clear_cache()


@pytest.mark.memory
def test_prometheus_collector_collect_skips_none_metrics():
    """Test CachierCollector.collect() skips functions where metrics is None."""
    pytest.importorskip("prometheus_client")
    from prometheus_client import generate_latest

    exporter = PrometheusExporter(port=19200, use_prometheus_client=True)

    class _NoMetrics:
        __module__ = "test"
        __name__ = "no_metrics"
        metrics = None

        def __call__(self, *a, **kw):
            pass

    exporter._registered_functions["test.no_metrics"] = _NoMetrics()

    assert exporter._registry is not None
    output = generate_latest(exporter._registry).decode()
    assert isinstance(output, str)
