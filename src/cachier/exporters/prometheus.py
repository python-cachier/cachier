"""Prometheus exporter for cachier metrics."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license

import threading
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Protocol, cast

from .base import MetricsExporter

if TYPE_CHECKING:
    from ..metrics import CacheMetrics, MetricSnapshot


class _MetricsEnabledCallable(Protocol):
    """Callable wrapper that exposes cachier metrics."""

    __module__: str
    __name__: str
    metrics: Optional["CacheMetrics"]

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Invoke the wrapped callable."""


def _get_func_metrics(func: Callable[..., Any]) -> Optional["CacheMetrics"]:
    """Return the metrics object for a registered function, if available."""
    metrics_func = cast(_MetricsEnabledCallable, func)
    return metrics_func.metrics


try:
    import prometheus_client  # type: ignore[import-not-found]
    from prometheus_client import CollectorRegistry  # type: ignore[import-not-found]
    from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily  # type: ignore[import-not-found]

    PROMETHEUS_CLIENT_AVAILABLE = True
except (ImportError, AttributeError):  # pragma: no cover
    PROMETHEUS_CLIENT_AVAILABLE = False
    prometheus_client = None  # type: ignore[assignment]
    CollectorRegistry = None  # type: ignore[assignment]
    CounterMetricFamily = None  # type: ignore[assignment]
    GaugeMetricFamily = None  # type: ignore[assignment]


class CachierCollector:
    """Custom Prometheus collector that pulls metrics from registered functions."""

    def __init__(self, exporter: "PrometheusExporter") -> None:
        self.exporter = exporter

    def describe(self) -> list:
        """Return an empty list; metrics are described at collect time."""
        return []

    def collect(self) -> Any:
        """Collect metrics from all registered functions."""
        # Snapshot all metrics in one lock acquisition for consistency
        with self.exporter._lock:
            snapshots: Dict[str, "MetricSnapshot"] = {}
            for func_name, func in self.exporter._registered_functions.items():
                m = _get_func_metrics(func)
                if m is not None:
                    snapshots[func_name] = m.get_stats()

        # Build metric families outside the lock using the snapshots
        hits = CounterMetricFamily("cachier_cache_hits_total", "Total cache hits", labels=["function"])
        misses = CounterMetricFamily("cachier_cache_misses_total", "Total cache misses", labels=["function"])
        hit_rate = GaugeMetricFamily("cachier_cache_hit_rate", "Cache hit rate percentage", labels=["function"])
        stale_hits = CounterMetricFamily("cachier_stale_hits_total", "Total stale cache hits", labels=["function"])
        recalculations = CounterMetricFamily(
            "cachier_recalculations_total", "Total cache recalculations", labels=["function"]
        )
        wait_timeouts = CounterMetricFamily("cachier_wait_timeouts_total", "Total wait timeouts", labels=["function"])
        entry_count = GaugeMetricFamily("cachier_entry_count", "Current number of cache entries", labels=["function"])
        cache_size = GaugeMetricFamily("cachier_cache_size_bytes", "Total cache size in bytes", labels=["function"])

        for func_name, stats in snapshots.items():
            hits.add_metric([func_name], stats.hits)
            misses.add_metric([func_name], stats.misses)
            hit_rate.add_metric([func_name], stats.hit_rate)
            stale_hits.add_metric([func_name], stats.stale_hits)
            recalculations.add_metric([func_name], stats.recalculations)
            wait_timeouts.add_metric([func_name], stats.wait_timeouts)
            entry_count.add_metric([func_name], stats.entry_count)
            cache_size.add_metric([func_name], stats.total_size_bytes)

        # Yield metrics one by one as required by Prometheus collector protocol
        yield hits
        yield misses
        yield hit_rate
        yield stale_hits
        yield recalculations
        yield wait_timeouts
        yield entry_count
        yield cache_size


class PrometheusExporter(MetricsExporter):
    """Export cachier metrics in Prometheus format.

    This exporter provides a simple HTTP server that exposes metrics in
    Prometheus text format. It can be used with prometheus_client or
    as a standalone exporter.

    Parameters
    ----------
    port : int, optional
        Port for the HTTP server, by default 9090
    use_prometheus_client : bool, optional
        Whether to use prometheus_client library if available, by default True

    Examples
    --------
    >>> from cachier import cachier
    >>> from cachier.exporters import PrometheusExporter
    >>>
    >>> @cachier(backend='memory', enable_metrics=True)
    ... def my_func(x):
    ...     return x * 2
    >>>
    >>> exporter = PrometheusExporter(port=9090)
    >>> exporter.register_function(my_func)
    >>> exporter.start()

    """

    def __init__(self, port: int = 9090, use_prometheus_client: bool = True, host: str = "127.0.0.1"):
        """Initialize Prometheus exporter.

        Parameters
        ----------
        port : int
            HTTP server port
        use_prometheus_client : bool
            Whether to use prometheus_client library
        host : str
            Host address to bind to (default: 127.0.0.1 for localhost only)

        """
        self.port = port
        self.host = host
        self.use_prometheus_client = use_prometheus_client
        self._registered_functions: Dict[str, _MetricsEnabledCallable] = {}
        self._lock = threading.Lock()
        self._server: Optional[Any] = None
        self._server_thread: Optional[threading.Thread] = None

        # Try to import prometheus_client if requested
        self._prom_client = None
        # Per-instance registry to avoid double-registration on the global
        # REGISTRY when multiple PrometheusExporter instances are created.
        self._registry: Optional[Any] = None
        if use_prometheus_client and PROMETHEUS_CLIENT_AVAILABLE:
            self._prom_client = prometheus_client
            self._init_prometheus_metrics()
            self._setup_collector()

    def _setup_collector(self) -> None:
        """Set up a custom collector to pull metrics from registered functions."""
        if not self._prom_client:  # pragma: no cover
            return

        # Use a per-instance registry so multiple exporters don't conflict
        self._registry = CollectorRegistry()
        self._registry.register(CachierCollector(self))

    def _init_prometheus_metrics(self) -> None:
        """Initialize Prometheus metrics using prometheus_client.

        Note: With custom collector, we don't need to pre-define metrics.
        The collector will generate them dynamically at scrape time.

        """
        # Metrics are now handled by the custom collector in _setup_collector()
        pass

    def register_function(self, func: Callable[..., Any]) -> None:
        """Register a cached function for metrics export.

        Parameters
        ----------
        func : Callable
            A function decorated with @cachier that has metrics enabled

        Raises
        ------
        ValueError
            If the function doesn't have metrics enabled

        """
        metrics = _get_func_metrics(func)
        if metrics is None:
            raise ValueError(
                f"Function {func.__name__} does not have metrics enabled. Use @cachier(enable_metrics=True)"
            )

        with self._lock:
            func_name = f"{func.__module__}.{func.__name__}"
            self._registered_functions[func_name] = cast(_MetricsEnabledCallable, func)

    def export_metrics(self, func_name: str, metrics: Any) -> None:
        """Export metrics for a specific function to Prometheus.

        With custom collector mode, metrics are automatically pulled at scrape time.
        This method is kept for backward compatibility but is a no-op when using
        prometheus_client with custom collector.

        Parameters
        ----------
        func_name : str
            Name of the function
        metrics : MetricSnapshot
            Metrics snapshot to export

        """
        # With custom collector, metrics are pulled automatically at scrape time
        # No need to manually push metrics
        pass

    def _generate_text_metrics(self) -> str:
        """Generate Prometheus text format metrics.

        Returns
        -------
        str
            Metrics in Prometheus text format

        """
        # Snapshot all metrics in one lock acquisition for consistency
        with self._lock:
            snapshots: Dict[str, "MetricSnapshot"] = {}
            for func_name, func in self._registered_functions.items():
                m = _get_func_metrics(func)
                if m is not None:
                    snapshots[func_name] = m.get_stats()

        # (name, help, type, getter, fmt)
        metric_defs = [
            ("cachier_cache_hits_total", "Total cache hits", "counter", lambda s: s.hits, "{}"),
            ("cachier_cache_misses_total", "Total cache misses", "counter", lambda s: s.misses, "{}"),
            ("cachier_cache_hit_rate", "Cache hit rate percentage", "gauge", lambda s: s.hit_rate, "{:.2f}"),
            (
                "cachier_avg_latency_ms",
                "Average cache operation latency in milliseconds",
                "gauge",
                lambda s: s.avg_latency_ms,
                "{:.4f}",
            ),
            ("cachier_stale_hits_total", "Total stale cache hits", "counter", lambda s: s.stale_hits, "{}"),
            ("cachier_recalculations_total", "Total cache recalculations", "counter", lambda s: s.recalculations, "{}"),
            ("cachier_wait_timeouts_total", "Total wait timeouts", "counter", lambda s: s.wait_timeouts, "{}"),
            ("cachier_entry_count", "Current cache entries", "gauge", lambda s: s.entry_count, "{}"),
            ("cachier_cache_size_bytes", "Total cache size in bytes", "gauge", lambda s: s.total_size_bytes, "{}"),
            (
                "cachier_size_limit_rejections_total",
                "Entries rejected due to size limit",
                "counter",
                lambda s: s.size_limit_rejections,
                "{}",
            ),
        ]

        lines: list[str] = []
        for name, help_text, metric_type, getter, fmt in metric_defs:
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} {metric_type}")
            for func_name, stats in snapshots.items():
                value = fmt.format(getter(stats))
                lines.append(f'{name}{{function="{func_name}"}} {value}')
            lines.append("")

        return "\n".join(lines)

    def start(self) -> None:
        """Start the Prometheus exporter.

        If prometheus_client is available, starts the HTTP server using the per-instance registry. Otherwise, provides a
        simple HTTP server for text format metrics.

        """
        if self._prom_client and self._registry is not None:
            # Use a simple HTTP server that serves from our per-instance registry
            # instead of prometheus_client's start_http_server which uses the
            # global REGISTRY.
            self._start_prometheus_server()
        else:
            # Provide simple HTTP server for text format
            self._start_simple_server()

    def _start_prometheus_server(self) -> None:
        """Start an HTTP server that serves metrics from the per-instance registry."""
        from http.server import BaseHTTPRequestHandler, HTTPServer

        from prometheus_client import exposition

        if self._registry is None:  # pragma: no cover
            raise RuntimeError("registry must be initialized before starting server")
        registry = self._registry

        class MetricsHandler(BaseHTTPRequestHandler):
            """HTTP handler that serves Prometheus metrics from a specific registry."""

            def do_GET(self) -> None:
                """Handle GET requests for /metrics endpoint."""
                if self.path == "/metrics":
                    output = exposition.generate_latest(registry)
                    self.send_response(200)
                    self.send_header("Content-Type", exposition.CONTENT_TYPE_LATEST)
                    self.end_headers()
                    self.wfile.write(output)
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, fmt: str, *args: Any) -> None:
                """Suppress log messages."""

        server = HTTPServer((self.host, self.port), MetricsHandler)
        self._server = server

        def run_server() -> None:
            server.serve_forever()

        self._server_thread = threading.Thread(target=run_server, daemon=True)
        self._server_thread.start()

    def _start_simple_server(self) -> None:
        """Start a simple HTTP server for Prometheus text format."""
        from http.server import BaseHTTPRequestHandler, HTTPServer

        exporter = self

        class MetricsHandler(BaseHTTPRequestHandler):
            """HTTP handler that serves Prometheus text-format metrics."""

            def do_GET(self) -> None:
                """Handle GET requests for /metrics endpoint."""
                if self.path == "/metrics":
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain")
                    self.end_headers()
                    metrics_text = exporter._generate_text_metrics()
                    self.wfile.write(metrics_text.encode())
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, fmt: str, *args: Any) -> None:
                """Suppress log messages."""

        server = HTTPServer((self.host, self.port), MetricsHandler)
        self._server = server

        def run_server() -> None:
            server.serve_forever()

        self._server_thread = threading.Thread(target=run_server, daemon=True)
        self._server_thread.start()

    def stop(self) -> None:
        """Stop the Prometheus exporter and clean up resources."""
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._server_thread:
            self._server_thread.join(timeout=5)
            self._server_thread = None
