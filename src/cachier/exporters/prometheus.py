"""Prometheus exporter for cachier metrics."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license

import threading
from typing import Any, Callable, Dict, Optional

from .base import MetricsExporter

try:
    import prometheus_client  # type: ignore[import-not-found]

    PROMETHEUS_CLIENT_AVAILABLE = True
except ImportError:
    PROMETHEUS_CLIENT_AVAILABLE = False
    prometheus_client = None  # type: ignore[assignment]


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

    def __init__(
        self,
        port: int = 9090,
        use_prometheus_client: bool = True,
        host: str = "127.0.0.1",
    ):
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
        self._registered_functions: Dict[str, Callable] = {}
        self._lock = threading.Lock()
        self._server: Optional[Any] = None
        self._server_thread: Optional[threading.Thread] = None

        # Try to import prometheus_client if requested
        self._prom_client = None
        if use_prometheus_client and PROMETHEUS_CLIENT_AVAILABLE:
            self._prom_client = prometheus_client
            self._init_prometheus_metrics()
            self._setup_collector()

    def _setup_collector(self) -> None:
        """Set up a custom collector to pull metrics from registered functions."""
        if not self._prom_client:
            return

        try:
            from prometheus_client import REGISTRY
            from prometheus_client.core import (
                CounterMetricFamily,
                GaugeMetricFamily,
            )
        except (ImportError, AttributeError):
            # If prometheus_client is not properly available, skip collector setup
            return

        class CachierCollector:
            """Custom Prometheus collector that pulls metrics from registered functions."""

            def __init__(self, exporter):
                self.exporter = exporter

            def collect(self):
                """Collect metrics from all registered functions."""
                with self.exporter._lock:
                    # Collect hits
                    hits = CounterMetricFamily(
                        "cachier_cache_hits_total",
                        "Total cache hits",
                        labels=["function"]
                    )

                    # Collect misses
                    misses = CounterMetricFamily(
                        "cachier_cache_misses_total",
                        "Total cache misses",
                        labels=["function"]
                    )

                    # Collect hit rate
                    hit_rate = GaugeMetricFamily(
                        "cachier_cache_hit_rate",
                        "Cache hit rate percentage",
                        labels=["function"]
                    )

                    # Collect stale hits
                    stale_hits = CounterMetricFamily(
                        "cachier_stale_hits_total",
                        "Total stale cache hits",
                        labels=["function"]
                    )

                    # Collect recalculations
                    recalculations = CounterMetricFamily(
                        "cachier_recalculations_total",
                        "Total cache recalculations",
                        labels=["function"]
                    )

                    # Collect entry count
                    entry_count = GaugeMetricFamily(
                        "cachier_entry_count",
                        "Current number of cache entries",
                        labels=["function"]
                    )

                    # Collect cache size
                    cache_size = GaugeMetricFamily(
                        "cachier_cache_size_bytes",
                        "Total cache size in bytes",
                        labels=["function"]
                    )

                    for (
                        func_name,
                        func,
                    ) in self.exporter._registered_functions.items():
                        if not hasattr(func, "metrics") or func.metrics is None:
                            continue

                        stats = func.metrics.get_stats()

                        hits.add_metric([func_name], stats.hits)
                        misses.add_metric([func_name], stats.misses)
                        hit_rate.add_metric([func_name], stats.hit_rate)
                        stale_hits.add_metric([func_name], stats.stale_hits)
                        recalculations.add_metric([func_name], stats.recalculations)
                        entry_count.add_metric([func_name], stats.entry_count)
                        cache_size.add_metric([func_name], stats.total_size_bytes)

                    # Yield metrics one by one as required by Prometheus collector protocol
                    yield hits
                    yield misses
                    yield hit_rate
                    yield stale_hits
                    yield recalculations
                    yield entry_count
                    yield cache_size

        # Register the custom collector
        from contextlib import suppress
        with suppress(Exception):
            # If registration fails, continue without collector
            REGISTRY.register(CachierCollector(self))

    def _init_prometheus_metrics(self) -> None:
        """Initialize Prometheus metrics using prometheus_client.

        Note: With custom collector, we don't need to pre-define metrics.
        The collector will generate them dynamically at scrape time.

        """
        # Metrics are now handled by the custom collector in _setup_collector()
        pass

    def register_function(self, func: Callable) -> None:
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
        if not hasattr(func, "metrics") or func.metrics is None:
            raise ValueError(
                f"Function {func.__name__} does not have metrics enabled. Use @cachier(enable_metrics=True)"
            )

        with self._lock:
            func_name = f"{func.__module__}.{func.__name__}"
            self._registered_functions[func_name] = func

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
        lines = []

        # Emit HELP/TYPE headers once at the top for each metric
        lines.append("# HELP cachier_cache_hits_total Total cache hits")
        lines.append("# TYPE cachier_cache_hits_total counter")

        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(f'cachier_cache_hits_total{{function="{func_name}"}} {stats.hits}')

        # Misses
        lines.append(
            "\n# HELP cachier_cache_misses_total Total cache misses\n"
            "# TYPE cachier_cache_misses_total counter"
        )

        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(f'cachier_cache_misses_total{{function="{func_name}"}} {stats.misses}')

        # Hit rate
        lines.append(
            "\n# HELP cachier_cache_hit_rate Cache hit rate percentage\n"
            "# TYPE cachier_cache_hit_rate gauge"
        )

        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(f'cachier_cache_hit_rate{{function="{func_name}"}} {stats.hit_rate:.2f}')

        # Average latency
        lines.append(
            "\n# HELP cachier_avg_latency_ms Average cache operation latency in milliseconds\n"
            "# TYPE cachier_avg_latency_ms gauge"
        )

        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(f'cachier_avg_latency_ms{{function="{func_name}"}} {stats.avg_latency_ms:.4f}')

        # Stale hits
        lines.append(
            "\n# HELP cachier_stale_hits_total Total stale cache hits\n"
            "# TYPE cachier_stale_hits_total counter"
        )

        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(f'cachier_stale_hits_total{{function="{func_name}"}} {stats.stale_hits}')

        # Recalculations
        lines.append(
            "\n# HELP cachier_recalculations_total Total cache recalculations\n"
            "# TYPE cachier_recalculations_total counter"
        )

        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(f'cachier_recalculations_total{{function="{func_name}"}} {stats.recalculations}')

        # Entry count
        lines.append(
            "\n# HELP cachier_entry_count Current cache entries\n"
            "# TYPE cachier_entry_count gauge"
        )

        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(f'cachier_entry_count{{function="{func_name}"}} {stats.entry_count}')

        # Cache size
        lines.append(
            "\n# HELP cachier_cache_size_bytes Total cache size in bytes\n"
            "# TYPE cachier_cache_size_bytes gauge"
        )

        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(f'cachier_cache_size_bytes{{function="{func_name}"}} {stats.total_size_bytes}')

        # Size limit rejections
        lines.append(
            "\n# HELP cachier_size_limit_rejections_total Entries rejected due to size limit\n"
            "# TYPE cachier_size_limit_rejections_total counter"
        )

        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(
                    f'cachier_size_limit_rejections_total{{function="{func_name}"}} {stats.size_limit_rejections}'
                )

        return "\n".join(lines) + "\n"

    def start(self) -> None:
        """Start the Prometheus exporter.

        If prometheus_client is available, starts the HTTP server. Otherwise, provides a simple HTTP server for text
        format metrics.

        """
        if self._prom_client:
            # Use prometheus_client's built-in HTTP server
            from prometheus_client import start_http_server

            # Try to bind to the configured host; fall back gracefully for
            # prometheus_client versions that don't support addr/host.
            try:
                start_http_server(self.port, addr=self.host)
            except TypeError:
                try:
                    start_http_server(self.port, host=self.host)  # type: ignore[call-arg]
                except TypeError:
                    # Old version doesn't support host parameter
                    start_http_server(self.port)
        else:
            # Provide simple HTTP server for text format
            self._start_simple_server()

    def _start_simple_server(self) -> None:
        """Start a simple HTTP server for Prometheus text format."""
        from http.server import BaseHTTPRequestHandler, HTTPServer

        exporter = self

        class MetricsHandler(BaseHTTPRequestHandler):
            def do_GET(self):
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

            def log_message(self, fmt, *args):
                """Suppress log messages."""

        self._server = HTTPServer((self.host, self.port), MetricsHandler)

        def run_server():
            self._server.serve_forever()

        self._server_thread = threading.Thread(target=run_server, daemon=True)
        self._server_thread.start()

    def stop(self) -> None:
        """Stop the Prometheus exporter and clean up resources."""
        if self._server:
            self._server.shutdown()
            self._server = None
            self._server_thread = None
