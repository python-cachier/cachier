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

    def _init_prometheus_metrics(self) -> None:
        """Initialize Prometheus metrics using prometheus_client."""
        if not self._prom_client:
            return

        # Define Prometheus metrics
        from prometheus_client import Counter, Gauge, Histogram

        self._hits = Counter(
            "cachier_cache_hits_total",
            "Total number of cache hits",
            ["function"],
        )
        self._misses = Counter(
            "cachier_cache_misses_total",
            "Total number of cache misses",
            ["function"],
        )
        self._hit_rate = Gauge(
            "cachier_cache_hit_rate",
            "Cache hit rate percentage",
            ["function"],
        )
        self._latency = Histogram(
            "cachier_operation_latency_seconds",
            "Cache operation latency in seconds",
            ["function"],
        )
        self._stale_hits = Counter(
            "cachier_stale_hits_total",
            "Total number of stale cache hits",
            ["function"],
        )
        self._recalculations = Counter(
            "cachier_recalculations_total",
            "Total number of cache recalculations",
            ["function"],
        )
        self._entry_count = Gauge(
            "cachier_entry_count",
            "Current number of cache entries",
            ["function"],
        )
        self._cache_size = Gauge(
            "cachier_cache_size_bytes",
            "Total cache size in bytes",
            ["function"],
        )

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
                f"Function {func.__name__} does not have metrics enabled. "
                "Use @cachier(enable_metrics=True)"
            )

        with self._lock:
            func_name = f"{func.__module__}.{func.__name__}"
            self._registered_functions[func_name] = func

    def export_metrics(self, func_name: str, metrics: Any) -> None:
        """Export metrics for a specific function to Prometheus.

        Parameters
        ----------
        func_name : str
            Name of the function
        metrics : MetricSnapshot
            Metrics snapshot to export

        """
        if not self._prom_client:
            return

        # Update Prometheus metrics
        self._hits.labels(function=func_name).inc(metrics.hits)
        self._misses.labels(function=func_name).inc(metrics.misses)
        self._hit_rate.labels(function=func_name).set(metrics.hit_rate)
        self._stale_hits.labels(function=func_name).inc(metrics.stale_hits)
        self._recalculations.labels(function=func_name).inc(
            metrics.recalculations
        )
        self._entry_count.labels(function=func_name).set(metrics.entry_count)
        self._cache_size.labels(function=func_name).set(
            metrics.total_size_bytes
        )

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
                lines.append(
                    f'cachier_cache_hits_total{{function="{func_name}"}} {stats.hits}'
                )

        # Misses
        lines.append("")
        lines.append("# HELP cachier_cache_misses_total Total cache misses")
        lines.append("# TYPE cachier_cache_misses_total counter")
        
        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(
                    f'cachier_cache_misses_total{{function="{func_name}"}} {stats.misses}'
                )

        # Hit rate
        lines.append("")
        lines.append("# HELP cachier_cache_hit_rate Cache hit rate percentage")
        lines.append("# TYPE cachier_cache_hit_rate gauge")
        
        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(
                    f'cachier_cache_hit_rate{{function="{func_name}"}} {stats.hit_rate:.2f}'
                )

        # Average latency
        lines.append("")
        lines.append("# HELP cachier_avg_latency_ms Average cache operation latency in milliseconds")
        lines.append("# TYPE cachier_avg_latency_ms gauge")
        
        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(
                    f'cachier_avg_latency_ms{{function="{func_name}"}} {stats.avg_latency_ms:.4f}'
                )

        # Stale hits
        lines.append("")
        lines.append("# HELP cachier_stale_hits_total Total stale cache hits")
        lines.append("# TYPE cachier_stale_hits_total counter")
        
        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(
                    f'cachier_stale_hits_total{{function="{func_name}"}} {stats.stale_hits}'
                )

        # Recalculations
        lines.append("")
        lines.append("# HELP cachier_recalculations_total Total cache recalculations")
        lines.append("# TYPE cachier_recalculations_total counter")
        
        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(
                    f'cachier_recalculations_total{{function="{func_name}"}} {stats.recalculations}'
                )

        # Entry count
        lines.append("")
        lines.append("# HELP cachier_entry_count Current cache entries")
        lines.append("# TYPE cachier_entry_count gauge")
        
        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(
                    f'cachier_entry_count{{function="{func_name}"}} {stats.entry_count}'
                )

        # Cache size
        lines.append("")
        lines.append("# HELP cachier_cache_size_bytes Total cache size in bytes")
        lines.append("# TYPE cachier_cache_size_bytes gauge")
        
        with self._lock:
            for func_name, func in self._registered_functions.items():
                if not hasattr(func, "metrics") or func.metrics is None:
                    continue
                stats = func.metrics.get_stats()
                lines.append(
                    f'cachier_cache_size_bytes{{function="{func_name}"}} {stats.total_size_bytes}'
                )

        # Size limit rejections
        lines.append("")
        lines.append("# HELP cachier_size_limit_rejections_total Entries rejected due to size limit")
        lines.append("# TYPE cachier_size_limit_rejections_total counter")
        
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

        If prometheus_client is available, starts the HTTP server. Otherwise,
        provides a simple HTTP server for text format metrics.

        """
        if self._prom_client:
            # Use prometheus_client's built-in HTTP server
            try:
                from prometheus_client import start_http_server

                start_http_server(self.port)
            except Exception:  # noqa: S110
                # Silently fail if server can't start
                pass
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
