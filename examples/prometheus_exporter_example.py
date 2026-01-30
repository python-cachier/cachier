"""Prometheus Exporter Example for Cachier.

This example demonstrates using the PrometheusExporter to export cache metrics
to Prometheus for monitoring and alerting.

Usage with Prometheus
---------------------

To use this exporter with Prometheus:

1. Start the exporter HTTP server:
   >>> exporter.start()

2. Configure Prometheus to scrape the metrics endpoint.
   Add this to your prometheus.yml:

   scrape_configs:
     - job_name: 'cachier'
       static_configs:
         - targets: ['localhost:9090']

3. Access metrics at http://localhost:9090/metrics

4. Create dashboards in Grafana or set up alerts based on:
   - cachier_cache_hit_rate (target: > 80%)
   - cachier_cache_misses_total (alert on spikes)
   - cachier_avg_latency_ms (monitor performance)

Available Metrics
-----------------
- cachier_cache_hits_total: Total number of cache hits
- cachier_cache_misses_total: Total number of cache misses
- cachier_cache_hit_rate: Cache hit rate percentage
- cachier_avg_latency_ms: Average cache operation latency
- cachier_stale_hits_total: Total stale cache hits
- cachier_recalculations_total: Total cache recalculations
- cachier_entry_count: Current number of cache entries
- cachier_cache_size_bytes: Total cache size in bytes
- cachier_size_limit_rejections_total: Entries rejected due to size limit

"""

import time

from cachier import cachier
from cachier.exporters import PrometheusExporter


def demo_basic_metrics():
    """Demonstrate basic metrics collection."""
    print("\n=== Basic Metrics Collection ===")

    @cachier(backend="memory", enable_metrics=True)
    def compute(x):
        time.sleep(0.1)  # Simulate work
        return x * 2

    compute.clear_cache()

    # Generate some traffic
    for i in range(5):
        result = compute(i)
        print(f"  compute({i}) = {result}")

    # Access hits create cache hits
    for i in range(3):
        compute(i)

    stats = compute.metrics.get_stats()
    print("\nMetrics:")
    print(f"  Hits: {stats.hits}")
    print(f"  Misses: {stats.misses}")
    print(f"  Hit Rate: {stats.hit_rate:.1f}%")
    print(f"  Avg Latency: {stats.avg_latency_ms:.2f}ms")

    compute.clear_cache()


def demo_prometheus_export():
    """Demonstrate exporting metrics to Prometheus."""
    print("\n=== Prometheus Export ===")

    @cachier(backend="memory", enable_metrics=True)
    def calculate(x, y):
        return x + y

    calculate.clear_cache()

    # Create exporter
    exporter = PrometheusExporter(port=9090, use_prometheus_client=False)
    exporter.register_function(calculate)

    # Generate some metrics
    calculate(1, 2)
    calculate(1, 2)  # hit
    calculate(3, 4)  # miss

    # Show text format metrics
    metrics_text = exporter._generate_text_metrics()
    print("\nGenerated Prometheus metrics:")
    print(metrics_text[:500] + "...")

    print("\nNote: In production, call exporter.start() to serve metrics")
    print("      Metrics would be available at http://localhost:9090/metrics")

    calculate.clear_cache()


def main():
    """Run all demonstrations."""
    print("Cachier Prometheus Exporter Demo")
    print("=" * 60)

    # Print usage instructions from module docstring
    if __doc__:
        print(__doc__)

    demo_basic_metrics()
    demo_prometheus_export()

    print("\n" + "=" * 60)
    print("✓ All demonstrations completed!")


if __name__ == "__main__":
    main()
