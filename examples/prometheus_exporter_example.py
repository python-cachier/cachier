"""Demonstration of Prometheus metrics exporter for cachier.

This example shows how to export cachier metrics to Prometheus for monitoring.
The exporter can work with or without the prometheus_client library.

"""

import time

from cachier import cachier
from cachier.exporters import PrometheusExporter

print("=" * 60)
print("Cachier Prometheus Exporter Demo")
print("=" * 60)


# Define some cached functions with metrics enabled
@cachier(backend="memory", enable_metrics=True)
def calculate_square(x):
    """Calculate square of a number."""
    time.sleep(0.01)  # Simulate computation
    return x**2


@cachier(backend="memory", enable_metrics=True)
def calculate_cube(x):
    """Calculate cube of a number."""
    time.sleep(0.01)  # Simulate computation
    return x**3


# Create a Prometheus exporter
# Set use_prometheus_client=False to use built-in text format
exporter = PrometheusExporter(port=9100, use_prometheus_client=False)

# Register functions to export
print("\nRegistering functions with exporter...")
exporter.register_function(calculate_square)
exporter.register_function(calculate_cube)
print("✓ Functions registered")

# Generate some cache activity
print("\nGenerating cache activity...")
calculate_square.clear_cache()
calculate_cube.clear_cache()

# Create some metrics
for i in range(20):
    calculate_square(i % 5)  # Will create hits and misses

for i in range(15):
    calculate_cube(i % 3)

print("✓ Generated activity on both functions")

# Display metrics for each function
print("\n" + "=" * 60)
print("Metrics Summary")
print("=" * 60)

square_stats = calculate_square.metrics.get_stats()
print("\ncalculate_square:")
print(f"  Hits: {square_stats.hits}")
print(f"  Misses: {square_stats.misses}")
print(f"  Hit rate: {square_stats.hit_rate:.1f}%")
print(f"  Total calls: {square_stats.total_calls}")

cube_stats = calculate_cube.metrics.get_stats()
print("\ncalculate_cube:")
print(f"  Hits: {cube_stats.hits}")
print(f"  Misses: {cube_stats.misses}")
print(f"  Hit rate: {cube_stats.hit_rate:.1f}%")
print(f"  Total calls: {cube_stats.total_calls}")

# Generate Prometheus text format
print("\n" + "=" * 60)
print("Prometheus Text Format Export")
print("=" * 60)

metrics_text = exporter._generate_text_metrics()
print("\nSample of exported metrics:")
print("-" * 60)
# Print first 20 lines
lines = metrics_text.split("\n")[:20]
for line in lines:
    print(line)
print("...")
print(f"\nTotal lines exported: {len(metrics_text.split(chr(10)))}")

# Instructions for using with Prometheus
print("\n" + "=" * 60)
print("Usage with Prometheus")
print("=" * 60)
print("""
To use this exporter with Prometheus:

1. Start the exporter HTTP server:
   >>> exporter.start()

2. Add to your prometheus.yml:
   scrape_configs:
     - job_name: 'cachier'
       static_configs:
         - targets: ['localhost:9100']

3. Access metrics at http://localhost:9100/metrics

4. Query in Prometheus:
   - cachier_cache_hit_rate
   - rate(cachier_cache_hits_total[5m])
   - cachier_entry_count

Alternative: Use with prometheus_client
---------------------------------------
If you have prometheus_client installed:

>>> from prometheus_client import start_http_server
>>> exporter = PrometheusExporter(port=9100, use_prometheus_client=True)
>>> exporter.register_function(my_cached_func)
>>> exporter.start()

This provides additional features like:
- Automatic metric registration
- Built-in histograms
- Gauges and counters
- Integration with Prometheus pushgateway
""")

print("\n" + "=" * 60)
print("Demo Complete")
print("=" * 60)
print("""
Key Benefits:
  • Track cache performance in production
  • Identify optimization opportunities
  • Set up alerts for low hit rates
  • Monitor cache effectiveness over time
  • Integrate with existing monitoring infrastructure
""")

# Clean up
calculate_square.clear_cache()
calculate_cube.clear_cache()
