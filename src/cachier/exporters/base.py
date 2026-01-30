"""Base interface for metrics exporters."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license

import abc
from typing import Any, Callable


class MetricsExporter(metaclass=abc.ABCMeta):
    """Abstract base class for metrics exporters.

    Exporters collect metrics from cached functions and export them to monitoring systems like Prometheus, StatsD,
    CloudWatch, etc.

    """

    @abc.abstractmethod
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

    @abc.abstractmethod
    def export_metrics(self, func_name: str, metrics: Any) -> None:
        """Export metrics for a specific function.

        Parameters
        ----------
        func_name : str
            Name of the function
        metrics : MetricSnapshot
            Metrics snapshot to export

        """

    @abc.abstractmethod
    def start(self) -> None:
        """Start the exporter (e.g., start HTTP server for Prometheus)."""

    @abc.abstractmethod
    def stop(self) -> None:
        """Stop the exporter and clean up resources."""
