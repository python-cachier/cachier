"""A command-line interface for cachier."""

import click

from cachier.core import _set_max_workers


@click.group()
def cli():
    """A command-line interface for cachier."""


@cli.command("Limits the number of worker threads used by cachier.")
@click.argument("max_workers", nargs=1, type=int)
def set_max_workers(max_workers):
    """Limits the number of worker threads used by cachier."""
    _set_max_workers(max_workers)
