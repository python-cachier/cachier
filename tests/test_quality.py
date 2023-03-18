import pytest
import subprocess


def test_flake8():
    """Flake8 linter passes with no warnings or errors."""
    command = ['flake8']
    parameters = ['--max-line-length=120', 'cachier', 'tests']
    subprocess.check_call(command + parameters)


@pytest.mark.skip
def test_pylint():
    """Pylint linter passes with no warnings or errors."""
    command = ['pylint']
    parameters = ['--max-line-length=120', 'cachier', 'tests']
    subprocess.check_call(command + parameters)
