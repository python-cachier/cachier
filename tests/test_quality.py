import pytest
import subprocess


@pytest.mark.skip  # todo: dead check, so let's replace it with Ruff
def test_pylint():
    """Pylint linter passes with no warnings or errors."""
    command = ['pylint']
    parameters = ['--max-line-length=120', 'cachier', 'tests']
    subprocess.check_call(command + parameters)
