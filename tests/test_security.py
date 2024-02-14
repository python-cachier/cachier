import pytest
import subprocess


@pytest.mark.skip  # todo: dead check, enable it in separate PR w/ pre-commit
def test_safety():
    """Safety security scan passes with no warnings or errors."""
    command = ["safety"]
    parameters = ["check", "--full-report"]
    subprocess.check_call(command + parameters)
