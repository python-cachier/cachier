import pytest
import subprocess


@pytest.mark.skip  # todo: dead check, so let's replace it with Ruff
def test_bandit():
    """Bandit security scan passes with no warnings or errors."""
    command = ["bandit"]
    parameters = ["-r", "cachier"]
    subprocess.check_call(command + parameters)
    parameters = ["-s", "B101,B311,B404,B603", "-r", "tests"]
    subprocess.check_call(command + parameters)


@pytest.mark.skip  # todo: dead check, enable it in separate PR w/ pre-commit
def test_safety():
    """Safety security scan passes with no warnings or errors."""
    command = ["safety"]
    parameters = ["check", "--full-report"]
    subprocess.check_call(command + parameters)
