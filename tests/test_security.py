import pytest
import subprocess


@pytest.mark.skip
def test_bandit():
    """Bandit security scan passes with no warnings or errors."""
    command = ['bandit']
    parameters = ['-r', 'cachier']
    subprocess.check_call(command + parameters)
    parameters = ['-s', 'B101,B311,B404,B603', '-r', 'tests']
    subprocess.check_call(command + parameters)


def test_safety():
    """Safety security scan passes with no warnings or errors."""
    command = ['safety']
    parameters = ['check', '--full-report']
    subprocess.check_call(command + parameters)
