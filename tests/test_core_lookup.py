"""Testing the MongoDB core of cachier."""

from cachier import cachier
from cachier.core import MissingMongetter


def test_bad_name():
    """Test that the appropriate exception is thrown when an invalid backend
    is given."""

    name = 'nope'
    try:
        @cachier(backend=name)
        def func():
            pass
    except ValueError as e:
        assert name in e.args[0]
    else:
        assert False


def test_missing_mongetter():
    """Test that the appropriate exception is thrown when forgetting to
    specify the mongetter."""
    try:
        @cachier(backend='mongo', mongetter=None)
        def func():
            pass
    except MissingMongetter:
        assert True
    else:
        assert False
