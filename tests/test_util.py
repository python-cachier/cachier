"""Additional tests for util module to improve coverage."""

import pytest

from cachier.util import parse_bytes


def test_parse_bytes_int_input():
    """Test parse_bytes with integer input."""
    # Test line 12: direct return for int input
    assert parse_bytes(1024) == 1024
    assert parse_bytes(0) == 0
    assert parse_bytes(1000000) == 1000000


def test_parse_bytes_invalid_format():
    """Test parse_bytes with invalid format."""
    # Test line 15: ValueError for invalid format
    with pytest.raises(ValueError, match="Invalid size value: invalid"):
        parse_bytes("invalid")
    
    with pytest.raises(ValueError, match="Invalid size value: 123XB"):
        parse_bytes("123XB")
    
    with pytest.raises(ValueError, match="Invalid size value: abc123"):
        parse_bytes("abc123")