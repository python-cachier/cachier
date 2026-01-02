from datetime import timedelta
from unittest.mock import NonCallableMock, patch

import pytest

from cachier.cores.redis import _RedisCore


@pytest.mark.redis
class TestRedisCoreExceptions:
    @pytest.fixture
    def mock_redis(self):
        """Fixture providing a mock Redis client."""
        return NonCallableMock()

    @pytest.fixture
    def core(self, mock_redis):
        """Fixture providing a Redis core instance with mock client."""
        core = _RedisCore(
            hash_func=None, redis_client=mock_redis, wait_for_calc_timeout=10
        )
        core.set_func(lambda x: x)  # Set a dummy function
        return core

    def test_loading_pickle_exceptions_bytes(self):
        """Test _loading_pickle handles exceptions when deserializing bytes."""
        with (
            patch("pickle.loads", side_effect=Exception("Pickle error")),
            pytest.warns(
                UserWarning, match="Redis value deserialization failed"
            ),
        ):
            assert _RedisCore._loading_pickle(b"data") is None

    def test_loading_pickle_exceptions_str_success(self):
        """Test _loading_pickle latin-1 fallback for str input."""
        with patch("pickle.loads") as mock_loads:
            mock_loads.side_effect = [Exception("UTF-8 error"), "success"]
            res = _RedisCore._loading_pickle("data")
            assert res == "success"
            assert mock_loads.call_count == 2

    def test_loading_pickle_exceptions_str_fail(self):
        """Test _loading_pickle decoding failure for str input."""
        with (
            patch("pickle.loads", side_effect=Exception("Pickle error")),
            pytest.warns(
                UserWarning, match="Redis value deserialization failed"
            ),
        ):
            assert _RedisCore._loading_pickle("data") is None

    def test_loading_pickle_exceptions_other_type(self):
        """Test _loading_pickle exception handling for unsupported types."""
        with patch("pickle.loads", side_effect=Exception("Pickle error")):
            res = _RedisCore._loading_pickle(123)
            assert res is None

    def test_get_bool_field_exceptions(self):
        """Test _get_bool_field decoding exception fallback to latin-1."""
        # Byte string that fails utf-8 but works with latin-1
        # b'\xff' is invalid start byte in utf-8

        with patch.object(_RedisCore, "_get_raw_field", return_value=b"\xff"):
            res = _RedisCore._get_bool_field({}, "flag")
            assert res is False  # "ÿ" != "true"

    def test_get_entry_by_key_exceptions_hgetall(self, core, mock_redis):
        """Test get_entry_by_key hgetall exception."""
        mock_redis.hgetall.side_effect = Exception("Redis error")
        with pytest.warns(UserWarning, match="Redis get_entry_by_key failed"):
            assert core.get_entry_by_key("key")[1] is None

    def test_get_entry_by_key_exceptions_timestamp(self, core, mock_redis):
        """Test get_entry_by_key timestamp decoding exception."""
        mock_redis.hgetall.side_effect = None
        mock_redis.hgetall.return_value = {
            b"timestamp": b"\xff"
        }  # Invalid utf-8
        with pytest.warns(UserWarning, match="Redis get_entry_by_key failed"):
            core.get_entry_by_key("key")

    def test_set_entry_exceptions(self, core, mock_redis):
        """Test set_entry Redis hset exception handling and return False."""
        mock_redis.hset.side_effect = Exception("Redis error")
        with pytest.warns(UserWarning, match="Redis set_entry failed"):
            assert core.set_entry("key", "value") is False

    def test_mark_entry_being_calculated_exceptions(self, core, mock_redis):
        """Test mark_entry_being_calculated Redis hset exception handling."""
        mock_redis.hset.side_effect = Exception("Redis error")
        with pytest.warns(
            UserWarning, match="Redis mark_entry_being_calculated failed"
        ):
            core.mark_entry_being_calculated("key")

    def test_mark_entry_not_calculated_exceptions(self, core, mock_redis):
        """Test mark_entry_not_calculated Redis hset exception handling."""
        mock_redis.hset.side_effect = Exception("Redis error")
        with pytest.warns(
            UserWarning, match="Redis mark_entry_not_calculated failed"
        ):
            core.mark_entry_not_calculated("key")

    def test_clear_cache_exceptions(self, core, mock_redis):
        """Test clear_cache Redis keys exception handling."""
        mock_redis.keys.side_effect = Exception("Redis error")
        with pytest.warns(UserWarning, match="Redis clear_cache failed"):
            core.clear_cache()

    def test_clear_being_calculated_exceptions(self, core, mock_redis):
        """Test clear_being_calculated Redis keys exception handling."""
        mock_redis.keys.side_effect = Exception("Redis error")
        with pytest.warns(
            UserWarning, match="Redis clear_being_calculated failed"
        ):
            core.clear_being_calculated()

    def test_delete_stale_entries_keys_exception(self, core, mock_redis):
        """Test delete_stale_entries Redis keys exception handling."""
        mock_redis.keys.side_effect = Exception("Redis error")
        with pytest.warns(
            UserWarning, match="Redis delete_stale_entries failed"
        ):
            core.delete_stale_entries(timedelta(seconds=1))

    def test_delete_stale_entries_timestamp_parse_exception(
        self, core, mock_redis
    ):
        """Test delete_stale_entries timestamp parsing exception handling."""
        mock_redis.keys.return_value = [b"key1"]
        mock_redis.hget.return_value = b"invalid_timestamp"

        with pytest.warns(UserWarning, match="Redis timestamp parse failed"):
            core.delete_stale_entries(timedelta(seconds=1))
