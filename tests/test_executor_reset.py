"""Regression tests for replacing background worker pools."""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from cachier.core import MAX_WORKERS_ENVAR_NAME, _get_executor


def test_reset_retires_pool_without_dropping_pending_work(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep running and queued work alive while retiring the old pool.

    The replacement must become usable before the old work completes.

    """
    previous = ThreadPoolExecutor(max_workers=1)
    monkeypatch.setattr(_get_executor, "executor", previous, raising=False)
    monkeypatch.setenv(MAX_WORKERS_ENVAR_NAME, "1")
    started = threading.Event()
    release = threading.Event()

    def blocked_work() -> str:
        """Wait for the test to release the old worker.

        A timeout bounds cleanup if an assertion fails.

        """
        started.set()
        release.wait(timeout=5)
        return "running"

    running = previous.submit(blocked_work)
    reset = threading.Thread(target=_get_executor, kwargs={"reset": True})
    try:
        assert started.wait(timeout=2)
        queued = previous.submit(str, "queued")
        reset.start()
        reset.join(timeout=1)
        assert not reset.is_alive()
        replacement = _get_executor()
        assert replacement is not previous
        assert replacement.submit(str, "new").result(timeout=2) == "new"
        with pytest.raises(RuntimeError, match="shutdown"):
            previous.submit(str, "rejected")
        release.set()
        assert running.result(timeout=2) == "running"
        assert queued.result(timeout=2) == "queued"
    finally:
        release.set()
        if reset.ident is not None:
            reset.join(timeout=5)
        previous.shutdown(wait=True)
        _get_executor().shutdown(wait=True)


def test_failed_reset_preserves_usable_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the existing pool available when replacement creation fails.

    Invalid worker settings must not shut down a working executor.

    """
    with ThreadPoolExecutor(max_workers=1) as previous:
        monkeypatch.setattr(_get_executor, "executor", previous, raising=False)
        monkeypatch.setenv(MAX_WORKERS_ENVAR_NAME, "0")
        with pytest.raises(ValueError, match="max_workers"):
            _get_executor(reset=True)
        assert _get_executor() is previous
        assert previous.submit(str, "still usable").result(timeout=2) == "still usable"
