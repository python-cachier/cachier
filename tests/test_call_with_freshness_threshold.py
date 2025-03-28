import time
from datetime import timedelta

import cachier


def test_call_with_freshness_threshold():
    @cachier.cachier()
    def test_func(a, b):
        print("Computing...")
        return a + b

    print(f"{test_func(1, 2) = }")
    print(f"{test_func(1, 2) = }")
    caller_with_freshness_threshold = (
        test_func.caller_with_freshness_threshold(
            timedelta(seconds=0.5),
        )
    )
    print(f"{caller_with_freshness_threshold(1, 2) = }")
    print(f"{time.sleep(1.0) = }")
    print(f"{test_func(1, 2) = }")
    print(f"{caller_with_freshness_threshold(1, 2) = }")


if __name__ == "__main__":
    test_call_with_freshness_threshold()
