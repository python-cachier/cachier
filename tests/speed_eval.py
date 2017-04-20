
from time import time

from cachier import cachier


@cachier(next_time=True)
def _test_int_pickling(int_1, int_2):
    """Add the two given ints."""
    return int_1 + int_2


def _test_int_pickling_compare(int_1, int_2):
    """Add the two given ints."""
    return int_1 + int_2


def test_pickle_speed():
    """Test speeds"""
    print("Comparing speeds of decorated vs non-decorated functions...")
    num_of_vals = 100
    times = []
    for i in range(1, num_of_vals):
        tic = time()
        _test_int_pickling_compare(i, i + 1)
        toc = time()
        times.append(toc - tic)
    print('  - Non-decorated average = {:.8f}'.format(
        sum(times) / num_of_vals))

    _test_int_pickling.clear_cache()
    times = []
    for i in range(1, num_of_vals):
        tic = time()
        _test_int_pickling(i, i + 1)
        toc = time()
        times.append(toc - tic)
    print('  - Decorated average = {:.8f}'.format(
        sum(times) / num_of_vals))

if __name__ == '__main__':
    test_pickle_speed()
