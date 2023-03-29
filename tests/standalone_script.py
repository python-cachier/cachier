import cachier
import time


@cachier.cachier()
def _takes_3_seconds(label, value):
    time.sleep(3)
    return f'{label} {value}'


print(_takes_3_seconds('two', 2))
