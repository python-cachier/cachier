import datetime
import cachier

cachier.set_default_params(caching_enabled=False, separate_files=True)

class Test:
    def __init__(self, cache_ttl = None):
        self.counter = 0
        if cache_ttl is not None:
            stale_after = datetime.timedelta(seconds=cache_ttl)
            cachier.set_default_params(stale_after=stale_after)
            cachier.enable_caching()

    @cachier.cachier()
    def test(self, param):
        self.counter += 1
        assert self.counter < 2
        return param

if __name__ == "__main__":
    t = Test(cache_ttl=1)
    t.test("a")
    t.test("a")