import datetime
import cachier

# Test to understand the problem better
print("=== Test understanding ===")

# First, set caching disabled initially
cachier.set_global_params(caching_enabled=False, separate_files=True)
print(f"Initial caching enabled: {cachier.get_global_params().caching_enabled}")

class Test:
    def __init__(self, cache_ttl = None):
        self.counter = 0
        if cache_ttl is not None:
            stale_after = datetime.timedelta(seconds=cache_ttl)
            # Update stale_after and enable caching
            cachier.set_global_params(stale_after=stale_after)
            cachier.enable_caching()
            print(f"After enabling: {cachier.get_global_params().caching_enabled}")

    @cachier.cachier()
    def test(self, param):
        print(f"Function called, counter: {self.counter}")
        self.counter += 1
        return param

if __name__ == "__main__":
    print("Creating Test instance...")
    t = Test(cache_ttl=1)
    
    print("First call:")
    result1 = t.test("a")
    print(f"Result: {result1}, Counter: {t.counter}")
    
    print("Second call:")
    result2 = t.test("a")
    print(f"Result: {result2}, Counter: {t.counter}")
    
    print(f"Final caching enabled: {cachier.get_global_params().caching_enabled}")