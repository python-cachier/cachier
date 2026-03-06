"""Cachier S3 backend example.

Demonstrates persistent function caching backed by AWS S3 (or any S3-compatible
service).  Requires boto3 to be installed::

    pip install cachier[s3]

A real S3 bucket (or a local S3-compatible service such as MinIO / localstack)
is needed to run this example.  Adjust the configuration variables below to
match your environment.

"""

import time
from datetime import timedelta

try:
    import boto3

    from cachier import cachier
except ImportError as exc:
    print(f"Missing required package: {exc}")
    print("Install with: pip install cachier[s3]")
    raise SystemExit(1) from exc

# ---------------------------------------------------------------------------
# Configuration - adjust these to your environment
# ---------------------------------------------------------------------------
BUCKET_NAME = "my-cachier-bucket"
REGION = "us-east-1"

# Optional: point to a local S3-compatible service
# ENDPOINT_URL = "http://localhost:9000"  # MinIO default
ENDPOINT_URL = None


# ---------------------------------------------------------------------------
# Helper: verify S3 connectivity
# ---------------------------------------------------------------------------


def _check_bucket(client, bucket: str) -> bool:
    """Return True if the bucket is accessible."""
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except Exception as exc:
        print(f"Cannot access bucket '{bucket}': {exc}")
        return False


# ---------------------------------------------------------------------------
# Demos
# ---------------------------------------------------------------------------


def demo_basic_caching():
    """Show basic S3 caching: the first call computes, the second reads cache."""
    print("\n=== Basic S3 caching ===")

    @cachier(
        backend="s3",
        s3_bucket=BUCKET_NAME,
        s3_region=REGION,
        s3_endpoint_url=ENDPOINT_URL,
    )
    def expensive(n: int) -> int:
        """Simulate an expensive computation."""
        print(f"  computing expensive({n})...")
        time.sleep(1)
        return n * n

    expensive.clear_cache()

    start = time.time()
    r1 = expensive(5)
    t1 = time.time() - start
    print(f"First call:  {r1}  ({t1:.2f}s)")

    start = time.time()
    r2 = expensive(5)
    t2 = time.time() - start
    print(f"Second call: {r2}  ({t2:.2f}s) - from cache")

    assert r1 == r2
    assert t2 < t1
    print("Basic caching works correctly.")


def demo_stale_after():
    """Show stale_after: results expire and are recomputed after the timeout."""
    print("\n=== Stale-after demo ===")

    @cachier(
        backend="s3",
        s3_bucket=BUCKET_NAME,
        s3_region=REGION,
        s3_endpoint_url=ENDPOINT_URL,
        stale_after=timedelta(seconds=3),
    )
    def timed(n: int) -> float:
        print(f"  computing timed({n})...")
        return time.time()

    timed.clear_cache()
    r1 = timed(1)
    r2 = timed(1)
    assert r1 == r2, "Second call should hit cache"

    print("Sleeping 4 seconds so the entry becomes stale...")
    time.sleep(4)

    r3 = timed(1)
    assert r3 > r1, "Should have recomputed after stale period"
    print("Stale-after works correctly.")


def demo_client_factory():
    """Show using a callable factory instead of a pre-built client."""
    print("\n=== Client factory demo ===")

    def make_client():
        """Lazily create a boto3 S3 client."""
        kwargs = {"region_name": REGION}
        if ENDPOINT_URL:
            kwargs["endpoint_url"] = ENDPOINT_URL
        return boto3.client("s3", **kwargs)

    @cachier(
        backend="s3",
        s3_bucket=BUCKET_NAME,
        s3_client_factory=make_client,
    )
    def compute(n: int) -> int:
        return n + 100

    compute.clear_cache()
    assert compute(7) == compute(7)
    print("Client factory works correctly.")


def demo_cache_management():
    """Show clear_cache and overwrite_cache."""
    print("\n=== Cache management demo ===")
    call_count = [0]

    @cachier(
        backend="s3",
        s3_bucket=BUCKET_NAME,
        s3_region=REGION,
        s3_endpoint_url=ENDPOINT_URL,
    )
    def managed(n: int) -> int:
        call_count[0] += 1
        return n * 3

    managed.clear_cache()
    managed(10)
    managed(10)
    assert call_count[0] == 1, "Should have been called once (cached on second call)"

    managed.clear_cache()
    managed(10)
    assert call_count[0] == 2, "Should have recomputed after cache clear"

    managed(10, cachier__overwrite_cache=True)
    assert call_count[0] == 3, "Should have recomputed due to overwrite_cache"
    print("Cache management works correctly.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    """Run all S3 backend demos."""
    print("Cachier S3 Backend Demo")
    print("=" * 50)

    client = boto3.client(
        "s3",
        region_name=REGION,
        **({"endpoint_url": ENDPOINT_URL} if ENDPOINT_URL else {}),
    )

    if not _check_bucket(client, BUCKET_NAME):
        print(f"\nCreate the bucket first:  aws s3 mb s3://{BUCKET_NAME} --region {REGION}")
        raise SystemExit(1)

    try:
        demo_basic_caching()
        demo_stale_after()
        demo_client_factory()
        demo_cache_management()

        print("\n" + "=" * 50)
        print("All S3 demos completed successfully.")
        print("\nKey benefits of the S3 backend:")
        print("- Persistent cache survives process restarts")
        print("- Shared across machines without a running service")
        print("- Works with any S3-compatible object storage")
    finally:
        client.close()


if __name__ == "__main__":
    main()
