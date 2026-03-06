"""S3 test helpers."""

import pytest

try:
    import boto3
    import moto

    S3_DEPS_AVAILABLE = True
except ImportError:
    boto3 = None  # type: ignore[assignment]
    moto = None  # type: ignore[assignment]
    S3_DEPS_AVAILABLE = False

TEST_BUCKET = "cachier-test-bucket"
TEST_REGION = "us-east-1"


def skip_if_missing():
    """Skip the test if boto3 or moto are not installed."""
    if not S3_DEPS_AVAILABLE:
        pytest.skip("boto3 and moto are required for S3 tests")


def make_s3_client(endpoint_url=None):
    """Return a boto3 S3 client pointed at the moto mock or a custom endpoint."""
    if not S3_DEPS_AVAILABLE:
        pytest.skip("boto3 and moto are required for S3 tests")
    kwargs = {"region_name": TEST_REGION}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client("s3", **kwargs)
