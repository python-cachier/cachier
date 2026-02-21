"""Shared S3 test fixtures."""

import pytest

from .helpers import S3_DEPS_AVAILABLE, TEST_BUCKET, TEST_REGION, skip_if_missing

if S3_DEPS_AVAILABLE:
    import boto3
    from moto import mock_aws


@pytest.fixture
def s3_bucket():
    """Yield a mocked S3 bucket name, set up and torn down around each test."""
    skip_if_missing()
    with mock_aws():
        client = boto3.client("s3", region_name=TEST_REGION)
        client.create_bucket(Bucket=TEST_BUCKET)
        yield TEST_BUCKET


@pytest.fixture
def s3_client(s3_bucket):
    """Yield a boto3 S3 client within the mocked AWS context."""
    # s3_bucket fixture already sets up the mock_aws context manager;
    # we just need to return a client pointing at the same mock.
    return boto3.client("s3", region_name=TEST_REGION)
