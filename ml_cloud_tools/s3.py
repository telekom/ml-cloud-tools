# Copyright (c) 2021 Timothy Wolff-Piggott, Deutsche Telekom AG
# Copyright (c) 2021 Philip May, Deutsche Telekom AG
# This software is distributed under the terms of the MIT license
# which is available at https://opensource.org/licenses/MIT


"""S3 tools."""

import logging
import os
from typing import Any, Dict, Optional

import boto3


_logger = logging.getLogger(__name__)


def _get_s3_bucket(s3_bucket_name: Optional[str] = None):
    if s3_bucket_name is None:
        s3_bucket_name = os.getenv("DEFAULT_S3_BUCKET_NAME")

    if s3_bucket_name is None:
        raise ValueError(
            "S3 bucket name must be set by parameter or "
            "'DEFAULT_S3_BUCKET_NAME' environment variable!"
        )

    # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#bucket
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(s3_bucket_name)
    return bucket


def download_s3_file(
    s3_file_key: str,
    local_file: str,
    s3_bucket_name: Optional[str] = None,
    s3_kwargs: Optional[Dict[str, Any]] = None,
):
    """Download file from S3 to local file system.

    Download file at ``s3_prefix`` from bucket S3_BUCKET_NAME
    to local file path <destination>.

    Args:
        s3_file_key: The name of the so called key to download from.
            This is the part after the ``s3_bucket_name``. Example: ``/foo/bar/baz.txt``
        local_file: The local path to the file to download to.
            Example: ``/home/my_username/baz.txt``
        s3_bucket_name: The S3 bucket name. Can also be provided by the ``DEFAULT_S3_BUCKET_NAME``
            environment variable. One of the two must be specified.
        s3_kwargs: Additional kwargs to be passed to the S3 client function.
    """
    s3_bucket = _get_s3_bucket(s3_bucket_name)

    # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.download_file  # NOQA: E501
    s3_bucket.download_file(s3_file_key, local_file, **s3_kwargs)
