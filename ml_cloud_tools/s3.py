# Copyright (c) 2021 Timothy Wolff-Piggott, Deutsche Telekom AG
# Copyright (c) 2021 Philip May, Deutsche Telekom AG
# This software is distributed under the terms of the MIT license
# which is available at https://opensource.org/licenses/MIT


"""S3 tools."""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import boto3


_logger = logging.getLogger(__name__)


def _get_s3_bucket(s3_bucket_name: Optional[str] = None):
    if s3_bucket_name is None:
        s3_bucket_name = os.getenv("DEFAULT_S3_BUCKET_NAME")
    if s3_bucket_name is None:
        raise ValueError(
            "S3 bucket name must be set by parameter or the "
            "'DEFAULT_S3_BUCKET_NAME' environment variable!"
        )
    # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#bucket
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(s3_bucket_name)
    return bucket


def copy_s3_file_to_file(
    s3_file_name: str,
    local_file_name: str,
    s3_bucket_name: Optional[str] = None,
    overwrite: bool = True,
    s3_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    """Copy a file from S3 to a file on the local file system.

    Download the S3 file at ``s3_dir_name`` from the S3 bucket ``S3_BUCKET_NAME``
    to the local file ``local_file_name``.

    Args:
        s3_file_name: The name of the so called key to download from.
            This is the part after the ``s3_bucket_name``. Example: ``/foo/bar/baz.txt``
        local_file_name: The local path to the file to download to.
            Example: ``/home/my_username/baz.txt``
        s3_bucket_name: The S3 bucket name. Can also be provided by the ``DEFAULT_S3_BUCKET_NAME``
            environment variable. One of the two must be specified.
        overwrite: Overwrite local file.
        s3_kwargs: Additional kwargs to be passed to the S3 client function.
    """
    s3_kwargs = {} if s3_kwargs is None else s3_kwargs
    if (not overwrite) and Path(local_file_name).is_file():
        _logger.debug("File %s is already available. Skipping it.", local_file_name)
    else:
        _logger.debug("Copying S3 file %s to %s", s3_file_name, local_file_name)
        s3_bucket = _get_s3_bucket(s3_bucket_name)
        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.download_file  # NOQA: E501
        s3_bucket.download_file(s3_file_name, local_file_name, **s3_kwargs)


def copy_file_to_s3_file(
    local_file_name: str,
    s3_file_name: str,
    s3_bucket_name: Optional[str] = None,
    s3_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    """Copy a file on the local file system to a file on S3.

    Upload a local file ``local_file_name`` to the S3 file at ``s3_dir_name`` from the
    S3 bucket ``S3_BUCKET_NAME``.

    Args:
        local_file_name: The local path to the file to upload.
            Example: ``/home/my_username/baz.txt``
        s3_file_name: The name of the so called key to upload to.
            This is the part after the ``s3_bucket_name``. Example: ``/foo/bar/baz.txt``
        s3_bucket_name: The S3 bucket name. Can also be provided by the ``DEFAULT_S3_BUCKET_NAME``
            environment variable. One of the two must be specified.
        s3_kwargs: Additional kwargs to be passed to the S3 client function.
    """
    s3_kwargs = {} if s3_kwargs is None else s3_kwargs
    _logger.debug("Copying %s to S3 file %s", local_file_name, s3_file_name)
    s3_bucket = _get_s3_bucket(s3_bucket_name)
    # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.upload_file  # NOQA: E501
    s3_bucket.upload_file(local_file_name, s3_file_name, **s3_kwargs)


def copy_s3_dir_to_dir(
    s3_dir_name: str,
    local_dir_name: str,
    s3_bucket_name: Optional[str] = None,
    overwrite: bool = True,
    s3_kwargs: Optional[Dict[str, Any]] = None,
) -> str:
    """Copy a directory from S3 to a directory on the local file system.

    Returns:
        Local directory where files are stored.
    """
    s3_kwargs = {} if s3_kwargs is None else s3_kwargs
    local_dir_path = Path(local_dir_name)
    if not local_dir_path.is_dir():
        raise ValueError(f"'local_dir_name' must be a directory! It was: {local_dir_name}")

    s3_bucket = _get_s3_bucket(s3_bucket_name)
    final_local_dir_path = local_dir_path / Path(s3_dir_name).name
    for obj in s3_bucket.objects.filter(Prefix=s3_dir_name):
        if obj.key[-1] == "/":
            _logger.debug("Skipping dir %s", obj.key)
        else:
            local_path = final_local_dir_path / Path(obj.key).relative_to(s3_dir_name)
            if (not overwrite) and local_path.is_file():
                _logger.debug("File %s is already available. Skipping it.", local_path.as_posix())
            else:
                _logger.debug("Copying S3 %s to %s", obj.key, local_path.as_posix())
                local_path.parents[0].mkdir(exist_ok=True, parents=True)
                # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.download_file  # NOQA: E501
                s3_bucket.download_file(obj.key, local_path.as_posix(), **s3_kwargs)
    return final_local_dir_path.as_posix()


def copy_dir_to_s3_dir(
    local_dir_name: str,
    s3_dir_name: str,
    s3_bucket_name: Optional[str] = None,
    s3_kwargs: Optional[Dict[str, Any]] = None,
) -> str:
    """Copy a directory from the local file system to a directory on S3.

    Returns:
        S3 directory where files are stored.
    """
    s3_kwargs = {} if s3_kwargs is None else s3_kwargs
    local_dir_path = Path(local_dir_name)
    if not local_dir_path.is_dir():
        raise ValueError(f"'local_dir_name' must be a directory! It was: {local_dir_name}")

    s3_bucket = _get_s3_bucket(s3_bucket_name)
    final_s3_dir_path = Path(s3_dir_name) / local_dir_path.name
    _logger.debug(
        "Uploading dir %s to S3 dir %s", local_dir_path.as_posix(), final_s3_dir_path.as_posix()
    )
    for file_path in local_dir_path.glob("**/*"):
        if file_path.is_dir():
            # s3 has no directories, just files with prefixes
            _logger.debug("Skipping dir %s", file_path.as_posix())
        else:
            s3_file_path = final_s3_dir_path / file_path.relative_to(local_dir_path)
            _logger.debug("Copying %s to S3 %s", file_path.as_posix(), s3_file_path.as_posix())
            # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.upload_file  # NOQA: E501
            s3_bucket.upload_file(file_path.as_posix(), s3_file_path.as_posix(), **s3_kwargs)
    return final_s3_dir_path.as_posix()
