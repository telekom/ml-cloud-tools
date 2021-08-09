# Copyright (c) 2021 Timothy Wolff-Piggott, Deutsche Telekom AG
# Copyright (c) 2021 Philip May, Deutsche Telekom AG
# This software is distributed under the terms of the MIT license
# which is available at https://opensource.org/licenses/MIT

"""S3 tools."""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3


_logger = logging.getLogger(__name__)


def _get_s3_bucket_name(s3_bucket_name: Optional[str] = None) -> str:
    if s3_bucket_name is None:
        s3_bucket_name = os.getenv("DEFAULT_S3_BUCKET_NAME")
    if s3_bucket_name is None:
        raise ValueError(
            "S3 bucket name must be set by parameter or the "
            "'DEFAULT_S3_BUCKET_NAME' environment variable!"
        )
    _logger.debug("Using s3_bucket_name: %s", s3_bucket_name)
    return s3_bucket_name


def _get_s3_bucket(s3_bucket_name: Optional[str] = None):
    s3_bucket_name = _get_s3_bucket_name(s3_bucket_name)
    # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/boto3.html?highlight=resource#boto3.resource  # NOQA: E501
    s3_resource = boto3.resource("s3")
    # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#bucket
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

    Download the S3 file at ``s3_dir_name`` from the S3 bucket ``s3_bucket_name``
    to the local file ``local_file_name``.

    Args:
        s3_file_name: Name of the so called key to download from.
            This is the part after the ``s3_bucket_name``. Example: ``/foo/bar/baz.txt``
        local_file_name: Local path to the file to download to.
            Example: ``/home/my_username/baz.txt``
        s3_bucket_name: S3 bucket name. Can also be provided by the ``DEFAULT_S3_BUCKET_NAME``
            environment variable. One of the two must be specified. If both are specified this
            argument has priority.
        overwrite: Overwrite local file.
        s3_kwargs: Additional kwargs to be passed to the S3 client function
            :meth:`S3.Bucket.download_file`.
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
    S3 bucket ``s3_bucket_name``.

    Args:
        local_file_name: Local path to the file to upload.
            Example: ``/home/my_username/baz.txt``
        s3_file_name: Name of the so called key to upload to.
            This is the part after the ``s3_bucket_name``. Example: ``/foo/bar/baz.txt``
        s3_bucket_name: S3 bucket name. Can also be provided by the ``DEFAULT_S3_BUCKET_NAME``
            environment variable. One of the two must be specified. If both are specified this
            argument has priority.
        s3_kwargs: Additional kwargs to be passed to the S3 client function
            :meth:`S3.Bucket.upload_file`.
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

    If you call this function with ``s3_dir_name = "a/x"`` and ``local_dir_name = "y"``
    it will create a local directory ``y/x`` and copy the S3 content in ``a/x`` to that location.
    This way a S3 file at ``a/x/file.txt`` would be copied to ``y/x/file.txt``.

    Args:
        s3_dir_name: Name of the S3 directory.
            This is the part after the ``s3_bucket_name``. Example: ``/foo/bar``
        local_dir_name: Name of the local directory.
        s3_bucket_name: S3 bucket name. Can also be provided by the ``DEFAULT_S3_BUCKET_NAME``
            environment variable. One of the two must be specified. If both are specified this
            argument has priority.
        overwrite: Overwrite already existing files.
        s3_kwargs: Additional kwargs to be passed to the S3 client function
            :meth:`S3.Bucket.download_file`.

    Returns:
        Local directory where files are stored.
        In the example above, this would be ``y/x``.
    """
    s3_kwargs = {} if s3_kwargs is None else s3_kwargs
    local_dir_path = Path(local_dir_name)
    if not local_dir_path.is_dir():
        raise ValueError(f"'local_dir_name' must be a directory! It was: {local_dir_name}")

    s3_bucket = _get_s3_bucket(s3_bucket_name)
    final_local_dir_path = local_dir_path / Path(s3_dir_name).name
    for obj in s3_bucket.objects.filter(Prefix=s3_dir_name):
        if obj.key[-1] == "/":  # TODO: this might not be needed
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

    If you call this function with ``local_dir_name = "a/x"`` and ``s3_dir_name = "y"``
    it will copy the content in ``a/x`` to the S3 location below ``y/x``.
    This way the local file at ``a/x/file.txt`` would be copied to S3 at the
    location ``y/x/tfile.txt``.

    Args:
        local_dir_name: Name of the local directory.
        s3_dir_name: Name of the S3 directory.
            This is the part after the ``s3_bucket_name``. Example: ``/foo/bar``
        s3_bucket_name: S3 bucket name. Can also be provided by the ``DEFAULT_S3_BUCKET_NAME``
            environment variable. One of the two must be specified. If both are specified this
            argument has priority.
        s3_kwargs: Additional kwargs to be passed to the S3 client function
            :meth:`S3.Bucket.upload_file`.

    Returns:
        S3 directory where files are stored.
        In the example above, this would be ``y/x``.
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


def list_s3_files(
    s3_dir_name: str,
    s3_bucket_name: Optional[str] = None,
    s3_kwargs: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """List files in S3 directory.

    Args:
        s3_dir_name: Name of the S3 directory.
            This is the part after the ``s3_bucket_name``. Example: ``/foo/bar``
        s3_bucket_name: S3 bucket name. Can also be provided by the ``DEFAULT_S3_BUCKET_NAME``
            environment variable. One of the two must be specified. If both are specified this
            argument has priority.
        s3_kwargs: Additional kwargs to be passed to the S3 client function
            :meth:`S3.Client.list_objects_v2`.

    Returns:
        List of files in ``s3_dir_name``.
    """
    s3_kwargs = {} if s3_kwargs is None else s3_kwargs
    s3_bucket_name = _get_s3_bucket_name(s3_bucket_name)
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_dir_name, **s3_kwargs)
    if response["KeyCount"] == 0:
        _logger.warning("S3 directory is empty. s3_dir_name: %s", s3_dir_name)
        return []
    files = [key_dict["Key"] for key_dict in response["Contents"]]
    while response["IsTruncated"]:
        _logger.debug("Got continuation token, re-listing")
        continuation_token = response["NextContinuationToken"]
        response = s3_client.list_objects_v2(
            Bucket=s3_bucket_name,
            Prefix=s3_dir_name,
            ContinuationToken=continuation_token,
            **s3_kwargs,
        )
        files.extend(key_dict["Key"] for key_dict in response["Contents"])
    return files
