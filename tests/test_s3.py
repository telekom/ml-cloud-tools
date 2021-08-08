# Copyright (c) 2021 Philip May
# This software is distributed under the terms of the MIT license
# which is available at https://opensource.org/licenses/MIT

import os
from pathlib import Path

import boto3
from moto import mock_s3

from ml_cloud_tools import copy_file_to_s3_file


@mock_s3
def test_copy_file_to_s3_file(tmpdir):
    bucket_name = "moto_mock_bucket"
    file_name = "s3_test_fie.txt"
    s3_file_dir = "test_dir"
    full_s3_file_name = (Path(s3_file_dir) / Path(s3_file_dir)).as_posix()
    file_content = "This is a s3 test file."

    # create mock bucket
    s3_resource = boto3.resource("s3")
    s3_resource.create_bucket(Bucket=bucket_name)

    # create locale test file
    tmp_file_name = os.path.join(tmpdir, file_name)
    with open(tmp_file_name, "w", encoding="utf-8") as f:
        f.write(file_content)

    copy_file_to_s3_file(tmp_file_name, full_s3_file_name, bucket_name)

    body = s3_resource.Object(bucket_name, full_s3_file_name).get()["Body"].read().decode("utf-8")
    assert body == file_content
