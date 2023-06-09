# Copyright (c) 2021 Philip May
# This software is distributed under the terms of the MIT license
# which is available at https://opensource.org/licenses/MIT

"""ML-Cloud-Tools main package."""

from ml_cloud_tools.s3 import (
    copy_dir_to_s3_dir,
    copy_file_to_s3_file,
    copy_s3_dir_to_dir,
    copy_s3_file_to_file,
    list_s3_files,
)


# Versioning follows the Semantic Versioning Specification https://semver.org/ and
# PEP 440 -- Version Identification and Dependency Specification: https://www.python.org/dev/peps/pep-0440/  # noqa: E501
__version__ = "0.0.1"

__all__ = [
    "copy_dir_to_s3_dir",
    "copy_file_to_s3_file",
    "copy_s3_dir_to_dir",
    "copy_s3_file_to_file",
    "list_s3_files",
    "__version__",
]
