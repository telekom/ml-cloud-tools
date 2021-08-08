# Copyright (c) 2021 Philip May
# This software is distributed under the terms of the MIT license
# which is available at https://opensource.org/licenses/MIT

from packaging.version import Version

from ml_cloud_tools import __version__


def test__version__():
    # if __version__ does not conform to PEP 440 InvalidVersion will be raised
    Version(__version__)
