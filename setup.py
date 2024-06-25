#!/usr/bin/env python
import os
from pathlib import Path

import sys

# require python 3.8 or newer
if sys.version_info < (3, 8):
    print("Error: dbt does not support this version of Python.")
    print("Please upgrade to Python 3.8 or higher.")
    sys.exit(1)


# require version of setuptools that supports find_namespace_packages
from setuptools import setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" ' "and try again")
    sys.exit(1)


# pull long description from README
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()


# used for this adapter's version
VERSION = Path(__file__).parent / "dbt/adapters/snowflake/__version__.py"


def _plugin_version() -> str:
    """
    Pull the package version from the main package version file
    """
    attributes = {}
    exec(VERSION.read_text(), attributes)
    return attributes["version"]


package_name = "dbt-snowflake"
description = """The Snowflake adapter plugin for dbt"""

setup(
    name=package_name,
    version=_plugin_version(),
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="dbt Labs",
    author_email="info@dbtlabs.com",
    url="https://github.com/dbt-labs/dbt-snowflake",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-common>=1.3.0,<2.0",
        "dbt-adapters>=1.3.1,<2.0",
        "snowflake-connector-python[secure-local-storage]~=3.0",
        # add dbt-core to ensure backwards compatibility of installation, this is not a functional dependency
        "dbt-core>=1.8.0",
        # installed via dbt-core but referenced directly; don't pin to avoid version conflicts with dbt-core
        "agate",
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
)
