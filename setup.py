#!/usr/bin/env python
import sys


# require python 3.7 or newer
if sys.version_info < (3, 7):
    print("Error: dbt does not support this version of Python.")
    print("Please upgrade to Python 3.7 or higher.")
    sys.exit(1)


# require version of setuptools that supports find_namespace_packages
try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" ' "and try again")
    sys.exit(1)


from pathlib import Path
from setuptools import setup
from packaging.version import Version
from typing import Dict


# pull the long description from the README
_README = Path(__file__).parent / "README.md"


def _adapter_version() -> str:
    """
    Pull the package version from the main package `__version__` file
    """
    version_file = Path(__file__).parent / "dbt/adapters/redshift/__version__.py"
    attributes: Dict[str, str] = {}
    exec(version_file.read_text(), attributes)
    return attributes["version"]


def _core_version(adapter_version: str = _adapter_version()) -> str:
    """
    Determine the appropriate corresponding version of `dbt-core` based on the version of this adapter.

    If this is a pre-release, we want to get the latest pre-release for this minor version. Since this will
    be used with `dbt-core~={version}`, it suffices to select a patch of "0" and a pre-release of the same kind.

    If this is not a pre-release, it suffices to select a patch of "0".

    e.g.
        1.4.2b2 -> `dbt-core~=1.4.0b1`
        1.4.2 -> `dbt-core~=1.4.0`

    Args:
        adapter_version: the version of this adapter directly from `__version__.py`

    Returns:
        the appropriate version of `dbt-core`
    """
    adapter_version_parsed = Version(adapter_version)
    major, minor, _ = adapter_version_parsed.release
    core_release = f"{major}.{minor}.0"
    if adapter_version_parsed.is_prerelease:
        stage, _ = adapter_version_parsed.pre  # type: ignore
        core_release += f"{stage}1"
    assert Version(core_release)
    return core_release


setup(
    name="dbt-snowflake",
    version=_adapter_version(),
    description="The Snowflake adapter plugin for dbt",
    long_description=_README.read_text(),
    long_description_content_type="text/markdown",
    author="dbt Labs",
    author_email="info@dbtlabs.com",
    url="https://github.com/dbt-labs/dbt-snowflake",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        f"dbt-core~={_core_version()}",
        "snowflake-connector-python[secure-local-storage]~=3.0",
        # don't pin these; they're included in the above packages, but we use them directly
        "pytz",
        "requests",
        "types-pytz",
        "types-requests",
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7",
)
