#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import codecs
import os
import re
import sys
import argparse
import glob

from setuptools import find_packages
from setuptools import setup


# read file content
def read(*parts):
    path = os.path.join(os.path.dirname(__file__), *parts)
    with codecs.open(path, encoding="utf-8") as fobj:
        return fobj.read()


# finds the version of the package
def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


# setup main
# required modules
install_requires = [
    "yamc_server>=1.1.0",
    "influxdb>=5.3.1",
]

setup(
    name="yamc-influxdb",
    version=find_version("yamc_influxdb", "__init__.py"),
    description="yamc writer for influxdb",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="Tomas Vitvar",
    author_email="tomas@vitvar.com",
    packages=find_packages(exclude=["tests.*", "tests"]),
    include_package_data=True,
    install_requires=install_requires,
    python_requires=">=3.6.0",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
)
