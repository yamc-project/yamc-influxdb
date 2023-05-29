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


# setup main
# required modules
install_requires = [
    "yamc_server>=1.1.0",
    "influxdb>=5.3.1",
    "setuptools_scm>=6.0.1",
]

setup(
    name="yamc-influxdb",
    use_scm_version={"root": ".", "relative_to": __file__, "local_scheme": "node-and-timestamp"},
    description="yamc writer for influxdb",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="Tomas Vitvar",
    author_email="tomas@vitvar.com",
    packages=find_packages(exclude=["tests.*", "tests"]),
    include_package_data=True,
    install_requires=install_requires,
    python_requires=">=3.11.0",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.11",
    ],
)
