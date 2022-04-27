#!/usr/bin/env python
import os
from typing import Any, Dict

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))


with open(os.path.join(here, "README.md"), "rt") as f:
    long_description = "\n" + f.read()


version_mod: Dict[str, Any] = {}
with open(os.path.join(here, "sqs_workers", "__version__.py")) as f:
    exec(f.read(), version_mod)


setup(
    name="sqs-workers",
    version=version_mod["__version__"],
    description="SQS Workers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Doist Developers",
    author_email="dev@doist.com",
    python_requires=">=3.8",
    url="https://github.com/Doist/sqs-workers",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "boto3",
        "pytest-runner",
        "attrs",
        "typing",
    ],
    include_package_data=True,
    license="MIT",
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
