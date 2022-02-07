#!/usr/bin/env python
"""The setup script."""
from setuptools import find_packages
from setuptools import setup

with open("README.md") as readme_file:
    readme = readme_file.read()

requirements = [
    "google-api-python-client==2.36.0",
    "google-cloud-bigquery==2.32.0",
    "google-cloud-storage==2.1.0",
]

test_requirements = ["pytest>=3"]

setup(
    author="Zhong Dai",
    author_email="zhongdai.au@gmail.com",
    python_requires=">=3.7.3",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    description="A lightweight wrapper for GCS and BigQuery client library",
    install_requires=requirements,
    long_description=readme + "\n\n",
    long_description_content_type="text/markdown",
    include_package_data=True,
    keywords="google cloud bigquery gcs",
    name="gfluent",
    packages=find_packages(include=["gfluent", "gfluent.*"]),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/simple-dev-tools/gfluent",
    version="1.2.0",
    zip_safe=False,
)
